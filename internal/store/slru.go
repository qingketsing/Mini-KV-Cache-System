package store

import "container/list"

type policySegment uint8

const (
	probation policySegment = iota
	protected
)

type policyItem struct {
	key        string
	generation uint64
	cost       int64
	segment    policySegment
}

type policyCandidate struct {
	key        string
	generation uint64
	cost       int64
	ok         bool
}

type policyExclusion struct {
	key        string
	generation uint64
	enabled    bool
}

type slru struct {
	probation list.List
	protected list.List
	nodes     map[string]*list.Element

	protectedBytes int64
	protectedLimit int64
}

func newSLRU(protectedLimit int64) slru {
	if protectedLimit < 1 {
		protectedLimit = 1
	}
	return slru{
		nodes:          make(map[string]*list.Element),
		protectedLimit: protectedLimit,
	}
}

func (p *slru) insert(key string, generation uint64, cost int64) {
	if current, ok := p.nodes[key]; ok {
		p.removeElement(current)
	}
	item := &policyItem{
		key:        key,
		generation: generation,
		cost:       cost,
		segment:    probation,
	}
	p.nodes[key] = p.probation.PushFront(item)
}

func (p *slru) touch(key string, generation uint64) bool {
	element, ok := p.nodes[key]
	if !ok {
		return false
	}
	item := element.Value.(*policyItem)
	if item.generation != generation {
		return false
	}

	if item.segment == protected {
		p.protected.MoveToFront(element)
		return true
	}

	p.probation.Remove(element)
	item.segment = protected
	p.protectedBytes += item.cost
	p.nodes[key] = p.protected.PushFront(item)
	p.rebalanceProtected()
	return true
}

func (p *slru) remove(key string, generation uint64) bool {
	element, ok := p.nodes[key]
	if !ok {
		return false
	}
	item := element.Value.(*policyItem)
	if item.generation != generation {
		return false
	}
	p.removeElement(element)
	return true
}

func (p *slru) victim(exclusion policyExclusion) policyCandidate {
	if candidate := candidateFromList(&p.probation, exclusion); candidate.ok {
		return candidate
	}
	return candidateFromList(&p.protected, exclusion)
}

func (p *slru) rebalanceProtected() {
	for p.protectedBytes > p.protectedLimit && p.protected.Len() > 1 {
		element := p.protected.Back()
		item := element.Value.(*policyItem)
		p.protected.Remove(element)
		p.protectedBytes -= item.cost
		item.segment = probation
		p.nodes[item.key] = p.probation.PushFront(item)
	}
}

func (p *slru) removeElement(element *list.Element) {
	item := element.Value.(*policyItem)
	if item.segment == protected {
		p.protected.Remove(element)
		p.protectedBytes -= item.cost
	} else {
		p.probation.Remove(element)
	}
	delete(p.nodes, item.key)
}

func candidateFromList(values *list.List, exclusion policyExclusion) policyCandidate {
	for element := values.Back(); element != nil; element = element.Prev() {
		item := element.Value.(*policyItem)
		if exclusion.enabled && item.key == exclusion.key && item.generation == exclusion.generation {
			continue
		}
		return policyCandidate{
			key:        item.key,
			generation: item.generation,
			cost:       item.cost,
			ok:         true,
		}
	}
	return policyCandidate{}
}
