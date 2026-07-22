package store

import "testing"

func TestSLRUPromotionAndVictimOrder(t *testing.T) {
	policy := newSLRU(8)
	policy.insert("a", 1, 4)
	policy.insert("b", 2, 4)

	if got := policy.victim(policyExclusion{}); !got.ok || got.key != "a" {
		t.Fatalf("first victim = %+v", got)
	}
	if !policy.touch("a", 1) {
		t.Fatal("touch a failed")
	}
	if !policy.touch("b", 2) {
		t.Fatal("touch b failed")
	}
	policy.insert("c", 3, 4)
	if got := policy.victim(policyExclusion{}); !got.ok || got.key != "c" {
		t.Fatalf("probation victim = %+v", got)
	}
}

func TestSLRUDemotesProtectedLRUByBytes(t *testing.T) {
	policy := newSLRU(4)
	policy.insert("a", 1, 4)
	policy.insert("b", 2, 4)
	policy.touch("a", 1)
	policy.touch("b", 2)

	if policy.protectedBytes != 4 {
		t.Fatalf("protected bytes = %d", policy.protectedBytes)
	}
	if got := policy.victim(policyExclusion{}); !got.ok || got.key != "a" {
		t.Fatalf("demoted victim = %+v", got)
	}
}

func TestSLRUAllowsOneOversizedProtectedEntry(t *testing.T) {
	policy := newSLRU(4)
	policy.insert("large", 1, 8)
	if !policy.touch("large", 1) {
		t.Fatal("touch failed")
	}
	if policy.protected.Len() != 1 || policy.protectedBytes != 8 {
		t.Fatalf("protected len=%d bytes=%d", policy.protected.Len(), policy.protectedBytes)
	}
}

func TestSLRUIgnoresStaleGeneration(t *testing.T) {
	policy := newSLRU(8)
	policy.insert("a", 2, 4)

	if policy.touch("a", 1) {
		t.Fatal("stale touch accepted")
	}
	if policy.remove("a", 1) {
		t.Fatal("stale remove accepted")
	}
	if !policy.remove("a", 2) {
		t.Fatal("current remove rejected")
	}
	if got := policy.victim(policyExclusion{}); got.ok {
		t.Fatalf("victim after remove = %+v", got)
	}
}

func TestSLRUInsertReplacesGeneration(t *testing.T) {
	policy := newSLRU(8)
	policy.insert("a", 1, 4)
	policy.touch("a", 1)
	policy.insert("a", 2, 6)

	if policy.protectedBytes != 0 {
		t.Fatalf("protected bytes = %d", policy.protectedBytes)
	}
	if got := policy.victim(policyExclusion{}); !got.ok || got.generation != 2 || got.cost != 6 {
		t.Fatalf("victim = %+v", got)
	}
}

func TestSLRUVictimExclusion(t *testing.T) {
	policy := newSLRU(8)
	policy.insert("a", 1, 4)
	policy.insert("b", 2, 4)

	got := policy.victim(policyExclusion{key: "a", generation: 1, enabled: true})
	if !got.ok || got.key != "b" {
		t.Fatalf("victim = %+v", got)
	}
}
