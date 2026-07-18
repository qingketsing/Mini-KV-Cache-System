package store

import "testing"

func TestMemoryStorePutGetDelete(t *testing.T) {
	s := NewMemoryStore()

	s.Put("alpha", []byte("one"))

	value, ok := s.Get("alpha")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(value) != "one" {
		t.Fatalf("expected value one, got %q", string(value))
	}

	if deleted := s.Delete("alpha"); !deleted {
		t.Fatal("expected delete to report existing key")
	}
	if _, ok := s.Get("alpha"); ok {
		t.Fatal("expected key to be removed")
	}
}

func TestMemoryStoreCopiesValues(t *testing.T) {
	s := NewMemoryStore()
	input := []byte("one")

	s.Put("alpha", input)
	input[0] = 'x'

	value, ok := s.Get("alpha")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(value) != "one" {
		t.Fatalf("expected stored value to be isolated, got %q", string(value))
	}

	value[0] = 'y'
	again, ok := s.Get("alpha")
	if !ok {
		t.Fatal("expected key to exist")
	}
	if string(again) != "one" {
		t.Fatalf("expected returned value to be isolated, got %q", string(again))
	}
}
