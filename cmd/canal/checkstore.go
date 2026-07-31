package main

import (
	"context"
	"errors"
	"iter"

	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/store/wal"
)

// capsOnlyStore answers what a WAL-backed deployment can promise, and refuses everything else.
//
// SCAFFOLDING, LABELLED (design rule R10). This is not a store. It exists so `canal check` can
// negotiate a spec without opening a state directory, which matters for two reasons: an operator
// should be able to check a spec before the directory exists, and checking a spec must never take
// the exclusive lock off a pipeline that is running.
//
// The negotiation it feeds is nonetheless the REAL one, because [wal.Caps] is the same value the
// opened store reports — pinned by TestCapabilitiesNeedNoOpenStore. Substituting a memory store
// here instead would have reported the tier of a deployment nobody is going to run.
//
// Every data method returns an error naming the violated contract rather than silently succeeding.
// Build promises to do no I/O; if that ever stops being true, this turns it into a loud failure in
// `canal check` instead of a subtly wrong answer.
type capsOnlyStore struct{}

var errCheckIsPure = errors.New(
	"canal check does not open a state store: Build must not perform I/O, and this call means it did")

func (capsOnlyStore) Get(context.Context, []store.Key) (map[string]store.Versioned, error) {
	return nil, errCheckIsPure
}

func (capsOnlyStore) Range(context.Context, store.Key) (iter.Seq2[store.Key, store.Versioned], error) {
	return nil, errCheckIsPure
}

func (capsOnlyStore) Set(context.Context, store.Batch) error { return errCheckIsPure }

func (capsOnlyStore) Delete(context.Context, []store.Key) error { return errCheckIsPure }

func (capsOnlyStore) Capabilities() store.StoreCaps { return wal.Caps() }
