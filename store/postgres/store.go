// Package postgres is the coordinated deployment's [store.StateStore]: one SQL transaction per
// Set, per-key compare-and-set, per-key epoch fencing, and the first store in canal whose
// durability domain is [connector.DurabilityCluster] — the bytes survive node loss and another
// worker can read them.
//
// It lives in its own Go module, deliberately (ADR 0033): the root module advertises zero
// third-party dependencies and CI enforces that mechanically, so the one dependency ADR 0021
// accepted — the Postgres driver — lives here, where the root cannot import it without failing its
// own guard.
//
// # Semantics, and where they come from
//
// The contract is [pkg/storetest], the same suite the wal and memstore implementations run, and
// two of its cases dictate this store's shape:
//
//   - A DELETE IS A TOMBSTONE. The fence floor a delete raises must outlive the key — across a
//     reopen and across whatever a store does to reclaim space — so a removal sets live=false and
//     keeps the row's epoch. A recreated key's version restarts at 1, matching the wal store.
//   - EVERYTHING RETURNED IS THE CALLER'S. Rows come off the wire copied by construction; Key.Parts
//     are fresh slices per row.
//
// Set takes row locks (SELECT ... FOR UPDATE) on every touched key, checks every precondition,
// then applies — the same two-pass shape as the wal store, so a refused batch changes nothing and
// the refusal classifies identically: [fault.ErrFenced] for a stale epoch, a contract fault for a
// version mismatch.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BernardoCSACarreira/canal/pkg/connector"
	"github.com/BernardoCSACarreira/canal/pkg/fault"
	"github.com/BernardoCSACarreira/canal/pkg/store"
)

// Store is a Postgres-backed [store.StateStore].
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to dsn, migrates the schema forward if it is behind (refusing loudly if it is
// ahead — see schema.go), and returns the store. The DSN chooses the database and, through
// search_path, the schema; two pipelines sharing a database are separated by tenant inside the
// tables, exactly as the key model prescribes.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parsing the connection configuration: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: reaching the database: %w", err)
	}
	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// Get reads several keys. A tombstone is absent: the row exists to carry a floor, not a value.
func (s *Store) Get(ctx context.Context, keys []store.Key) (map[string]store.Versioned, error) {
	out := make(map[string]store.Versioned, len(keys))
	for _, k := range keys {
		var value []byte
		var version int64
		err := s.pool.QueryRow(ctx,
			`SELECT value, version FROM canal_state
			  WHERE tenant=$1 AND space=$2 AND parts=$3 AND live`,
			string(k.Tenant), string(k.Space), partsOf(k)).Scan(&value, &version)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fault.Unknown(fault.OpRead, fmt.Errorf("postgres: reading %s: %w", k, err))
		}
		out[k.String()] = store.Versioned{Key: keyCopy(k), Value: value, Version: uint64(version)}
	}
	return out, nil
}

// Range iterates every live key under a prefix, in Key.String order — the order the other stores
// yield, so a consumer cannot come to depend on SQL collation by accident. The result set is
// snapshotted before the first yield, on the same reasoning as the in-memory store: an iterator
// holding a connection open across arbitrary caller code is a resource leak shaped like an API.
func (s *Store) Range(ctx context.Context, prefix store.Key) (iter.Seq2[store.Key, store.Versioned], error) {
	rows, err := s.pool.Query(ctx,
		`SELECT parts, value, version FROM canal_state
		  WHERE tenant=$1 AND space=$2 AND parts[1:cardinality($3::text[])]=$3 AND live`,
		string(prefix.Tenant), string(prefix.Space), partsOf(prefix))
	if err != nil {
		return nil, fault.Unknown(fault.OpRead, fmt.Errorf("postgres: ranging %s: %w", prefix, err))
	}
	defer rows.Close()

	var snapshot []store.Versioned
	for rows.Next() {
		var parts []string
		var value []byte
		var version int64
		if err := rows.Scan(&parts, &value, &version); err != nil {
			return nil, fault.Unknown(fault.OpRead, fmt.Errorf("postgres: scanning a row: %w", err))
		}
		snapshot = append(snapshot, store.Versioned{
			Key:     store.Key{Tenant: prefix.Tenant, Space: prefix.Space, Parts: parts},
			Value:   value,
			Version: uint64(version),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fault.Unknown(fault.OpRead, fmt.Errorf("postgres: ranging %s: %w", prefix, err))
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Key.String() < snapshot[j].Key.String()
	})

	return func(yield func(store.Key, store.Versioned) bool) {
		for _, v := range snapshot {
			if !yield(v.Key, v) {
				return
			}
		}
	}, nil
}

// Set applies one batch atomically: row locks on every touched key, every precondition checked,
// then every mutation applied, one transaction. Commit is the durability point — Postgres fsyncs
// its own WAL before acknowledging — which is what FlushIsDurable promises.
func (s *Store) Set(ctx context.Context, w store.Batch) error {
	if err := boundsCheck(w); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: beginning: %w", err))
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Pass one: lock and check. Locking in sorted key order keeps two concurrent batches over the
	// same keys from deadlocking; they serialize instead, and the loser's preconditions see the
	// winner's world.
	type rowState struct {
		exists  bool // a row is present, live or tombstone
		live    bool
		version int64
		epoch   int64
	}
	names := make([]string, 0, len(w.Writes))
	for name := range w.Writes {
		names = append(names, name)
	}
	sort.Strings(names)

	current := map[string]rowState{}
	lock := func(k store.Key) (rowState, error) {
		name := k.String()
		if st, ok := current[name]; ok {
			return st, nil
		}
		var st rowState
		err := tx.QueryRow(ctx,
			`SELECT live, version, epoch FROM canal_state
			  WHERE tenant=$1 AND space=$2 AND parts=$3 FOR UPDATE`,
			string(k.Tenant), string(k.Space), partsOf(k)).Scan(&st.live, &st.version, &st.epoch)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
		case err != nil:
			return st, fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: locking %s: %w", k, err))
		default:
			st.exists = true
		}
		current[name] = st
		return st, nil
	}

	for _, name := range names {
		v := w.Writes[name]
		st, err := lock(v.Key)
		if err != nil {
			return err
		}
		if st.epoch > 0 && int64(w.EpochFor(v)) < st.epoch {
			return fault.ErrFenced
		}
		liveVersion := int64(0)
		if st.live {
			liveVersion = st.version
		}
		switch {
		case v.IfVersion == 0 && st.live:
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("postgres: %s exists but the write required it not to", name))
		case v.IfVersion != 0 && (!st.live || liveVersion != int64(v.IfVersion)):
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("postgres: %s is at version %d, not %d", name, liveVersion, v.IfVersion))
		}
	}
	for _, d := range w.Deletes {
		st, err := lock(d.Key)
		if err != nil {
			return err
		}
		if st.epoch > 0 && int64(w.EpochForDelete(d)) < st.epoch {
			return fault.ErrFenced
		}
	}

	// Pass two: apply, writes then deletes, matching the wal store's order so a key both written
	// and deleted in one batch ends deleted.
	for _, name := range names {
		v := w.Writes[name]
		st := current[name]
		epoch := int64(w.EpochFor(v))
		if st.epoch > epoch {
			epoch = st.epoch
		}
		version := int64(1)
		if st.live {
			version = st.version + 1
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO canal_state (tenant, space, parts, value, version, epoch, live)
			 VALUES ($1,$2,$3,$4,$5,$6,true)
			 ON CONFLICT (tenant, space, parts)
			 DO UPDATE SET value=$4, version=$5, epoch=$6, live=true`,
			string(v.Key.Tenant), string(v.Key.Space), partsOf(v.Key),
			v.Value, version, epoch); err != nil {
			return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: writing %s: %w", name, err))
		}
		current[name] = rowState{exists: true, live: true, version: version, epoch: epoch}
	}
	for _, d := range w.Deletes {
		name := d.Key.String()
		st := current[name]
		epoch := int64(w.EpochForDelete(d))
		if st.epoch > epoch {
			epoch = st.epoch
		}
		// THE TOMBSTONE IS THE FLOOR: live drops, the value goes, the version resets, the epoch
		// stays — which is what lets delete_floors_survive_reopen hold against a real database.
		if _, err := tx.Exec(ctx,
			`INSERT INTO canal_state (tenant, space, parts, value, version, epoch, live)
			 VALUES ($1,$2,$3,NULL,0,$4,false)
			 ON CONFLICT (tenant, space, parts)
			 DO UPDATE SET value=NULL, version=0, epoch=$4, live=false`,
			string(d.Key.Tenant), string(d.Key.Space), partsOf(d.Key), epoch); err != nil {
			return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: deleting %s: %w", name, err))
		}
		current[name] = rowState{exists: true, live: false, version: 0, epoch: epoch}
	}

	if err := tx.Commit(ctx); err != nil {
		return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: committing: %w", err))
	}
	return nil
}

// Delete removes keys unconditionally, durably. Unconditional means no epoch to compare — but an
// existing floor still survives as a tombstone, because unconditional removal is not permission
// for a superseded worker to recreate what it removed.
func (s *Store) Delete(ctx context.Context, keys []store.Key) error {
	if len(keys) == 0 {
		return nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: beginning: %w", err))
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, k := range keys {
		if _, err := tx.Exec(ctx,
			`UPDATE canal_state SET value=NULL, version=0, live=false
			  WHERE tenant=$1 AND space=$2 AND parts=$3`,
			string(k.Tenant), string(k.Space), partsOf(k)); err != nil {
			return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: deleting %s: %w", k, err))
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fault.Unknown(fault.OpPersist, fmt.Errorf("postgres: committing: %w", err))
	}
	return nil
}

// Capabilities reports what this store can honestly promise.
func (s *Store) Capabilities() store.StoreCaps { return Caps() }

// Caps is what every Postgres store promises, as a value that needs no open store — the same
// pure-negotiation reasoning as the wal store's package-level Caps.
func Caps() store.StoreCaps {
	return store.StoreCaps{
		AtomicMultiKey: true,
		CAS:            true,
		EpochFencing:   true,
		// The first shipped store whose bytes survive node loss and are readable by another
		// worker, which is what the coordinated deployment is FOR.
		Durability:     connector.DurabilityCluster,
		FlushIsDurable: true,
	}
}

// boundsCheck refuses the epochs and versions bigint cannot hold, loudly, instead of wrapping
// silently at 2^63 — a number no real lease sequence reaches, so hitting this is a caller bug
// worth naming.
func boundsCheck(w store.Batch) error {
	over := func(v uint64) bool { return v > math.MaxInt64 }
	if over(w.Epoch) {
		return fault.Contract(fault.OpPersist,
			fmt.Errorf("postgres: batch epoch %d exceeds the storable range", w.Epoch))
	}
	for name, v := range w.Writes {
		if over(v.IfVersion) || over(w.EpochFor(v)) {
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("postgres: %s carries a version or epoch beyond the storable range", name))
		}
	}
	for _, d := range w.Deletes {
		if over(w.EpochForDelete(d)) {
			return fault.Contract(fault.OpPersist,
				fmt.Errorf("postgres: a delete of %s carries an epoch beyond the storable range", d.Key))
		}
	}
	return nil
}

func partsOf(k store.Key) []string {
	if k.Parts == nil {
		return []string{}
	}
	return k.Parts
}

func keyCopy(k store.Key) store.Key {
	out := store.Key{Tenant: k.Tenant, Space: k.Space}
	if len(k.Parts) > 0 {
		out.Parts = append([]string(nil), k.Parts...)
	}
	return out
}
