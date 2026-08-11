package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The schema is versioned from its first migration — ADR 0033 rule 6, in ADR 0032's shape: a
// version marker read at open, migrate forward mechanically, refuse loudly outside the window
// naming the way forward. "Forward" here is stepwise: each entry in migrations is one version, a
// store at version N applies entries N+1..len(migrations) in order, and a binary whose list is
// SHORTER than the stored version refuses, because it cannot know what the newer schema means.
//
// Every entry must be transactional DDL (Postgres DDL is), because the whole walk happens inside
// one transaction under an advisory lock: two workers opening the same database concurrently
// serialize, and a crash mid-migration leaves the schema at the version it started from.
var migrations = []string{
	// Version 1: the state table.
	//
	// One row per key, structured columns rather than Key.String — that string is documented as
	// not being a storage encoding. A DELETE IS A TOMBSTONE, not a row removal: the epoch column
	// is the fence floor and it must outlive the key (pkg/storetest's delete_floors_survive_reopen
	// is the contract), so live=false marks removal while the floor stays. Version resets with the
	// row — a recreated key starts at 1 — which is the semantic the wal store exhibits and the
	// conformance suite pins.
	`CREATE TABLE canal_state (
	    tenant  text   NOT NULL,
	    space   text   NOT NULL,
	    parts   text[] NOT NULL,
	    value   bytea,
	    version bigint NOT NULL DEFAULT 0,
	    epoch   bigint NOT NULL DEFAULT 0,
	    live    boolean NOT NULL,
	    PRIMARY KEY (tenant, space, parts)
	);`,
}

// advisoryLockKey serialises concurrent migrations of one database. The value is arbitrary and
// permanent: changing it lets two builds migrate concurrently, which is the race the lock exists
// to remove.
const advisoryLockKey = int64(0x63616e616c) // "canal"

// migrate walks the schema forward to this build's version, inside one transaction under an
// advisory lock, and refuses a schema from the future by name.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("postgres: beginning the migration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, advisoryLockKey); err != nil {
		return fmt.Errorf("postgres: taking the migration lock: %w", err)
	}

	// The meta table exists unconditionally and a missing row means version zero. Probing for the
	// table instead — catch undefined_table and carry on — does not work inside a transaction:
	// Postgres aborts the whole transaction on any error, so the tolerated probe poisons every
	// statement after it.
	if _, err := tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS canal_meta (
	    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
	    version   bigint NOT NULL
	)`); err != nil {
		return fmt.Errorf("postgres: ensuring the meta table: %w", err)
	}

	var current int64
	err = tx.QueryRow(ctx, `SELECT version FROM canal_meta`).Scan(&current)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("postgres: reading the schema version: %w", err)
	}

	switch {
	case current == int64(len(migrations)):
		return tx.Commit(ctx)
	case current > int64(len(migrations)):
		return fmt.Errorf("postgres: the schema is at version %d, written by a newer build; this "+
			"binary knows versions up to %d and cannot guess what the newer columns mean — run the "+
			"newer build, or restore the database from before its migration", current, len(migrations))
	}

	for v := current; v < int64(len(migrations)); v++ {
		if _, err := tx.Exec(ctx, migrations[v]); err != nil {
			return fmt.Errorf("postgres: applying schema migration %d: %w", v+1, err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO canal_meta (version) VALUES ($1)
	    ON CONFLICT (singleton) DO UPDATE SET version = $1`, len(migrations)); err != nil {
		return fmt.Errorf("postgres: recording schema version %d: %w", len(migrations), err)
	}
	return tx.Commit(ctx)
}
