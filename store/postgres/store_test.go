package postgres

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/BernardoCSACarreira/canal/pkg/store"
	"github.com/BernardoCSACarreira/canal/pkg/storetest"
)

// dsnEnv names the database the tests run against. CI sets it against a service container; locally
// a disposable one does: docker run --rm -e POSTGRES_PASSWORD=canal -e POSTGRES_USER=canal
// -e POSTGRES_DB=canal -p 5433:5432 postgres:16-alpine
const dsnEnv = "CANAL_POSTGRES_TEST_URL"

// testDSN returns a DSN scoped to a FRESH SCHEMA, because storetest's cases construct a new store
// per case and expect an empty world: schema-per-store is this module's equivalent of the wal
// suite's t.TempDir. The schema rides the DSN's search_path, so Open needs no test-only parameter.
func testDSN(t *testing.T) string {
	t.Helper()
	base := os.Getenv(dsnEnv)
	if base == "" {
		t.Skipf("%s is not set; the conformance suite needs a real database (CI provides one)", dsnEnv)
	}
	name := fmt.Sprintf("canal_test_%08x", rand.Uint32())

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatalf("connecting to create a test schema: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE SCHEMA `+name); err != nil {
		t.Fatalf("creating schema %s: %v", name, err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("closing the setup connection: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, base)
		if err != nil {
			return
		}
		_, _ = conn.Exec(ctx, `DROP SCHEMA `+name+` CASCADE`)
		_ = conn.Close(ctx)
	})

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + "options=-csearch_path%3D" + name
}

func TestConformance(t *testing.T) {
	storetest.Run(t, storetest.Subject{
		Name: "postgres",
		New: func(t *testing.T) store.StateStore {
			s, err := Open(context.Background(), testDSN(t))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			t.Cleanup(func() { _ = s.Close() })
			return s
		},
		// Reopen against the SAME schema: close the pool, connect fresh, and the floors and rows
		// must all still be there — for this store that exercises the round trip through real SQL
		// rather than an in-memory index.
		Reopen: func(t *testing.T, s store.StateStore) store.StateStore {
			ps, ok := s.(*Store)
			if !ok {
				t.Fatalf("reopening a %T", s)
			}
			dsn := ps.pool.Config().ConnString()
			if err := ps.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			next, err := Open(context.Background(), dsn)
			if err != nil {
				t.Fatalf("reopening: %v", err)
			}
			t.Cleanup(func() { _ = next.Close() })
			return next
		},
	})
}

// TestASchemaFromTheFutureIsRefusedByName pins the migration window's loud side: a database whose
// version this build does not know is a refusal naming both versions and the way forward, not a
// guess about what the newer columns mean.
func TestASchemaFromTheFutureIsRefusedByName(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	s, err := Open(ctx, dsn)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `UPDATE canal_meta SET version = 9000`); err != nil {
		t.Fatalf("forging a future version: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	_, err = Open(ctx, dsn)
	if err == nil {
		t.Fatal("a schema at version 9000 opened under a build that knows fewer migrations")
	}
	for _, want := range []string{"9000", "newer build"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q does not say %q", err, want)
		}
	}
}

// TestMigrationIsIdempotentAcrossConcurrentOpens: two stores opening one empty schema race the
// migration; the advisory lock serialises them and both arrive at the same version.
func TestMigrationIsIdempotentAcrossConcurrentOpens(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()

	type result struct {
		s   *Store
		err error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			s, err := Open(ctx, dsn)
			results <- result{s, err}
		}()
	}
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent Open: %v", r.err)
		}
		defer r.s.Close()
	}
}
