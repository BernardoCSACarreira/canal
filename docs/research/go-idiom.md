# Prior art: Go stdlib idiom for driver/plugin systems

> Interface archaeology on the Go standard library itself, plus `golang.org/x/sync`. The question is
> not "what features do these packages have" but **how does Go express an extensible driver system so
> that it feels like Go**.

## Scope and provenance

Everything quoted below was read from source on this machine, not recalled:

| What | Path (all under `/opt/homebrew/opt/go/libexec/src/`) |
|---|---|
| `database/sql/driver` | `database/sql/driver/driver.go`, `database/sql/driver/types.go` |
| `database/sql` | `database/sql/sql.go`, `database/sql/ctxutil.go`, `database/sql/convert.go` |
| `io` | `io/io.go` |
| `iter` | `iter/iter.go` |
| `errors` | `errors/wrap.go` |
| `context` | `context/context.go` |
| `net/http` | `net/http/server.go`, `net/http/responsecontroller.go` |
| `net` | `net/net.go` |
| `image` | `image/format.go` |
| `crypto` | `crypto/crypto.go` |
| `log/slog` | `log/slog/handler.go` |
| `encoding` | `encoding/encoding.go` |
| `flag` | `flag/flag.go` |
| `bufio` | `bufio/bufio.go` |
| `errgroup`, `semaphore` | `cmd/vendor/golang.org/x/sync/errgroup/errgroup.go`, `.../semaphore/semaphore.go` |

**Version: `go1.23.6` (`time 2025-01-31T18:38:03Z`)**, read from `/opt/homebrew/opt/go/libexec/VERSION`.
A second toolchain at `/usr/local/go` is `go1.20.7`; I used the 1.23 tree throughout because canal
targets Go 1.23+. Where the two differ the difference is cosmetic (1.23 adds `[...]` doc links).

`golang.org/x/sync/errgroup` is quoted from the copy **vendored inside the Go distribution**
(`src/cmd/vendor/...`), which is primary source but is pinned to whatever revision this Go release
vendored — it may lag the upstream module.

**Not verified — see the `unverified` list.** Web fetch was unavailable for the entire session
(the tool-safety classifier was down, and it never recovered), so `hashicorp/go-plugin` could
**not** be fetched. Section 10 therefore describes the in-process/out-of-process shim pattern using
*locally verified stdlib examples of the same pattern*, and explicitly refuses to quote go-plugin
type signatures. Do not trust any go-plugin API shape from this document.

---

## 1. Core interfaces

### 1.1 The layering: `Driver` → `Connector` → `Conn` → `Stmt` → `Rows`/`Result`/`Tx`

`database/sql/driver` is the closest thing in the stdlib to canal's problem: a core engine that knows
nothing about any concrete backend, driving pluggable implementations. Its whole **required** surface
is five interfaces totalling **11 methods**.

The entry point is one method:

```go
// Driver is the interface that must be implemented by a database
// driver.
//
// Database drivers may implement [DriverContext] for access
// to contexts and to parse the name only once for a pool of connections,
// instead of once per connection.
type Driver interface {
	// Open returns a new connection to the database.
	// The name is a string in a driver-specific format.
	//
	// Open may return a cached connection (one previously
	// closed), but doing so is unnecessary; the sql package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Open(name string) (Conn, error)
}
```

A configured factory, separated from the driver identity:

```go
// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
//
// A Connector can be passed to [database/sql.OpenDB], to allow drivers
// to implement their own [database/sql.DB] constructors, or returned by
// [DriverContext]'s OpenConnector method, to allow drivers
// access to context and to avoid repeated parsing of driver
// configuration.
//
// If a Connector implements [io.Closer], the [database/sql.DB.Close]
// method will call the Close method and return error (if any).
type Connector interface {
	// Connect returns a connection to the database.
	// Connect may return a cached connection (one previously
	// closed), but doing so is unnecessary; the sql package
	// maintains a pool of idle connections for efficient re-use.
	//
	// The provided context.Context is for dialing purposes only
	// (see net.DialContext) and should not be stored or used for
	// other purposes. A default timeout should still be used
	// when dialing as a connection pool may call Connect
	// asynchronously to any query.
	//
	// The returned connection is only used by one goroutine at a
	// time.
	Connect(context.Context) (Conn, error)

	// Driver returns the underlying Driver of the Connector,
	// mainly to maintain compatibility with the Driver method
	// on sql.DB.
	Driver() Driver
}
```

Note two things that matter enormously for canal:

1. **`Connect(context.Context)` takes a context but the doc restricts its meaning**: "The provided
   context.Context is for dialing purposes only ... and should not be stored or used for other
   purposes." The stdlib found it necessary to *narrow the semantics of the context parameter in
   prose*, because a setup context and an operating context are different things.
2. **A single-goroutine concurrency contract is stated on the interface, not enforced.** "The
   returned connection is only used by one goroutine at a time." The core guarantees it; the
   implementer may therefore be lock-free. This is a *cheap* way to buy simple connectors.

The stateful handle:

```go
// Conn is a connection to a database. It is not used concurrently
// by multiple goroutines.
//
// Conn is assumed to be stateful.
type Conn interface {
	// Prepare returns a prepared statement, bound to this connection.
	Prepare(query string) (Stmt, error)

	// Close invalidates and potentially stops any current
	// prepared statements and transactions, marking this
	// connection as no longer in use.
	//
	// Because the sql package maintains a free pool of
	// connections and only calls Close when there's a surplus of
	// idle connections, it shouldn't be necessary for drivers to
	// do their own connection caching.
	//
	// Drivers must ensure all network calls made by Close
	// do not block indefinitely (e.g. apply a timeout).
	Close() error

	// Begin starts and returns a new transaction.
	//
	// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
	Begin() (Tx, error)
}
```

`Drivers must ensure all network calls made by Close do not block indefinitely (e.g. apply a
timeout).` — a hard obligation on the implementer that the core cannot enforce. Canal will need the
same clause on sink `Close`/`Flush`.

The write side and the read side:

```go
// Stmt is a prepared statement. It is bound to a [Conn] and not
// used by multiple goroutines concurrently.
type Stmt interface {
	// Close closes the statement.
	// ...
	Close() error

	// NumInput returns the number of placeholder parameters.
	//
	// If NumInput returns >= 0, the sql package will sanity check
	// argument counts from callers and return errors to the caller
	// before the statement's Exec or Query methods are called.
	//
	// NumInput may also return -1, if the driver doesn't know
	// its number of placeholders. In that case, the sql package
	// will not sanity check Exec or Query argument counts.
	NumInput() int

	// Exec executes a query that doesn't return rows, such
	// as an INSERT or UPDATE.
	//
	// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
	Exec(args []Value) (Result, error)

	// Query executes a query that may return rows, such as a
	// SELECT.
	//
	// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
	Query(args []Value) (Rows, error)
}

// Rows is an iterator over an executed query's results.
type Rows interface {
	// Columns returns the names of the columns. The number of
	// columns of the result is inferred from the length of the
	// slice. If a particular column name isn't known, an empty
	// string should be returned for that entry.
	Columns() []string

	// Close closes the rows iterator.
	Close() error

	// Next is called to populate the next row of data into
	// the provided slice. The provided slice will be the same
	// size as the Columns() are wide.
	//
	// Next should return io.EOF when there are no more rows.
	//
	// The dest should not be written to outside of Next. Care
	// should be taken when closing Rows not to modify
	// a buffer held in dest.
	Next(dest []Value) error
}

// Result is the result of a query execution.
type Result interface {
	// LastInsertId returns the database's auto-generated ID
	// after, for example, an INSERT into a table with primary
	// key.
	LastInsertId() (int64, error)

	// RowsAffected returns the number of rows affected by the
	// query.
	RowsAffected() (int64, error)
}

// Tx is a transaction.
type Tx interface {
	Commit() error
	Rollback() error
}
```

`NumInput() int` returning `-1` for "I don't know" is a **negative-sentinel capability probe**: a
required method that lets the implementer decline the feature without a second interface. Cheaper
than an optional interface when the answer is a scalar.

### 1.2 The capability-upgrade pattern: how the required surface stays tiny

This is the central mechanism and the single most important thing to steal.

`driver.go` opens with what is effectively a conformance checklist written as prose, because the
compiler cannot express it:

```go
// The driver interface has evolved over time. Drivers should implement
// [Connector] and [DriverContext] interfaces.
// The Connector.Connect and Driver.Open methods should never return [ErrBadConn].
// [ErrBadConn] should only be returned from [Validator], [SessionResetter], or
// a query method if the connection is already in an invalid (e.g. closed) state.
//
// All [Conn] implementations should implement the following interfaces:
// [Pinger], [SessionResetter], and [Validator].
//
// If named parameters or context are supported, the driver's [Conn] should implement:
// [ExecerContext], [QueryerContext], [ConnPrepareContext], and [ConnBeginTx].
//
// To support custom data types, implement [NamedValueChecker]. [NamedValueChecker]
// also allows queries to accept per-query options as a parameter by returning
// [ErrRemoveArgument] from CheckNamedValue.
//
// If multiple result sets are supported, [Rows] should implement [RowsNextResultSet].
// If the driver knows how to describe the types present in the returned result
// it should implement the following interfaces: [RowsColumnTypeScanType],
// [RowsColumnTypeDatabaseTypeName], [RowsColumnTypeLength], [RowsColumnTypeNullable],
// and [RowsColumnTypePrecisionScale]. A given row value may also return a [Rows]
// type, which may represent a database cursor value.
```

Counting the package: **7 structural interfaces** (`Driver`, `Connector`, `Conn`, `Stmt`, `Rows`,
`Result`, `Tx`) and **20 optional capability interfaces**:

| Attached to | Optional interface | Method |
|---|---|---|
| `Driver` | `DriverContext` | `OpenConnector(name string) (Connector, error)` |
| `Conn` | `Pinger` | `Ping(ctx context.Context) error` |
| `Conn` | `Execer` *(deprecated)* | `Exec(query string, args []Value) (Result, error)` |
| `Conn` | `ExecerContext` | `ExecContext(ctx context.Context, query string, args []NamedValue) (Result, error)` |
| `Conn` | `Queryer` *(deprecated)* | `Query(query string, args []Value) (Rows, error)` |
| `Conn` | `QueryerContext` | `QueryContext(ctx context.Context, query string, args []NamedValue) (Rows, error)` |
| `Conn` | `ConnPrepareContext` | `PrepareContext(ctx context.Context, query string) (Stmt, error)` |
| `Conn` | `ConnBeginTx` | `BeginTx(ctx context.Context, opts TxOptions) (Tx, error)` |
| `Conn` | `SessionResetter` | `ResetSession(ctx context.Context) error` |
| `Conn` | `Validator` | `IsValid() bool` |
| `Conn`/`Stmt` | `NamedValueChecker` | `CheckNamedValue(*NamedValue) error` |
| `Stmt` | `StmtExecContext` | `ExecContext(ctx context.Context, args []NamedValue) (Result, error)` |
| `Stmt` | `StmtQueryContext` | `QueryContext(ctx context.Context, args []NamedValue) (Rows, error)` |
| `Stmt` | `ColumnConverter` *(deprecated)* | `ColumnConverter(idx int) ValueConverter` |
| `Rows` | `RowsNextResultSet` | `HasNextResultSet() bool` / `NextResultSet() error` |
| `Rows` | `RowsColumnTypeScanType` | `ColumnTypeScanType(index int) reflect.Type` |
| `Rows` | `RowsColumnTypeDatabaseTypeName` | `ColumnTypeDatabaseTypeName(index int) string` |
| `Rows` | `RowsColumnTypeLength` | `ColumnTypeLength(index int) (length int64, ok bool)` |
| `Rows` | `RowsColumnTypeNullable` | `ColumnTypeNullable(index int) (nullable, ok bool)` |
| `Rows` | `RowsColumnTypePrecisionScale` | `ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool)` |

**Ratio: 11 required methods, 20 optional interfaces.** A minimal working driver is ~11 methods; a
world-class driver implements ~30. That spread is the design.

**The core-side machinery is a type assertion and a fallback.** `ctxutil.go` exists for nothing else.
Every function in it is the same three-line shape — try the rich interface, else degrade:

```go
func ctxDriverPrepare(ctx context.Context, ci driver.Conn, query string) (driver.Stmt, error) {
	if ciCtx, is := ci.(driver.ConnPrepareContext); is {
		return ciCtx.PrepareContext(ctx, query)
	}
	si, err := ci.Prepare(query)
	if err == nil {
		select {
		default:
		case <-ctx.Done():
			si.Close()
			return nil, ctx.Err()
		}
	}
	return si, err
}
```

Note the consolation prize: when the driver has no context support, the core *still* honours
cancellation as best it can, by checking `ctx.Done()` after the blocking call and undoing the work
(`si.Close()`). **Degrading a capability is not the same as dropping the guarantee.**

`ConnBeginTx` shows the other half — when a capability is missing, options that depend on it must be
*rejected*, not silently ignored:

```go
func ctxDriverBegin(ctx context.Context, opts *TxOptions, ci driver.Conn) (driver.Tx, error) {
	if ciCtx, is := ci.(driver.ConnBeginTx); is {
		dopts := driver.TxOptions{}
		if opts != nil {
			dopts.Isolation = driver.IsolationLevel(opts.Isolation)
			dopts.ReadOnly = opts.ReadOnly
		}
		return ciCtx.BeginTx(ctx, dopts)
	}

	if opts != nil {
		// Check the transaction level. If the transaction level is non-default
		// then return an error here as the BeginTx driver value is not supported.
		if opts.Isolation != LevelDefault {
			return nil, errors.New("sql: driver does not support non-default isolation level")
		}

		// If a read-only transaction is requested return an error as the
		// BeginTx driver value is not supported.
		if opts.ReadOnly {
			return nil, errors.New("sql: driver does not support read-only transactions")
		}
	}
	...
}
```

This is exactly canal's R4-shaped hazard in miniature: *never let a requested guarantee be
downgraded silently*. If a sink cannot do transactional commit and the pipeline asked for
exactly-once, that is an error at configure time, not a quieter delivery guarantee at runtime.

**Three-level fallback chains** appear where the interface evolved twice. `DB.execDC` tries
`ExecerContext`, then `Execer`, then generic prepare-exec-close:

```go
func (db *DB) execDC(ctx context.Context, dc *driverConn, release func(error), query string, args []any) (res Result, err error) {
	defer func() {
		release(err)
	}()
	execerCtx, ok := dc.ci.(driver.ExecerContext)
	var execer driver.Execer
	if !ok {
		execer, ok = dc.ci.(driver.Execer)
	}
	if ok {
		var nvdargs []driver.NamedValue
		var resi driver.Result
		withLock(dc, func() {
			nvdargs, err = driverArgsConnLocked(dc.ci, nil, args)
			if err != nil {
				return
			}
			resi, err = ctxDriverExec(ctx, execerCtx, execer, query, nvdargs)
		})
		if err != driver.ErrSkip {
			if err != nil {
				return nil, err
			}
			return driverResult{dc, resi}, nil
		}
	}

	var si driver.Stmt
	withLock(dc, func() {
		si, err = ctxDriverPrepare(ctx, dc.ci, query)
	})
	if err != nil {
		return nil, err
	}
	ds := &driverStmt{Locker: dc, si: si}
	defer ds.Close()
	return resultFromStatement(ctx, dc.ci, ds, args...)
}
```

### 1.3 `ErrSkip`: declining a capability at *runtime*, per call

The type assertion answers "can you, in principle". `ErrSkip` answers "can you, for *this* call".

```go
// ErrSkip may be returned by some optional interfaces' methods to
// indicate at runtime that the fast path is unavailable and the sql
// package should continue as if the optional interface was not
// implemented. ErrSkip is only supported where explicitly
// documented.
var ErrSkip = errors.New("driver: skip fast-path; continue as if unimplemented")
```

"continue as if the optional interface was not implemented" — one sentinel converts a static
capability into a dynamic one. A driver can implement `ExecerContext` for the 90% of statements it
can fast-path and return `ErrSkip` for the rest. Note the discipline: `ErrSkip is only supported
where explicitly documented` — it is not a blanket protocol.

For canal: a sink can advertise `BatchWriter` and return `ErrSkip` for a batch containing a record
it cannot batch, falling back to the per-record path, **without the core knowing why**.

### 1.4 Two capabilities read as a *conjunction* to unlock a third behaviour

The most subtle use in the package. `beginDC` decides whether it is safe to keep a connection after a
rollback, and the decision is "does the driver implement *both* of the connection-hygiene
interfaces":

```go
func (db *DB) beginDC(ctx context.Context, dc *driverConn, release func(error), opts *TxOptions) (tx *Tx, err error) {
	var txi driver.Tx
	keepConnOnRollback := false
	withLock(dc, func() {
		_, hasSessionResetter := dc.ci.(driver.SessionResetter)
		_, hasConnectionValidator := dc.ci.(driver.Validator)
		keepConnOnRollback = hasSessionResetter && hasConnectionValidator
		txi, err = ctxDriverBegin(ctx, opts, dc.ci)
	})
	...
}
```

The core derives a *policy* from a *capability set*. Canal's engine should do the same: whether to
enable a parallel snapshot, whether to keep a checkpoint after an error, whether to offer
exactly-once — all derived from which optional interfaces the pair of connectors satisfies, computed
once at pipeline build time.

### 1.5 The same idiom at its smallest: `io`, `net/http`, `log/slog`

`io` is the extreme end — one method per interface, composition by embedding:

```go
type Reader interface {
	Read(p []byte) (n int, err error)
}

type Writer interface {
	Write(p []byte) (n int, err error)
}

type Closer interface {
	Close() error
}

type Seeker interface {
	Seek(offset int64, whence int) (int64, error)
}

type ReadWriter interface {
	Reader
	Writer
}

type ReadCloser interface {
	Reader
	Closer
}

type ReadSeekCloser interface {
	Reader
	Seeker
	Closer
}
```

There are 9 such composite interfaces in `io.go` built from 4 primitives, declared by embedding and
nothing else. No implementation anywhere declares that it implements them.

The performance capabilities are separate, optional, and asserted for by the *algorithm*, not the
type:

```go
// ReaderFrom is the interface that wraps the ReadFrom method.
//
// ReadFrom reads data from r until EOF or error.
// The return value n is the number of bytes read.
// Any error except EOF encountered during the read is also returned.
//
// The [Copy] function uses [ReaderFrom] if available.
type ReaderFrom interface {
	ReadFrom(r Reader) (n int64, err error)
}

// WriterTo is the interface that wraps the WriteTo method.
//
// WriteTo writes data to w until there's no more data to write or
// when an error occurs. The return value n is the number of bytes
// written. Any error encountered during the write is also returned.
//
// The Copy function uses WriterTo if available.
type WriterTo interface {
	WriteTo(w Writer) (n int64, err error)
}
```

`io.Copy` is *the* model for canal's pipeline runner — a generic loop that upgrades itself when
either end can do better:

```go
// copyBuffer is the actual implementation of Copy and CopyBuffer.
// if buf is nil, one is allocated.
func copyBuffer(dst Writer, src Reader, buf []byte) (written int64, err error) {
	// If the reader has a WriteTo method, use it to do the copy.
	// Avoids an allocation and a copy.
	if wt, ok := src.(WriterTo); ok {
		return wt.WriteTo(dst)
	}
	// Similarly, if the writer has a ReadFrom method, use it to do the copy.
	if rf, ok := dst.(ReaderFrom); ok {
		return rf.ReadFrom(src)
	}
	if buf == nil {
		size := 32 * 1024
		if l, ok := src.(*LimitedReader); ok && int64(size) > l.N {
			...
		}
		buf = make([]byte, size)
	}
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			...
		}
		if er != nil {
			if er != EOF {
				err = er
			}
			break
		}
	}
	return written, err
}
```

Three observations for canal's engine:

- The **source is asked first** (`WriterTo` before `ReaderFrom`). Precedence between two possible
  fast paths is a decision the core must make and document.
- It even type-asserts a *concrete* type (`*LimitedReader`) to size the buffer. The stdlib is willing
  to break its own abstraction for a measurable win — and this is the sort of thing that becomes a
  maintenance liability, because `*LimitedReader` is now load-bearing.
- `if er != EOF { err = er }` — end-of-stream is not a failure. Canal needs the same crisp separation
  between "source is exhausted" and "source is broken".

`net/http` reduces a whole server extension model to one method plus a func adapter:

```go
type Handler interface {
	ServeHTTP(ResponseWriter, *Request)
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as HTTP handlers. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// [Handler] that calls f.
type HandlerFunc func(ResponseWriter, *Request)

// ServeHTTP calls f(w, r).
func (f HandlerFunc) ServeHTTP(w ResponseWriter, r *Request) {
	f(w, r)
}
```

`HandlerFunc` is the reason middleware in Go is `func(Handler) Handler` and nothing more. Because the
interface has exactly one method, a closure can implement it; because a closure can implement it,
decoration needs no base class, no registration, no framework. **Canal should have the func-adapter
for every single-method interface it defines** (`SourceFunc`, `TransformFunc`, `CodecFunc`).

`log/slog.Handler` is the most recent stdlib pluggable interface (Go 1.21) and shows current taste —
four methods, one of which is a cheap pre-check and two of which return new instances:

```go
type Handler interface {
	// Enabled reports whether the handler handles records at the given level.
	// The handler ignores records whose level is lower.
	// It is called early, before any arguments are processed,
	// to save effort if the log event should be discarded.
	// If called from a Logger method, the first argument is the context
	// passed to that method, or context.Background() if nil was passed
	// or the method does not take a context.
	// The context is passed so Enabled can use its values
	// to make a decision.
	Enabled(context.Context, Level) bool

	// Handle handles the Record.
	// It will only be called when Enabled returns true.
	// The Context argument is as for Enabled.
	// It is present solely to provide Handlers access to the context's values.
	// Canceling the context should not affect record processing.
	// (Among other things, log messages may be necessary to debug a
	// cancellation-related problem.)
	// ...
	Handle(context.Context, Record) error

	// WithAttrs returns a new Handler whose attributes consist of
	// both the receiver's attributes and the arguments.
	// The Handler owns the slice: it may retain, modify or discard it.
	WithAttrs(attrs []Attr) Handler

	// WithGroup returns a new Handler with the given group appended to
	// the receiver's existing groups.
	// ...
	WithGroup(name string) Handler
}
```

Two transplantable ideas: (a) a **cheap predicate method that lets the core skip building an
expensive argument** — canal's sink could expose `Accepts(ctx, RecordMeta) bool` so the engine can
drop or route a record before serializing it; (b) `Handle`'s doc explicitly severs one meaning of the
context: "Canceling the context should not affect record processing." When a context is passed for
*values* and not for *cancellation*, say so on the method.

### 1.6 What this implies for canal's `Source` and `Sink`

Mapping the layering onto canal, keeping the required surface minimal:

- `Driver`/`Connector` → a **registered factory** and a **validated, configured instance**. Two
  types, not one: identity + config parsing happens once (`DriverContext.OpenConnector`), and the
  configured object mints runtime handles. This directly serves canal's "config declared once,
  surfaced to a UI, then frozen".
- `Conn` → a live, **single-goroutine** session against the remote system.
- `Rows` → the source's record stream: `Next(dest)` into a caller-owned buffer, `io.EOF` to end.
- `Stmt`/`Result` → the sink's write handle and its per-write outcome, where **every result field is
  `(value, error)` so "unsupported" is expressible**.
- `Tx` → the sink's optional commit protocol.

Everything else — batching, parallel snapshot, schema description, checkpoint positions, dedup keys,
exactly-once — is an **optional interface the core type-asserts for**, never a field on a required
one.

---

## 2. Record model

### 2.1 `driver.Value`: an open type with a closed, documented type set

```go
// Value is a value that drivers must be able to handle.
// It is either nil, a type handled by a database driver's [NamedValueChecker]
// interface, or an instance of one of these types:
//
//	int64
//	float64
//	bool
//	[]byte
//	string
//	time.Time
//
// If the driver supports cursors, a returned Value may also implement the [Rows] interface
// in this package. This is used, for example, when a user selects a cursor
// such as "select cursor(select * from my_table) from dual". If the [Rows]
// from the select is closed, the cursor [Rows] will also be closed.
type Value any
```

This is the stdlib's canonical record-payload decision and it is worth reading carefully, because
canal's R2 says the canonical record model is decided first.

- The static type is `any`. The *contract* is a **six-member closed set**, enforced at runtime, not
  by the compiler.
- The set is deliberately minimal and lossless-ish: one signed integer width, one float width, bool,
  bytes, string, timestamp. No `int32`, no `uint64`, no decimal, no nested map. Everything else is
  either narrowed into these or carried as `[]byte`.
- There is an **escape hatch that is itself a capability interface**: "a type handled by a database
  driver's `NamedValueChecker` interface". A driver may extend the type set for itself.
- **Recursion is permitted and typed**: a `Value` may itself be a `Rows`. A record field can be a
  nested stream. This is how the stdlib expresses a hierarchical/streaming payload without inventing
  a variant type — and it comes with a documented lifetime rule ("If the Rows from the select is
  closed, the cursor Rows will also be closed").

The runtime enforcement is a plain function plus a converter interface:

```go
// ValueConverter is the interface providing the ConvertValue method.
// ...
type ValueConverter interface {
	// ConvertValue converts a value to a driver Value.
	ConvertValue(v any) (Value, error)
}

// Valuer is the interface providing the Value method.
//
// Errors returned by the [Value] method are wrapped by the database/sql package.
// This allows callers to use [errors.Is] for precise error handling after operations
// like [database/sql.Query], [database/sql.Exec], or [database/sql.QueryRow].
//
// Types implementing Valuer interface are able to convert
// themselves to a driver [Value].
type Valuer interface {
	// Value returns a driver Value.
	// Value must not panic.
	Value() (Value, error)
}
```

`Value must not panic.` and "Errors returned by the Value method are wrapped ... This allows callers
to use `errors.Is`" — the conversion seam is (a) forbidden to panic and (b) required to preserve
error identity through wrapping. Both belong on canal's codec interface verbatim.

The core checks the contract at the boundary and produces a specific error naming the violation:

```go
if !driver.IsValue(sv) {
	return fmt.Errorf("non-subset type %T returned from Value", sv)
}
...
if !driver.IsValue(nv.Value) {
	return fmt.Errorf("driver ColumnConverter error converted %T to unsupported type %T", arg, nv.Value)
}
```

**Verdict for canal.** `type Value any` + a documented closed set + a runtime validator is a
*pre-generics* design. In Go 1.23 the honest options are:

1. `any` + closed set + validator (what `database/sql` does). Zero friction for connector authors,
   no compile-time safety, runtime type switches on the hot path.
2. A **closed interface** — a sealed sum type: `type Value interface { isValue() }` with unexported
   marker method and concrete `IntValue`, `BytesValue`, ... in the core package. Compile-time
   exhaustiveness is still not checked by Go, but third parties cannot widen the set, and the type
   switch is over named types.
3. Generics — `Record[T]`. This is where gratuitous generics bite: the engine must move records of
   *heterogeneous* type through one pipeline, buffer, and checkpoint them. A type parameter on the
   record forces the type parameter onto `Source`, `Sink`, `Buffer`, `Codec`, the registry, and the
   pipeline, and the registry then cannot hold them in one map without erasing back to `any`. **A
   type parameter that must be erased at the registry boundary buys nothing.** Generics are right for
   canal in *helpers* (`func Collect[T any](iter.Seq[T]) []T`, typed config decoding) and wrong for
   the record envelope.

Recommendation: option 2 for the field-value type; the envelope itself is a plain struct.

### 2.2 The envelope: `NamedValue` — the whole struct is three fields

```go
// NamedValue holds both the value name and value.
type NamedValue struct {
	// If the Name is not empty it should be used for the parameter identifier and
	// not the ordinal position.
	//
	// Name will not have a symbol prefix.
	Name string

	// Ordinal position of the parameter starting from one and is always set.
	Ordinal int

	// Value is the parameter value.
	Value Value
}
```

Note the invariant design: `Ordinal` "is always set", `Name` is optional, and the rule for which wins
is stated on the field. Also `Name will not have a symbol prefix` — the core **normalizes** before
handing to the driver, so no driver has to strip `@`/`:`/`$`. Canal should likewise normalize
(tenant, source id, timestamps, key encoding) *once in the core* rather than in 20 connectors.

### 2.3 Raw bytes and structured data coexisting

Three mechanisms, all verified:

**(a) `[]byte` is a first-class member of the value set.** Any payload the core cannot model
structurally travels as bytes with no wrapper.

**(b) Caller-owned buffer, reused across rows, with an explicit aliasing warning.**

```go
	// Next is called to populate the next row of data into
	// the provided slice. The provided slice will be the same
	// size as the Columns() are wide.
	//
	// Next should return io.EOF when there are no more rows.
	//
	// The dest should not be written to outside of Next. Care
	// should be taken when closing Rows not to modify
	// a buffer held in dest.
	Next(dest []Value) error
```

The core allocates once and passes the same slice every row. This is the allocation-free streaming
idiom, and it comes with a stated hazard: the driver must not retain or later mutate `dest`. `io`
states the same rule five times as `Implementations must not retain p.`

For canal this is the difference between one allocation per pipeline and one per record. But it
directly conflicts with buffering and fan-out — you cannot put a borrowed record into a queue. The
resolution the stdlib uses is **`sql.RawBytes` vs `[]byte` as two distinct destination types with
documented ownership**: scanning into `*[]byte` copies ("The copy is owned by the caller and can be
modified and held indefinitely"), scanning into `*RawBytes` does not. Canal needs the same explicit
two-mode contract: a borrowed view for the synchronous fast path, an owned copy the moment a record
crosses into a buffer, and the copy must be performed by the *core*, not trusted to connectors.

**(c) The codec seam is a pair of tiny interfaces, defined once, honoured by many packages.**

```go
// Package encoding defines interfaces shared by other packages that
// convert data to and from byte-level and textual representations.
// Packages that check for these interfaces include encoding/gob,
// encoding/json, and encoding/xml. As a result, implementing an
// interface once can make a type useful in multiple encodings.
// Standard types that implement these interfaces include time.Time and net.IP.
// The interfaces come in pairs that produce and consume encoded data.
//
// Adding encoding/decoding methods to existing types may constitute a breaking change,
// as they can be used for serialization in communicating with programs
// written with different library versions.
// ...
package encoding

type BinaryMarshaler interface {
	MarshalBinary() (data []byte, err error)
}

type BinaryUnmarshaler interface {
	// UnmarshalBinary must be able to decode the form generated by MarshalBinary.
	// UnmarshalBinary must copy the data if it wishes to retain the data
	// after returning.
	UnmarshalBinary(data []byte) error
}

type TextMarshaler interface {
	MarshalText() (text []byte, err error)
}

type TextUnmarshaler interface {
	// UnmarshalText must copy the text if it wishes to retain the text
	// after returning.
	UnmarshalText(text []byte) error
}
```

Two things to steal outright:

- **"The interfaces come in pairs that produce and consume encoded data."** Codecs are declared as
  marshal/unmarshal pairs, and the round-trip obligation is written on the unmarshaller
  (`UnmarshalBinary must be able to decode the form generated by MarshalBinary`). Canal's codec
  registry should register *pairs* and its conformance test should be the round-trip.
- **The compatibility warning**: "Adding encoding/decoding methods to existing types may constitute a
  breaking change, as they can be used for serialization in communicating with programs written with
  different library versions." This is canal's open decision #9 (checkpoint format compatibility
  across binary upgrades) stated as a general law: **once a type's serialized form is observable by
  another process or a stored checkpoint, its encoding is API.**

### 2.4 What the stdlib does *not* have, that canal must add

`database/sql` has no notion of key vs value, no operation type (insert/update/delete), no
before/after images, and no per-record metadata channel. A `Rows` row is a positional tuple plus
column names. That is the correct scope for SQL and the wrong scope for CDC.

There is no prior art here to copy, only a shape to imitate: canal's envelope should be a **plain
struct owned by the core**, with the same discipline `NamedValue` shows — every field's invariant
documented on the field, optional fields explicitly optional, one representation per concept (R1/R9),
and the *payload* typed by the closed value model above rather than by `any`.

---

## 3. Checkpoint model

**Honest headline: `database/sql` has no checkpoint model at all, and its absence is instructive.**
`driver.Rows` is a forward-only cursor that exposes *no position whatsoever*. There is no method to
ask "where am I", no way to resume a `Rows` after the process dies, and no way for the core to
persist anything about an in-flight result set. `Next(dest []Value) error` returns data or `io.EOF`;
that is the entire protocol. If canal modelled its source purely on `driver.Rows` it would be
unresumable by construction.

What the stdlib *does* give is the idiom for **position as an optional capability on a stream**.

### 3.1 `io.Seeker` — position as an upgrade, not a requirement

```go
// Seeker is the interface that wraps the basic Seek method.
//
// Seek sets the offset for the next Read or Write to offset,
// interpreted according to whence:
// [SeekStart] means relative to the start of the file,
// [SeekCurrent] means relative to the current offset, and
// [SeekEnd] means relative to the end
// (for example, offset = -2 specifies the penultimate byte of the file).
// Seek returns the new offset relative to the start of the
// file or an error, if any.
//
// Seeking to an offset before the start of the file is an error.
// Seeking to any positive offset may be allowed, but if the new offset exceeds
// the size of the underlying object the behavior of subsequent I/O operations
// is implementation-dependent.
type Seeker interface {
	Seek(offset int64, whence int) (int64, error)
}

const (
	SeekStart   = 0 // seek relative to the origin of the file
	SeekCurrent = 1 // seek relative to the current offset
	SeekEnd     = 2 // seek relative to the end
)
```

Structure worth copying:

- **Resumability is a separate interface** (`io.ReadSeeker`), so a non-resumable stream (a socket, a
  CDC tail) simply does not implement it, and the engine type-asserts to find out.
- The position is **one opaque scalar plus an origin enum**. A caller that only ever does
  `Seek(0, SeekStart)` (restart) works against every implementation; a caller that wants a saved
  position uses `Seek(0, SeekCurrent)` to read it back. **One method both reads and writes the
  position.**
- The failure modes are documented per-case, including one that is explicitly
  `implementation-dependent` — the stdlib names the undefined behaviour rather than leaving it
  ambiguous.

### 3.2 `SectionReader.Outer()` — "hand me back exactly what you were constructed from"

```go
// Outer returns the underlying [ReaderAt] and offsets for the section.
//
// The returned values are the same that were passed to [NewSectionReader]
// when the [SectionReader] was created.
func (s *SectionReader) Outer() (r ReaderAt, off int64, n int64) {
	return s.r, s.base, s.n
}
```

Small but exactly the checkpoint idiom: an object that can regurgitate the parameters needed to
reconstruct it. A canal source assignment (`shard`, `start`, `end`) should be recoverable from the
running reader in the form the constructor accepts, so that "checkpoint" and "construct" use the same
vocabulary. This is the structural defence against R1/R9-style dual representations: if the resume
payload and the construction payload are the same type, they cannot drift.

### 3.3 What canal must add, and the traps the stdlib flags

The stdlib gives no answer for who owns the offset, when it commits, or how it survives restart.
Three verified stdlib facts constrain the design anyway:

**(a) An acknowledgement must not outrun durability.** `driver.ErrBadConn`'s doc is the sharpest
statement of this in the standard library, and it is about exactly the "did the write happen"
ambiguity that governs checkpoint advance:

```go
// ErrBadConn should be returned by a driver to signal to the [database/sql]
// package that a driver.[Conn] is in a bad state (such as the server
// having earlier closed the connection) and the [database/sql] package should
// retry on a new connection.
//
// To prevent duplicate operations, ErrBadConn should NOT be returned
// if there's a possibility that the database server might have
// performed the operation. Even if the server sends back an error,
// you shouldn't return ErrBadConn.
//
// Errors will be checked using [errors.Is]. An error may
// wrap ErrBadConn or implement the Is(error) bool method.
var ErrBadConn = errors.New("driver: bad connection")
```

"should NOT be returned if there's a possibility that the database server might have performed the
operation. Even if the server sends back an error, you shouldn't return ErrBadConn." That is the
retry-safety predicate stated as an obligation on the *plugin*, checked by the core with `errors.Is`.
Canal's connector contract needs the identical clause: a connector may only report a *retriable*
failure when it knows the effect did not land. This is design-rules R4 and R5 in one sentence, and it
is enforceable in a conformance test.

**(b) Retries are bounded and the bound is a named constant, with a different final attempt.**

```go
// maxBadConnRetries is the number of maximum retries if the driver returns
// driver.ErrBadConn to signal a broken connection before forcing a new
// connection to be opened.
const maxBadConnRetries = 2

func (db *DB) retry(fn func(strategy connReuseStrategy) error) error {
	for i := int64(0); i < maxBadConnRetries; i++ {
		err := fn(cachedOrNewConn)
		// retry if err is driver.ErrBadConn
		if err == nil || !errors.Is(err, driver.ErrBadConn) {
			return err
		}
	}

	return fn(alwaysNewConn)
}
```

The last attempt uses a *different strategy* (`alwaysNewConn`) rather than repeating the same one — an
escalation, not a loop. Canal's backoff policy table should have the same shape: N attempts under the
cheap strategy, then one attempt under the expensive/clean strategy, then terminal.

**(c) `errors.Is` — not `==` — is mandated for sentinel checks, precisely so drivers can wrap.**
See §6.4.

### 3.4 Sink acknowledgement: `Result` as the "what actually happened" channel

```go
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}
```

Every field is `(value, error)`, and the stdlib ships **ready-made "not supported" implementations**
so a driver never has to invent the refusal:

```go
// RowsAffected implements [Result] for an INSERT or UPDATE operation
// which mutates a number of rows.
type RowsAffected int64

var _ Result = RowsAffected(0)

func (RowsAffected) LastInsertId() (int64, error) {
	return 0, errors.New("LastInsertId is not supported by this driver")
}

func (v RowsAffected) RowsAffected() (int64, error) {
	return int64(v), nil
}

// ResultNoRows is a pre-defined [Result] for drivers to return when a DDL
// command (such as a CREATE TABLE) succeeds. It returns an error for both
// LastInsertId and [RowsAffected].
var ResultNoRows noRows

type noRows struct{}

var _ Result = noRows{}

func (noRows) LastInsertId() (int64, error) {
	return 0, errors.New("no LastInsertId available after DDL statement")
}

func (noRows) RowsAffected() (int64, error) {
	return 0, errors.New("no RowsAffected available after DDL statement")
}
```

Note `var _ Result = RowsAffected(0)` — the **compile-time conformance assertion idiom**, used twice
in ~30 lines. Canal should put `var _ Source = (*myThing)(nil)` in every connector package and in its
conformance test kit.

The transplant: canal's sink acknowledgement should be an interface of `(value, error)` accessors
with core-provided default implementations for the unsupported cases, so that "this sink cannot tell
you how many records landed" is a *typed, ergonomic* answer rather than a zero that reads as success.
That is the direct structural fix for R7 (write the failure shape at the same time as the success
shape) — and note what `database/sql` still gets *wrong* here: `Result` has no per-record error
channel at all, so a partially-applied batch is inexpressible. Canal must not copy that.

---

## 4. Snapshot handling

### 4.1 `RowsNextResultSet` — the snapshot→stream handoff, already solved

This is the most directly transplantable interface in the standard library for canal's
hybrid-pipeline problem, and it is easy to overlook:

```go
// RowsNextResultSet extends the [Rows] interface by providing a way to signal
// the driver to advance to the next result set.
type RowsNextResultSet interface {
	Rows

	// HasNextResultSet is called at the end of the current result set and
	// reports whether there is another result set after the current one.
	HasNextResultSet() bool

	// NextResultSet advances the driver to the next result set even
	// if there are remaining rows in the current result set.
	//
	// NextResultSet should return io.EOF when there are no more result sets.
	NextResultSet() error
}
```

Read it as canal's snapshot-then-stream contract:

- **One stream object spans multiple phases.** The consumer holds a single `Rows` and does not know,
  or need to know, that it crossed a boundary. Snapshot and incremental stream are *result sets* of
  one logical read — not two objects the engine has to splice, and not two code paths in the core.
- **The phase boundary is explicit and consumer-driven.** `HasNextResultSet() bool` is a cheap
  predicate; `NextResultSet() error` is the transition; `io.EOF` terminates. The engine can therefore
  *checkpoint at the boundary* — which is exactly the point where "snapshot complete, switch to
  stream from position P" must be made durable.
- **`NextResultSet` may abandon a partially-consumed set** ("advances the driver to the next result
  set even if there are remaining rows"). That gives the engine an explicit "abort the snapshot, go
  straight to streaming" move.
- **Column metadata is re-derived per result set.** `sql.Rows` re-runs
  `rowsColumnInfoSetupConnLocked(rowsi)` after advancing, so a phase change may legitimately change
  the schema. That is the schema-drift-at-handoff case, handled by construction.
- **It is optional.** A CDC-only source that has no snapshot simply does not implement it; a
  snapshot-only batch source implements it and returns `io.EOF` immediately. **No core edits, no
  pipeline-type enum, no switch statement** — which is precisely canal's success criterion, and
  precisely the trap R1 warns about (a fixed stage/phase count frozen into the contract).

The one thing to fix in the copy: `HasNextResultSet() bool` cannot report an error, so a source that
must do I/O to discover whether another phase exists has nowhere to put a failure. Canal should use
`(bool, error)`.

### 4.2 Chunking and parallelism: `ReaderAt` is the parallel-safe capability

The stdlib separates *sequential* from *random/parallel* access into different interfaces, and states
the concurrency contract on each:

```go
// ReaderAt is the interface that wraps the basic ReadAt method.
//
// ReadAt reads len(p) bytes into p starting at offset off in the
// underlying input source. ...
//
// When ReadAt returns n < len(p), it returns a non-nil error
// explaining why more bytes were not returned. In this respect,
// ReadAt is stricter than Read.
//
// Even if ReadAt returns n < len(p), it may use all of p as scratch
// space during the call. If some data is available but not len(p) bytes,
// ReadAt blocks until either all the data is available or an error occurs.
// In this respect ReadAt is different from Read.
//
// If the n = len(p) bytes returned by ReadAt are at the end of the
// input source, ReadAt may return either err == EOF or err == nil.
//
// If ReadAt is reading from an input source with a seek offset,
// ReadAt should not affect nor be affected by the underlying
// seek offset.
//
// Clients of ReadAt can execute parallel ReadAt calls on the
// same input source.
//
// Implementations must not retain p.
type ReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

// WriterAt is the interface that wraps the basic WriteAt method.
// ...
// Clients of WriteAt can execute parallel WriteAt calls on the same
// destination if the ranges do not overlap.
//
// Implementations must not retain p.
type WriterAt interface {
	WriteAt(p []byte, off int64) (n int, err error)
}
```

Three facts to lift straight into canal's parallel-snapshot design:

1. **"Clients of ReadAt can execute parallel ReadAt calls on the same input source."** Parallelism is
   granted by the *interface's* documentation, so the engine may fan out without asking. The
   symmetric sink rule is conditional — "if the ranges do not overlap" — i.e. the *engine* owns
   non-overlap, the sink owns thread-safety within a range. That division is exactly what canal needs
   between a parallel snapshot planner and a sink.
2. **`ReadAt` is stricter than `Read`**: a short result *must* carry an explaining error. Random-access
   chunk reads have no "try again later"; ambiguity is banned. Canal's chunked snapshot read should
   have the same strictness (a chunk is complete or it errored).
3. **Position is independent of any cursor** ("should not affect nor be affected by the underlying
   seek offset") — so a parallel snapshot worker cannot corrupt the incremental reader's position.

The bounded-range reader is then a tiny struct over the capability:

```go
// NewSectionReader returns a [SectionReader] that reads from r
// starting at offset off and stops with EOF after n bytes.
func NewSectionReader(r ReaderAt, off int64, n int64) *SectionReader
```

`SectionReader` = one snapshot chunk assignment. `Outer()` (§3.2) = that chunk's resumable
descriptor. Together they are a complete, tested model of "split a snapshot into independently
readable, independently resumable, parallel-safe ranges" — in the stdlib, with no framework.

For the sequential half, `LimitReader` is the chunk-size primitive:

```go
// LimitReader returns a Reader that reads from r
// but stops with EOF after n bytes.
// The underlying implementation is a *LimitedReader.
func LimitReader(r Reader, n int64) Reader { return &LimitedReader{r, n} }

// A LimitedReader reads from R but limits the amount of
// data returned to just N bytes. Each call to Read
// updates N to reflect the new amount remaining.
// Read returns EOF when N <= 0 or when the underlying R returns EOF.
type LimitedReader struct {
	R Reader // underlying reader
	N int64  // max bytes remaining
}
```

Two design notes: the struct fields are **exported** so a caller can inspect remaining budget (and
`io.Copy` does exactly that), and a *budget exhausted* result is reported as `EOF` — the same signal
as *stream exhausted*. That conflation is a real wart: a consumer cannot distinguish "chunk done" from
"source done". Canal must keep those distinct, which is what `HasNextResultSet`-style phase signalling
is for.

### 4.3 Is a source an iterator? `iter.Seq` in Go 1.23

Canal's brief asks explicitly whether a source should be modelled as an iterator. Go 1.23 added:

```go
// Seq is an iterator over sequences of individual values.
// When called as seq(yield), seq calls yield(v) for each value v in the sequence,
// stopping early if yield returns false.
// See the [iter] package documentation for more details.
type Seq[V any] func(yield func(V) bool)

// Seq2 is an iterator over sequences of pairs of values, most commonly key-value pairs.
// When called as seq(yield), seq calls yield(k, v) for each pair (k, v) in the sequence,
// stopping early if yield returns false.
// See the [iter] package documentation for more details.
type Seq2[K, V any] func(yield func(K, V) bool)
```

**Arguments for.** The `yield func(V) bool` return value *is* a backpressure and early-stop channel:
the consumer says "stop" and the producer's own `defer`s run in its own stack frame, so cleanup is
natural. The package even names canal's exact case:

```
# Single-Use Iterators

Most iterators provide the ability to walk an entire sequence: ...
Some iterators break that convention, providing the ability to walk a
sequence only once. These “single-use iterators” typically report values
from a data stream that cannot be rewound to start over.
Calling the iterator again after stopping early may continue the
stream, but calling it again after the sequence is finished will yield
no values at all. Doc comments for functions or methods that return
single-use iterators should document this fact:

	// Lines returns an iterator over lines read from r.
	// It returns a single-use iterator.
	func (r *Reader) Lines() iter.Seq[string]
```

And `iter.Pull` converts push to pull when the engine wants control:

```go
// Pull converts the “push-style” iterator sequence seq
// into a “pull-style” iterator accessed by the two functions
// next and stop.
// ...
// Stop ends the iteration. It must be called when the caller is
// no longer interested in next values and next has not yet
// signaled that the sequence is over (with a false boolean return).
// It is valid to call stop multiple times and when next has
// already returned false. Typically, callers should “defer stop()”.
//
// It is an error to call next or stop from multiple goroutines
// simultaneously.
func Pull[V any](seq Seq[V]) (next func() (V, bool), stop func())
```

**Arguments against — decisive for canal's primary source interface.**

1. **There is nowhere to put an error.** `Seq[V]` is `func(yield func(V) bool)` — no error return.
   Every real Go iterator over a fallible stream ends up as `Seq2[V, error]`, or stashing the error in
   the producer struct for a separate `Err()` call (the `bufio.Scanner` pattern). Both are worse than
   `Next() error`.
2. **There is nowhere to put a checkpoint.** A source must interleave "here is a record" with "here is
   the position after that record", and must let the engine commit asynchronously. A bare `Seq` has no
   handle to query.
3. **`Pull` costs a coroutine per stream** (`newcoro`/`coroswitch`) and imposes
   "It is an error to call next or stop from multiple goroutines simultaneously."
4. **No context.** A `Seq` closure must capture its context, which the `context` package explicitly
   discourages as a stored value, and the engine cannot pass a per-poll deadline.
5. **`Pull`'s contract is easy to violate** — "yield called again before next" and "next called again
   before yield" are *panics*, not errors.

**Recommendation:** model the source as a `database/sql`-style cursor —
`Next(ctx, *Batch) error` returning `io.EOF`, with position exposed alongside — and offer
`iter.Seq`/`Seq2` **only as an ergonomic adapter at the edges** (tests, transforms, CLI tooling,
`for rec := range src.All(ctx)`). Same relationship `sql.Rows` has to `range`: a convenience over a
cursor, not the cursor itself.

---

## 5. Schema handling

### 5.1 The required minimum is names; everything else is an optional capability

```go
	// Columns returns the names of the columns. The number of
	// columns of the result is inferred from the length of the
	// slice. If a particular column name isn't known, an empty
	// string should be returned for that entry.
	Columns() []string
```

That is the whole *mandatory* schema surface of a Go SQL driver: a `[]string`, whose **length doubles
as the arity of the record**, and where an unknown name is the empty string rather than an error.
Minimal, total, and unfailable — `Columns()` cannot return an error, so schema discovery for the
required path can never break a read.

Types are then layered on by five independent optional interfaces, each re-embedding `Rows`:

```go
type RowsColumnTypeScanType interface {
	Rows
	ColumnTypeScanType(index int) reflect.Type
}

type RowsColumnTypeDatabaseTypeName interface {
	Rows
	ColumnTypeDatabaseTypeName(index int) string
}

type RowsColumnTypeLength interface {
	Rows
	ColumnTypeLength(index int) (length int64, ok bool)
}

type RowsColumnTypeNullable interface {
	Rows
	ColumnTypeNullable(index int) (nullable, ok bool)
}

type RowsColumnTypePrecisionScale interface {
	Rows
	ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool)
}
```

Each is **granular** (a driver that knows nullability but not decimal scale implements exactly one),
each returns an explicit `ok bool` for "not applicable to this column" — so there are *two* levels of
"don't know": interface absent (driver can never tell you) and `ok == false` (driver can't tell you
*about this column*). The doc comments even enumerate the expected values, which is how a spec avoids
per-driver drift:

```go
// If length is not limited other than system limits, it should return [math.MaxInt64].
// The following are examples of returned values for various types:
//
//	TEXT          (math.MaxInt64, true)
//	varchar(10)   (10, true)
//	nvarchar(10)  (10, true)
//	decimal       (0, false)
//	int           (0, false)
//	bytea(30)     (30, true)
```

```go
// Examples of returned types: "VARCHAR", "NVARCHAR", "VARCHAR2", "CHAR", "TEXT",
// "DECIMAL", "SMALLINT", "INT", "BIGINT", "BOOL", "[]BIGINT", "JSONB", "XML",
// "TIMESTAMP".
```

`Type names should be uppercase.` — a normalization rule stated in the interface's doc. This is the
cheap version of canal's R9 (one concept, one vocabulary): if you cannot enforce a vocabulary in the
type system, write the canonical spelling into the contract and assert it in the conformance kit.

### 5.2 The core snapshots capabilities into a plain struct — this is the UI answer

The single most transplantable piece of engineering in this section. The core probes all five optional
interfaces **once**, at result-set setup, and materializes a plain owned struct:

```go
func rowsColumnInfoSetupConnLocked(rowsi driver.Rows) []*ColumnType {
	names := rowsi.Columns()

	list := make([]*ColumnType, len(names))
	for i := range list {
		ci := &ColumnType{
			name: names[i],
		}
		list[i] = ci

		if prop, ok := rowsi.(driver.RowsColumnTypeScanType); ok {
			ci.scanType = prop.ColumnTypeScanType(i)
		} else {
			ci.scanType = reflect.TypeFor[any]()
		}
		if prop, ok := rowsi.(driver.RowsColumnTypeDatabaseTypeName); ok {
			ci.databaseType = prop.ColumnTypeDatabaseTypeName(i)
		}
		if prop, ok := rowsi.(driver.RowsColumnTypeLength); ok {
			ci.length, ci.hasLength = prop.ColumnTypeLength(i)
		}
		if prop, ok := rowsi.(driver.RowsColumnTypeNullable); ok {
			ci.nullable, ci.hasNullable = prop.ColumnTypeNullable(i)
		}
		if prop, ok := rowsi.(driver.RowsColumnTypePrecisionScale); ok {
			ci.precision, ci.scale, ci.hasPrecisionScale = prop.ColumnTypePrecisionScale(i)
		}
	}
	return list
}
```

Then the *public* surface is a struct with total accessors, and every "driver didn't say" case is
expressed as a second return value rather than a zero value or a panic:

```go
// Length returns the column type length for variable length column types such
// as text and binary field types. If the type length is unbounded the value will
// be [math.MaxInt64] (any database limits will still apply).
// If the column type is not variable length, such as an int, or if not supported
// by the driver ok is false.
func (ci *ColumnType) Length() (length int64, ok bool) {
	return ci.length, ci.hasLength
}

// DecimalSize returns the scale and precision of a decimal type.
// If not applicable or if not supported ok is false.
func (ci *ColumnType) DecimalSize() (precision, scale int64, ok bool) {
	return ci.precision, ci.scale, ci.hasPrecisionScale
}

// ScanType returns a Go type suitable for scanning into using [Rows.Scan].
// If a driver does not support this property ScanType will return
// the type of an empty interface.
func (ci *ColumnType) ScanType() reflect.Type {
	return ci.scanType
}

// Nullable reports whether the column may be null.
// If a driver does not support this property ok will be false.
func (ci *ColumnType) Nullable() (nullable, ok bool) {
	return ci.nullable, ci.hasNullable
}

// DatabaseTypeName returns the database system name of the column type. If an empty
// string is returned, then the driver type name is not supported.
// ...
func (ci *ColumnType) DatabaseTypeName() string {
	return ci.databaseType
}
```

**Why this matters so much for canal's frontend.** The optional-interface pattern is hostile to a UI —
a UI cannot type-assert, cannot be a Go caller, and must not contain per-connector knowledge (R10,
and the whole "specialized UI later without core changes" constraint). `ColumnType` is the answer:

> **The core collapses the connector's capability set into a serializable, core-owned struct at a
> well-defined moment, and the UI reads only that struct.**

Every "did the connector support this" question becomes a field with a companion `has*` boolean. That
is a JSON-shaped, versionable, testable artifact (canal's R8: assert real responses; and the
"deterministic read-model fixtures" note). It is also exactly the mechanism that lets canal honour
"honesty as a structural property of the UI" — `hasLength == false` renders as *unknown*, which is
distinguishable from `length == 0`.

Note also the *default* on the one non-optional-feeling property:
`ci.scanType = reflect.TypeFor[any]()`. There is always a value; absence degrades to the widest type,
not to nil.

### 5.3 In-band vs out-of-band, and drift

Schema in `database/sql` is strictly **in-band and per-result-set**: it is reachable only from a live
`Rows`, is computed at open time, and is recomputed when `NextResultSet` advances. There is no
registry, no version, no compatibility check, and no way to ask a `Conn` about a schema without
executing something.

Consequences, honestly stated:

- **Drift within a result set is impossible by construction** — arity is fixed by `len(Columns())` and
  `Next(dest []Value)` is handed a slice of exactly that width. Good.
- **Drift across result sets is permitted and unannounced.** The core silently recomputes. A consumer
  is not told "the schema changed"; it must diff for itself. For canal's snapshot→stream handoff this
  is under-specified: canal should emit an explicit schema-change event at the phase boundary rather
  than requiring the sink to notice.
- **Out-of-band schema is absent.** There is no `Driver`-level "describe what you can produce" call,
  so nothing can populate a UI before a pipeline runs. Canal needs that (it is the same gap as §7),
  and it should be a *separate optional interface on the configured connector* — `DescribeSchema(ctx)`
  — so that a connector which can only discover schema by reading simply doesn't implement it, and the
  UI shows "schema known only at runtime" rather than an empty table.

The transplantable rule: **schema travels with the record stream as a versioned, core-owned struct
attached to a phase, plus an optional out-of-band describe capability for the UI.** Never a
per-connector UI type.
