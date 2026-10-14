package state_test

// recovery_safety_test.go — targeted follow-up tests for the recovery-state safety patch.
//
// Three groups:
//  1. COMPLETED persistence with a canceled caller context: verify the durable status
//     write uses a bounded background context (not the caller ctx) and is never skipped.
//  2. Advisory-lock cleanup failure / connection reuse safety: verify that when
//     pg_advisory_unlock returns false or an error, Discard() is called so the
//     connection is never returned to the pool with a potentially held lock.
//  3. Concurrent EnsureSchema: verify that adding chk_state_workspace_recovery_status
//     is safe when multiple processes run EnsureSchema simultaneously (mock for the
//     IsDuplicateObject race; real-DB integration guarded by TCP probe skip).

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aaradhychinche-alt/WhatBreaks/internal/auth"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/config"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/database"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/state"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ---------------------------------------------------------------------------
// Shared mock plumbing
// ---------------------------------------------------------------------------

// mockRow implements pgx.Row for QueryRow stub returns.
type mockRow struct {
	scanFn func(dest ...any) error
}

func (r *mockRow) Scan(dest ...any) error { return r.scanFn(dest...) }

// errRow always returns a fixed error from Scan.
type errRow struct{ err error }

func (r *errRow) Scan(...any) error { return r.err }

// mockTx is a minimal database.Tx that always succeeds.
type mockTx struct {
	execFn func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func (t *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return &safeRows{}, nil
}
func (t *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	// Return a row whose Scan sets the first *int dest to 0 (for count queries).
	return &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			switch d := dest[0].(type) {
			case *int:
				*d = 0
			case *bool:
				*d = false
			}
		}
		return nil
	}}
}
func (t *mockTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if t.execFn != nil {
		return t.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}
func (t *mockTx) Commit(ctx context.Context) error   { return nil }
func (t *mockTx) Rollback(ctx context.Context) error { return nil }

// safeRows is an empty pgx.Rows implementation.
type safeRows struct{}

func (r *safeRows) Close()                                       {}
func (r *safeRows) Err() error                                   { return nil }
func (r *safeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *safeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *safeRows) Next() bool                                   { return false }
func (r *safeRows) Scan(...any) error                            { return nil }
func (r *safeRows) Values() ([]any, error)                       { return nil, nil }
func (r *safeRows) RawValues() [][]byte                          { return nil }
func (r *safeRows) Conn() *pgx.Conn                              { return nil }
func (r *safeRows) TypeMap() *pgtype.Map                         { return nil }

// ---------------------------------------------------------------------------
// mockConn — configurable Connection implementation
// ---------------------------------------------------------------------------

// connCallRecord captures which context was passed to each Exec call.
type connCallRecord struct {
	sql  string
	ctx  context.Context
	args []any
}

type mockConn struct {
	mu       sync.Mutex
	execLog  []connCallRecord
	discarded bool

	// execFn, if non-nil, handles all Exec calls; otherwise returns success.
	execFn func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)

	// queryRowFn, if non-nil, handles all QueryRow calls.
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row

	// withTxFn, if non-nil, is called instead of a real transaction.
	withTxFn func(ctx context.Context, fn func(context.Context, database.Tx) error) error
}

func (c *mockConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return &safeRows{}, nil
}

func (c *mockConn) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if c.queryRowFn != nil {
		return c.queryRowFn(ctx, sql, args...)
	}
	// Default: return a row that sets the first *bool dest to true (unlock succeeded).
	return &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				*b = true
			}
		}
		return nil
	}}
}

func (c *mockConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	c.mu.Lock()
	c.execLog = append(c.execLog, connCallRecord{sql: sql, ctx: ctx, args: args})
	c.mu.Unlock()
	if c.execFn != nil {
		return c.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (c *mockConn) Begin(ctx context.Context) (database.Tx, error) {
	return &mockTx{}, nil
}

func (c *mockConn) WithTransaction(ctx context.Context, fn func(context.Context, database.Tx) error) error {
	if c.withTxFn != nil {
		return c.withTxFn(ctx, fn)
	}
	tx := &mockTx{}
	return fn(ctx, tx)
}

func (c *mockConn) TryAdvisoryLock(ctx context.Context, key string) (bool, error) {
	return true, nil
}

func (c *mockConn) AdvisoryUnlock(ctx context.Context, key string) (bool, error) {
	return true, nil
}

func (c *mockConn) Discard() {
	c.mu.Lock()
	c.discarded = true
	c.mu.Unlock()
}

func (c *mockConn) RawConn() *pgxpool.Conn { return nil }

func (c *mockConn) wasDiscarded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.discarded
}

// execContexts returns the context recorded for each Exec call.
func (c *mockConn) execContexts() []context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]context.Context, len(c.execLog))
	for i, r := range c.execLog {
		out[i] = r.ctx
	}
	return out
}

// ---------------------------------------------------------------------------
// mockDB — minimal database.DB that routes WithConnection to a supplied conn
// ---------------------------------------------------------------------------

type mockDB struct {
	conn           *mockConn
	withConnCalled int32 // atomic

	// queryRowFn is used for pool-level QueryRow (EnsureSchema, recordRecoveryAudit, etc.)
	queryRowFn func(ctx context.Context, sql string, args ...any) pgx.Row
	execFn     func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	queryFn    func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (d *mockDB) Ping(ctx context.Context) error { return nil }
func (d *mockDB) Close() error                   { return nil }
func (d *mockDB) TestConnection(ctx context.Context) error { return nil }
func (d *mockDB) WaitForDatabase(ctx context.Context, maxAttempts int, delay ...any) error {
	return nil
}
func (d *mockDB) Stat() database.PoolStat { return database.PoolStat{} }
func (d *mockDB) AcquireLock(ctx context.Context, key string) (database.SessionLock, error) {
	return nil, errors.New("mockDB: AcquireLock not implemented")
}

func (d *mockDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if d.queryFn != nil {
		return d.queryFn(ctx, sql, args...)
	}
	return &safeRows{}, nil
}

func (d *mockDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if d.queryRowFn != nil {
		return d.queryRowFn(ctx, sql, args...)
	}
	return &mockRow{scanFn: func(dest ...any) error { return nil }}
}

func (d *mockDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if d.execFn != nil {
		return d.execFn(ctx, sql, args...)
	}
	return pgconn.CommandTag{}, nil
}

func (d *mockDB) Begin(ctx context.Context) (database.Tx, error) {
	return &mockTx{}, nil
}

func (d *mockDB) WithTransaction(ctx context.Context, fn func(context.Context, database.Tx) error) error {
	tx := &mockTx{}
	return fn(ctx, tx)
}

func (d *mockDB) WithConnection(ctx context.Context, fn func(context.Context, database.Connection) error) error {
	atomic.AddInt32(&d.withConnCalled, 1)
	return fn(ctx, d.conn)
}

// ---------------------------------------------------------------------------
// GROUP 1: COMPLETED persistence with a canceled caller context
//
// Invariant: even when the caller cancels their context before
// runWorkspaceLegacyRecoveryLocked writes the COMPLETED status, the write
// must still succeed because it uses context.WithTimeout(context.Background(),
// recoveryStatusCleanupTimeout) — not the caller ctx.
//
// We test this at two levels:
//   a. MemoryStore (no-DB, pure logic): canceled ctx before call → recovery
//      still persists COMPLETED and returns success.
//   b. PostgresStore with a mock DB: verify that the Exec call for the
//      COMPLETED update uses a non-canceled context (i.e. its own deadline,
//      not the already-canceled caller context).
// ---------------------------------------------------------------------------

// TestRecoverySafety_CanceledCallerCtx_MemoryStore_CompletedStatusPersists verifies
// that MemoryStore RecoverWorkspaceLegacyState uses its own internal timing for the
// COMPLETED status write and is not affected by a pre-canceled caller context.
func TestRecoverySafety_CanceledCallerCtx_MemoryStore_CompletedStatusPersists(t *testing.T) {
	memStore := state.NewMemoryStore()
	wsID := "ws-canceled-ctx-completed"
	now := time.Now().UTC()

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})

	// Cancel the context before calling RecoverWorkspaceLegacyState.
	canceledCtx, cancel := context.WithCancel(adminCtx)
	cancel() // already canceled

	// RecoverWorkspaceLegacyState on a clean workspace (no legacy state, no batch needed).
	// Even though the context is canceled the MemoryStore must still complete the operation
	// synchronously, because the persistence is done under the store's own mutex with no
	// ctx-aware blocking after authentication.
	result, err := memStore.RecoverWorkspaceLegacyState(canceledCtx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            "prod-cluster",
		AuthorizeDestructive: true,
		Reason:               "canceled-ctx test",
	})
	if err != nil {
		t.Fatalf("expected recovery to succeed with pre-canceled ctx (MemoryStore); got: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}

	// The durable COMPLETED status must have been written even though ctx was canceled.
	audits, err := memStore.ListRecoveryAudits(context.Background(), wsID)
	if err != nil {
		t.Fatalf("ListRecoveryAudits: %v", err)
	}
	if len(audits) == 0 {
		t.Fatal("expected audit entry after successful recovery")
	}
	last := audits[len(audits)-1]
	if last.Status != "SUCCESS" {
		t.Errorf("expected audit status SUCCESS, got %q", last.Status)
	}

	// Normal reconciliation must now work (recovery status is COMPLETED).
	_, recErr := memStore.ReconcileAuthoritativeBatch(context.Background(), state.AuthoritativeBatchParams{
		WorkspaceID: wsID,
		ClusterID:   "prod-cluster",
		Resources: []state.Resource{
			{
				WorkspaceID: wsID,
				Identity: state.ResourceIdentity{
					Provider:     "kubernetes",
					ResourceType: "pod",
					ProviderID:   "prod-cluster/default/pod-1",
				},
				FirstObservedAt: now,
				LastObservedAt:  now,
			},
		},
	})
	if recErr != nil {
		t.Errorf("expected reconciliation to succeed after COMPLETED recovery, got: %v", recErr)
	}
}

// TestRecoverySafety_CanceledCallerCtx_PostgresMock_CompletedExecUsesCleanContext
// injects a mock DB whose WithConnection hands in a mock connection that records
// every Exec call along with its context. After a successful recovery, the COMPLETED
// UPDATE must have been issued with a context that is NOT the already-canceled caller
// context — i.e. it must still be live (not canceled/timed-out) at the point of the call.
func TestRecoverySafety_CanceledCallerCtx_PostgresMock_CompletedExecUsesCleanContext(t *testing.T) {
	wsID := "ws-pg-mock-canceled-ctx"
	clusterID := "mock-cluster"

	// execCtxs captures the context passed to each conn.Exec(). We check the
	// COMPLETED UPDATE specifically — it must have a valid (non-done) context.
	type execCapture struct {
		sql string
		ctx context.Context
	}
	var mu sync.Mutex
	var captured []execCapture

	conn := &mockConn{}
	conn.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		mu.Lock()
		captured = append(captured, execCapture{sql: sql, ctx: ctx})
		mu.Unlock()
		return pgconn.CommandTag{}, nil
	}

	// pg_advisory_unlock returns released=true so the happy path runs.
	conn.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		if strings.Contains(sql, "pg_advisory_unlock") {
			return &mockRow{scanFn: func(dest ...any) error {
				if len(dest) > 0 {
					if b, ok := dest[0].(*bool); ok {
						*b = true
					}
				}
				return nil
			}}
		}
		return &mockRow{scanFn: func(dest ...any) error { return nil }}
	}

	// The WithTransaction callback must succeed so we reach the COMPLETED path.
	// We run a no-op transaction that returns nil.
	conn.withTxFn = func(ctx context.Context, fn func(context.Context, database.Tx) error) error {
		// Provide a tx that satisfies all sub-queries inside runWorkspaceLegacyRecoveryLocked:
		// - SELECT count(*) FROM state_evidence → 0
		// - Various SELECT queries return empty rows
		tx := &mockTx{
			execFn: func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
				return pgconn.CommandTag{}, nil
			},
		}
		return fn(ctx, tx)
	}

	db := &mockDB{conn: conn}
	// Pool-level Query (for resource/relationship inspection rows) returns empty rows.
	db.queryFn = func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return &safeRows{}, nil
	}
	// Pool-level Exec (for recordRecoveryAudit) always succeeds.
	db.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}

	pgStore := state.NewPostgresStore(db)

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})

	// Cancel the caller context before invoking the recovery.
	canceledCtx, cancel := context.WithCancel(adminCtx)
	cancel()

	_, err := pgStore.RecoverWorkspaceLegacyState(canceledCtx, state.LegacyRecoveryParams{
		WorkspaceID:          wsID,
		ClusterID:            clusterID,
		AuthorizeDestructive: true,
		Reason:               "mock canceled-ctx test",
	})

	// A canceled caller context should not prevent a successful recovery when
	// the underlying ops succeed. If advisory-lock acquire itself fails because
	// the context is already canceled, the test result depends on DB behavior —
	// so we accept either: success (COMPLETED write verifiable) or
	// context.Canceled (cancel arrived before lock acquire; COMPLETED path not reached).
	if err != nil {
		if errors.Is(err, context.Canceled) {
			t.Logf("context.Canceled reached advisory-lock acquire before work began — " +
				"COMPLETED path not exercised; this is acceptable")
			return
		}
		t.Fatalf("unexpected error from RecoverWorkspaceLegacyState: %v", err)
	}

	// Recovery succeeded. Verify the COMPLETED UPDATE was issued with a non-canceled context.
	mu.Lock()
	defer mu.Unlock()

	var completedExecCtx context.Context
	for _, cap := range captured {
		if strings.Contains(cap.sql, "COMPLETED") {
			completedExecCtx = cap.ctx
			break
		}
	}

	if completedExecCtx == nil {
		t.Fatal("expected a COMPLETED UPDATE exec to have been recorded")
	}

	// The context used for the COMPLETED write must be distinct from the canceled
	// caller context — it should still be alive at the moment of the call
	// (cleanupCtx derived from context.Background() with a timeout).
	select {
	case <-completedExecCtx.Done():
		// Context was already done at call time — this would indicate the bounded
		// cleanup context was not used. However, the context may have expired after
		// the call returned (recoveryStatusCleanupTimeout is 5 s). So we only fail
		// if the context's cause matches context.Canceled without a deadline.
		if completedExecCtx.Err() == context.Canceled {
			_, hasDeadline := completedExecCtx.Deadline()
			if !hasDeadline {
				t.Error("COMPLETED exec used a context with Canceled error but no deadline — " +
					"looks like the caller context was used instead of a bounded cleanup context")
			}
		}
	default:
		// Context is still live — bounded cleanup context was used correctly. ✓
	}
}

// ---------------------------------------------------------------------------
// GROUP 2: Advisory-lock cleanup failure → connection discard, not pool return
//
// Two sub-cases:
//   a. pg_advisory_unlock returns false (lock was not held or session changed).
//   b. pg_advisory_unlock returns a connection error.
//
// In both cases, conn.Discard() must be called so the connection is never
// returned to the pool with a potentially held session-level advisory lock.
// ---------------------------------------------------------------------------

// buildAdvisoryUnlockDB constructs a mock DB where pg_advisory_unlock behaves
// according to unlockRow. The inner recovery work always succeeds (no legacy state).
func buildAdvisoryUnlockDB(t *testing.T, unlockRow pgx.Row) (*mockDB, *mockConn) {
	t.Helper()
	conn := &mockConn{}

	conn.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		if strings.Contains(sql, "pg_advisory_unlock") {
			return unlockRow
		}
		// All other QueryRow calls (e.g. from tx sub-queries) succeed trivially.
		return &mockRow{scanFn: func(dest ...any) error { return nil }}
	}

	// WithTransaction: empty workspace, no legacy state — just return nil so
	// we always reach the advisory-unlock step.
	conn.withTxFn = func(ctx context.Context, fn func(context.Context, database.Tx) error) error {
		return fn(ctx, &mockTx{})
	}

	db := &mockDB{conn: conn}
	db.queryFn = func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return &safeRows{}, nil
	}
	db.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}
	return db, conn
}

// TestRecoverySafety_AdvisoryUnlock_ReturnsFalse_ConnectionDiscarded verifies that
// when pg_advisory_unlock returns false (the lock could not be confirmed released),
// the dedicated connection is discarded and not returned to the pool.
func TestRecoverySafety_AdvisoryUnlock_ReturnsFalse_ConnectionDiscarded(t *testing.T) {
	// Unlock scan succeeds but sets released=false.
	unlockRow := &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				*b = false // NOT released
			}
		}
		return nil
	}}

	db, conn := buildAdvisoryUnlockDB(t, unlockRow)
	pgStore := state.NewPostgresStore(db)

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})
	_, err := pgStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
		WorkspaceID:          "ws-unlock-false",
		ClusterID:            "test-cluster",
		AuthorizeDestructive: true,
		Reason:               "unlock-false test",
	})

	// An unlock failure must surface as an error (fail-closed).
	if err == nil {
		t.Fatal("expected error when pg_advisory_unlock returns false, got nil")
	}
	if !strings.Contains(err.Error(), "unlock returned false") {
		t.Errorf("expected 'unlock returned false' in error, got: %v", err)
	}

	// The connection must have been discarded — never returned to the pool.
	if !conn.wasDiscarded() {
		t.Error("expected conn.Discard() to be called when pg_advisory_unlock returns false")
	}
}

// TestRecoverySafety_AdvisoryUnlock_ScanError_ConnectionDiscarded verifies that
// when pg_advisory_unlock returns a scan error (connection failure mid-unlock),
// the dedicated connection is discarded and the error is surfaced.
func TestRecoverySafety_AdvisoryUnlock_ScanError_ConnectionDiscarded(t *testing.T) {
	unlockErr := errors.New("connection reset by peer during advisory unlock")
	// Unlock scan returns an error (simulates a broken connection).
	unlockRow := &errRow{err: unlockErr}

	db, conn := buildAdvisoryUnlockDB(t, unlockRow)
	pgStore := state.NewPostgresStore(db)

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})
	_, err := pgStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
		WorkspaceID:          "ws-unlock-err",
		ClusterID:            "test-cluster",
		AuthorizeDestructive: true,
		Reason:               "unlock-scan-error test",
	})

	// Unlock error must be surfaced to the caller.
	if err == nil {
		t.Fatal("expected error when pg_advisory_unlock scan fails, got nil")
	}
	if !strings.Contains(err.Error(), "advisory lock") {
		t.Errorf("expected advisory lock mention in error, got: %v", err)
	}

	// Connection must be discarded regardless of work outcome.
	if !conn.wasDiscarded() {
		t.Error("expected conn.Discard() to be called when pg_advisory_unlock scan returns error")
	}
}

// TestRecoverySafety_AdvisoryUnlock_Success_ConnectionNotDiscarded verifies the
// happy path: when pg_advisory_unlock succeeds (returns true without error), the
// connection must NOT be discarded — it should be returned to the pool normally.
func TestRecoverySafety_AdvisoryUnlock_Success_ConnectionNotDiscarded(t *testing.T) {
	unlockRow := &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				*b = true // successfully released
			}
		}
		return nil
	}}

	db, conn := buildAdvisoryUnlockDB(t, unlockRow)
	pgStore := state.NewPostgresStore(db)

	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})
	_, err := pgStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
		WorkspaceID:          "ws-unlock-ok",
		ClusterID:            "test-cluster",
		AuthorizeDestructive: true,
		Reason:               "unlock-success test",
	})

	if err != nil {
		t.Fatalf("expected success when unlock is clean, got: %v", err)
	}

	// Connection must NOT be discarded on the happy path.
	if conn.wasDiscarded() {
		t.Error("expected conn.Discard() NOT to be called when pg_advisory_unlock succeeds")
	}
}

// TestRecoverySafety_AdvisoryUnlock_WorkFails_UnlockFails_BothErrorsSurfaced verifies
// that when both the inner recovery work and the advisory unlock fail, both errors
// are surfaced in the returned error (neither is silently dropped).
func TestRecoverySafety_AdvisoryUnlock_WorkFails_UnlockFails_BothErrorsSurfaced(t *testing.T) {
	workErr := errors.New("inner recovery work failed")

	// Unlock scan returns false (not released).
	unlockRow := &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				*b = false
			}
		}
		return nil
	}}

	conn := &mockConn{}
	conn.queryRowFn = func(ctx context.Context, sql string, args ...any) pgx.Row {
		if strings.Contains(sql, "pg_advisory_unlock") {
			return unlockRow
		}
		return &mockRow{scanFn: func(dest ...any) error { return nil }}
	}
	// The inner recovery work fails via the transaction.
	conn.withTxFn = func(ctx context.Context, fn func(context.Context, database.Tx) error) error {
		return workErr
	}

	db := &mockDB{conn: conn}
	db.queryFn = func(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
		return &safeRows{}, nil
	}
	db.execFn = func(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
		return pgconn.CommandTag{}, nil
	}

	pgStore := state.NewPostgresStore(db)
	adminCtx := auth.WithUser(context.Background(), &auth.User{ID: "admin-1", Role: auth.RoleAdmin})

	_, err := pgStore.RecoverWorkspaceLegacyState(adminCtx, state.LegacyRecoveryParams{
		WorkspaceID:          "ws-both-fail",
		ClusterID:            "test-cluster",
		AuthorizeDestructive: true,
		Reason:               "double-failure test",
	})

	if err == nil {
		t.Fatal("expected error when both work and unlock fail")
	}

	// The error message must mention both failures so they are diagnosable.
	errMsg := err.Error()
	if !strings.Contains(errMsg, "advisory lock") {
		t.Errorf("expected 'advisory lock' in combined error, got: %v", err)
	}
	// The work error must not be silently swallowed.
	if !strings.Contains(errMsg, workErr.Error()) {
		t.Errorf("expected inner work error %q in combined error, got: %v", workErr.Error(), err)
	}
	if !conn.wasDiscarded() {
		t.Error("expected Discard() to be called when unlock fails regardless of work outcome")
	}
}

// ---------------------------------------------------------------------------
// GROUP 3: Concurrent EnsureSchema
//
// 3a. Mock-based: IsDuplicateObject race — verifies that when
//     ADD CONSTRAINT races and returns 42710, EnsureSchema does NOT return
//     an error (it treats duplicate-object as concurrent success).
//
// 3b. Real-DB integration (skipped if no PostgreSQL): 10 goroutines run
//     EnsureSchema simultaneously and all must succeed without data races
//     or spurious errors.
// ---------------------------------------------------------------------------

// duplicateObjectError simulates SQLSTATE 42710 returned by pgconn.
type duplicateObjectError struct{}

func (e *duplicateObjectError) Error() string   { return "ERROR: duplicate object (SQLSTATE 42710)" }
func (e *duplicateObjectError) SQLState() string { return "42710" }

// pgError wraps a SQLSTATE so database.IsDuplicateObject recognises it.
// We use *pgconn.PgError directly since IsDuplicateObject uses errors.As.
func makePgError(code, msg string) error {
	return &pgconn.PgError{Code: code, Message: msg}
}

// ensureSchemaMockDB builds a mock database.DB that simulates the
// IsDuplicateObject (42710) race in ensureWorkspaceRecoveryStatusConstraint.
//
// Sequence for the mock:
//  - First workspaceRecoveryStatusConstraintExists call → false (constraint absent)
//  - reportInvalidWorkspaceRecoveryStatuses query → 0 rows
//  - ALTER TABLE ADD CONSTRAINT → 42710 (concurrent peer won the race)
//  - Second workspaceRecoveryStatusConstraintExists call → true (post-verify)
type ensureSchemaState struct {
	mu            sync.Mutex
	constraintLookupCount int
}

func buildEnsureSchemaConcurrentMockDB(state *ensureSchemaState) *ensureSchemaMockDB {
	return &ensureSchemaMockDB{s: state}
}

type ensureSchemaMockDB struct {
	s *ensureSchemaState
}

func (d *ensureSchemaMockDB) Ping(ctx context.Context) error  { return nil }
func (d *ensureSchemaMockDB) Close() error                    { return nil }
func (d *ensureSchemaMockDB) TestConnection(ctx context.Context) error { return nil }
func (d *ensureSchemaMockDB) WaitForDatabase(ctx context.Context, maxAttempts int, delay ...any) error {
	return nil
}
func (d *ensureSchemaMockDB) Stat() database.PoolStat { return database.PoolStat{} }
func (d *ensureSchemaMockDB) AcquireLock(ctx context.Context, key string) (database.SessionLock, error) {
	return nil, errors.New("not implemented")
}
func (d *ensureSchemaMockDB) Begin(ctx context.Context) (database.Tx, error) {
	return &mockTx{}, nil
}
func (d *ensureSchemaMockDB) WithTransaction(ctx context.Context, fn func(context.Context, database.Tx) error) error {
	return fn(ctx, &mockTx{})
}
func (d *ensureSchemaMockDB) WithConnection(ctx context.Context, fn func(context.Context, database.Connection) error) error {
	return errors.New("not implemented")
}
func (d *ensureSchemaMockDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	// reportInvalidWorkspaceRecoveryStatuses: return empty rows (no invalid statuses).
	return &safeRows{}, nil
}
func (d *ensureSchemaMockDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	// workspaceRecoveryStatusConstraintExists: first call returns false, second returns true.
	d.s.mu.Lock()
	d.s.constraintLookupCount++
	n := d.s.constraintLookupCount
	d.s.mu.Unlock()

	return &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				// First lookup: constraint does not yet exist (triggers ADD CONSTRAINT attempt).
				// Second lookup: constraint exists (post-verify after IsDuplicateObject).
				*b = n > 1
			}
		}
		return nil
	}}
}
func (d *ensureSchemaMockDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	// Main schema CREATE TABLE block → success.
	// ADD CONSTRAINT → return IsDuplicateObject to simulate concurrent peer winning.
	if strings.Contains(sql, "ADD CONSTRAINT chk_state_workspace_recovery_status") {
		return pgconn.CommandTag{}, makePgError("42710", "duplicate object")
	}
	return pgconn.CommandTag{}, nil
}

// TestRecoverySafety_EnsureSchema_DuplicateObjectRaceIsHandled verifies that
// ensureWorkspaceRecoveryStatusConstraint treats a 42710 (duplicate_object) error
// from ADD CONSTRAINT as a concurrent-success and proceeds to verify the constraint,
// returning nil rather than propagating the duplicate-object error.
func TestRecoverySafety_EnsureSchema_DuplicateObjectRaceIsHandled(t *testing.T) {
	st := &ensureSchemaState{}
	db := buildEnsureSchemaConcurrentMockDB(st)

	ctx := context.Background()
	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema with concurrent 42710 race should succeed, got: %v", err)
	}

	// Both constraint lookups must have been performed:
	// 1. Pre-existence check  2. Post-verify after IsDuplicateObject fallthrough.
	st.mu.Lock()
	n := st.constraintLookupCount
	st.mu.Unlock()
	if n < 2 {
		t.Errorf("expected at least 2 constraint existence checks (pre + post-verify), got %d", n)
	}
}

// TestRecoverySafety_EnsureSchema_PostVerifyFails_ReturnsError verifies that if
// the post-migration constraint lookup cannot confirm the constraint exists, an
// error is returned rather than silently succeeding.
func TestRecoverySafety_EnsureSchema_PostVerifyFails_ReturnsError(t *testing.T) {
	// Constraint never appears: both lookups return false.
	db := &alwaysMissingConstraintDB{}
	ctx := context.Background()
	err := state.EnsureSchema(ctx, db)
	if err == nil {
		t.Fatal("expected EnsureSchema to return error when post-verify finds no constraint")
	}
	if !strings.Contains(err.Error(), "chk_state_workspace_recovery_status") {
		t.Errorf("expected constraint name in error, got: %v", err)
	}
}

// alwaysMissingConstraintDB: constraint never shows up, ADD CONSTRAINT succeeds,
// so the post-verify must fail.
type alwaysMissingConstraintDB struct{}

func (d *alwaysMissingConstraintDB) Ping(ctx context.Context) error { return nil }
func (d *alwaysMissingConstraintDB) Close() error                   { return nil }
func (d *alwaysMissingConstraintDB) TestConnection(ctx context.Context) error { return nil }
func (d *alwaysMissingConstraintDB) WaitForDatabase(ctx context.Context, maxAttempts int, delay ...any) error {
	return nil
}
func (d *alwaysMissingConstraintDB) Stat() database.PoolStat { return database.PoolStat{} }
func (d *alwaysMissingConstraintDB) AcquireLock(ctx context.Context, key string) (database.SessionLock, error) {
	return nil, errors.New("not implemented")
}
func (d *alwaysMissingConstraintDB) Begin(ctx context.Context) (database.Tx, error) {
	return &mockTx{}, nil
}
func (d *alwaysMissingConstraintDB) WithTransaction(ctx context.Context, fn func(context.Context, database.Tx) error) error {
	return fn(ctx, &mockTx{})
}
func (d *alwaysMissingConstraintDB) WithConnection(ctx context.Context, fn func(context.Context, database.Connection) error) error {
	return errors.New("not implemented")
}
func (d *alwaysMissingConstraintDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return &safeRows{}, nil
}
func (d *alwaysMissingConstraintDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	// constraint never found
	return &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				*b = false
			}
		}
		return nil
	}}
}
func (d *alwaysMissingConstraintDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

// TestRecoverySafety_EnsureSchema_InvalidRows_BlocksConstraint verifies that
// when reportInvalidWorkspaceRecoveryStatuses detects rows with a bad status,
// EnsureSchema returns an error with those workspace IDs in the diagnostic.
// Importantly, the rows are never deleted or modified — the error is purely
// diagnostic.
func TestRecoverySafety_EnsureSchema_InvalidRows_BlocksConstraint(t *testing.T) {
	db := &invalidRowsDB{}
	ctx := context.Background()
	err := state.EnsureSchema(ctx, db)
	if err == nil {
		t.Fatal("expected EnsureSchema to fail when invalid status rows exist")
	}
	if !strings.Contains(err.Error(), "ws-bad-status") {
		t.Errorf("expected workspace_id in diagnostic error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "UNKNOWN_BAD") {
		t.Errorf("expected invalid status value in diagnostic error, got: %v", err)
	}
	if !strings.Contains(err.Error(), "must be corrected manually") {
		t.Errorf("expected actionable guidance in error, got: %v", err)
	}
	if db.deletesCalled > 0 {
		t.Errorf("rows must never be deleted during EnsureSchema (preserves data): %d deletes", db.deletesCalled)
	}
}

// invalidRowsDB: constraint absent, but invalid status rows exist.
type invalidRowsDB struct {
	deletesCalled int
	lookupCount   int
	mu            sync.Mutex
}

func (d *invalidRowsDB) Ping(ctx context.Context) error { return nil }
func (d *invalidRowsDB) Close() error                   { return nil }
func (d *invalidRowsDB) TestConnection(ctx context.Context) error { return nil }
func (d *invalidRowsDB) WaitForDatabase(ctx context.Context, maxAttempts int, delay ...any) error {
	return nil
}
func (d *invalidRowsDB) Stat() database.PoolStat { return database.PoolStat{} }
func (d *invalidRowsDB) AcquireLock(ctx context.Context, key string) (database.SessionLock, error) {
	return nil, errors.New("not implemented")
}
func (d *invalidRowsDB) Begin(ctx context.Context) (database.Tx, error)       { return &mockTx{}, nil }
func (d *invalidRowsDB) WithTransaction(ctx context.Context, fn func(context.Context, database.Tx) error) error {
	return fn(ctx, &mockTx{})
}
func (d *invalidRowsDB) WithConnection(ctx context.Context, fn func(context.Context, database.Connection) error) error {
	return errors.New("not implemented")
}
func (d *invalidRowsDB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	// Constraint never exists.
	return &mockRow{scanFn: func(dest ...any) error {
		if len(dest) > 0 {
			if b, ok := dest[0].(*bool); ok {
				*b = false
			}
		}
		return nil
	}}
}
func (d *invalidRowsDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "status NOT IN") {
		// Return one invalid row: workspace_id="ws-bad-status", status="UNKNOWN_BAD"
		return &singleRowRows{
			rows: [][]string{{"ws-bad-status", "UNKNOWN_BAD"}},
		}, nil
	}
	return &safeRows{}, nil
}
func (d *invalidRowsDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	upperSQL := strings.ToUpper(sql)
	if strings.Contains(upperSQL, "DELETE") {
		d.mu.Lock()
		d.deletesCalled++
		d.mu.Unlock()
	}
	return pgconn.CommandTag{}, nil
}

// singleRowRows is a pgx.Rows that returns a fixed set of string-pair rows.
type singleRowRows struct {
	rows    [][]string
	pos     int
	current []string
}

func (r *singleRowRows) Close() {}
func (r *singleRowRows) Err() error { return nil }
func (r *singleRowRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *singleRowRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *singleRowRows) Next() bool {
	if r.pos < len(r.rows) {
		r.current = r.rows[r.pos]
		r.pos++
		return true
	}
	return false
}
func (r *singleRowRows) Scan(dest ...any) error {
	if r.current == nil {
		return errors.New("no current row")
	}
	for i, d := range dest {
		if i < len(r.current) {
			if s, ok := d.(*string); ok {
				*s = r.current[i]
			}
		}
	}
	return nil
}
func (r *singleRowRows) Values() ([]any, error)  { return nil, nil }
func (r *singleRowRows) RawValues() [][]byte      { return nil }
func (r *singleRowRows) Conn() *pgx.Conn          { return nil }

// ---------------------------------------------------------------------------
// GROUP 3b: Real-DB concurrent EnsureSchema integration (skipped if no PostgreSQL)
// ---------------------------------------------------------------------------

func getTestDBConfigForSafety(t *testing.T) *config.DatabaseConfig {
	t.Helper()
	host := os.Getenv("TEST_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := 5432
	if s := os.Getenv("TEST_DB_PORT"); s != "" {
		if p, err := strconv.Atoi(s); err == nil && p > 0 {
			port = p
		}
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 200*time.Millisecond)
	if err != nil {
		t.Skipf("PostgreSQL not reachable at %s:%d; skipping real-DB test", host, port)
	}
	_ = conn.Close()

	user := os.Getenv("TEST_DB_USER")
	if user == "" {
		user = "postgres"
	}
	dbName := os.Getenv("TEST_DB_NAME")
	if dbName == "" {
		dbName = "postgres"
	}
	return &config.DatabaseConfig{
		Host:              host,
		Port:              port,
		Database:          dbName,
		User:              user,
		Password:          os.Getenv("TEST_DB_PASSWORD"),
		PoolMax:           20,
		PoolMin:           5,
		PoolIdleTimeout:   10 * time.Second,
		ConnectionTimeout: 3 * time.Second,
	}
}

// TestRecoverySafety_EnsureSchema_ConcurrentRealDB_AllGoroutinesSucceed runs 10
// goroutines each calling EnsureSchema simultaneously against a real PostgreSQL
// instance. All calls must succeed — no goroutine should see an unhandled error.
//
// This is an integration test; it is automatically skipped when PostgreSQL is
// not reachable (CI without a database sidecar).
func TestRecoverySafety_EnsureSchema_ConcurrentRealDB_AllGoroutinesSucceed(t *testing.T) {
	cfg := getTestDBConfigForSafety(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := database.New(ctx, cfg)
	if err != nil {
		t.Skipf("failed to connect to test database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Skipf("database ping failed: %v", err)
	}

	// First ensure the schema exists as a baseline (removes any pre-existing
	// partial state left by a previous test run).
	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("baseline EnsureSchema failed: %v", err)
	}

	const goroutines = 10
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			if err := state.EnsureSchema(ctx, db); err != nil {
				errs <- fmt.Errorf("goroutine %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent EnsureSchema returned error: %v", err)
	}
}

// TestRecoverySafety_EnsureSchema_ConstraintScopedToTable_RealDB verifies that
// workspaceRecoveryStatusConstraintExists scopes the lookup to the correct schema
// and table (so a same-named constraint on a different table is never mistaken for
// the migration having been applied).
//
// Skipped when PostgreSQL is not reachable.
func TestRecoverySafety_EnsureSchema_ConstraintScopedToTable_RealDB(t *testing.T) {
	cfg := getTestDBConfigForSafety(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	db, err := database.New(ctx, cfg)
	if err != nil {
		t.Skipf("failed to connect to test database: %v", err)
	}
	defer db.Close()

	if err := db.Ping(ctx); err != nil {
		t.Skipf("database ping failed: %v", err)
	}

	// Run EnsureSchema; it must place the constraint on state_workspace_recovery.
	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Fatalf("EnsureSchema failed: %v", err)
	}

	// Query pg_catalog to verify the constraint is scoped to the right table + schema.
	var constraintCount int
	err = db.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_catalog.pg_constraint c
		JOIN pg_catalog.pg_class rel ON c.conrelid = rel.oid
		JOIN pg_catalog.pg_namespace nsp ON rel.relnamespace = nsp.oid
		WHERE c.conname = 'chk_state_workspace_recovery_status'
		  AND rel.relname = 'state_workspace_recovery'
		  AND nsp.nspname = current_schema()
		  AND c.contype = 'c'
	`).Scan(&constraintCount)
	if err != nil {
		t.Fatalf("pg_catalog query failed: %v", err)
	}
	if constraintCount != 1 {
		t.Errorf("expected exactly 1 constraint scoped to state_workspace_recovery in current_schema(), got %d", constraintCount)
	}

	// Idempotency: running EnsureSchema again must not return an error.
	if err := state.EnsureSchema(ctx, db); err != nil {
		t.Errorf("EnsureSchema is not idempotent: %v", err)
	}
}
func (r *singleRowRows) TypeMap() *pgtype.Map { return nil }
