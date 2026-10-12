package state

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
)

var (
	// ErrRecoveryInProgress is returned when an operation cannot proceed because recovery is actively running.
	ErrRecoveryInProgress = errors.New("state recovery is currently in progress")

	// ErrCoreNotAvailable is returned when the Rust Core Engine cannot be reached for recovery.
	ErrCoreNotAvailable = errors.New("rust core engine is not available for recovery")
)

// WorkspaceLister defines the store contract for enumerating persisted workspaces.
type WorkspaceLister interface {
	ListWorkspaces(ctx context.Context) ([]string, error)
}

// WorkspaceMaterializer defines the contract for pushing a workspace's state to Core Engine.
type WorkspaceMaterializer interface {
	Materialize(ctx context.Context, workspaceID string) (*corev1.LoadStateResponse, error)
}

// CoreLivenessChecker defines the contract for probing Rust Core Engine availability and instance ID.
type CoreLivenessChecker interface {
	CheckLiveness(ctx context.Context) (string, error)
}

// ReadinessSetter updates platform readiness state.
type ReadinessSetter interface {
	SetReady(ready bool)
	IsReady() bool
}

// RecoveryConfig configures timeouts, concurrency, and intervals for RecoveryManager.
type RecoveryConfig struct {
	MaxConcurrency int
	CheckInterval  time.Duration
	ProbeTimeout   time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// DefaultRecoveryConfig returns standard operational defaults for recovery.
func DefaultRecoveryConfig() RecoveryConfig {
	return RecoveryConfig{
		MaxConcurrency: 4,
		CheckInterval:  1 * time.Second,
		ProbeTimeout:   2 * time.Second,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     3 * time.Second,
	}
}

// RecoveryOption configures optional RecoveryManager parameters.
type RecoveryOption func(*RecoveryManager)

// WithRecoveryConfig sets a custom RecoveryConfig.
func WithRecoveryConfig(cfg RecoveryConfig) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.cfg = cfg
	}
}

// WithRecoveryLogger sets a custom logger.
func WithRecoveryLogger(l logging.Logger) RecoveryOption {
	return func(rm *RecoveryManager) {
		if l != nil {
			rm.logger = l
		}
	}
}

// WithRecoveryReadiness binds a ReadinessSetter to coordinate probe readiness.
func WithRecoveryReadiness(r ReadinessSetter) RecoveryOption {
	return func(rm *RecoveryManager) {
		rm.readiness = r
	}
}

// RecoveryManager coordinates initial startup recovery and automatic re-materialization
// after Rust Core Engine restarts.
type RecoveryManager struct {
	store        WorkspaceLister
	materializer WorkspaceMaterializer
	coreClient   CoreLivenessChecker
	readiness    ReadinessSetter
	logger       logging.Logger
	cfg          RecoveryConfig

	mu                  sync.RWMutex
	lastInstanceID      string
	recoveryComplete    bool // tracks whether recovery completed successfully for lastInstanceID
	recoveredWorkspaces map[string]bool
	isRecovering        atomic.Bool
	recoveryMu          sync.Mutex // serializes recovery execution; prevents overlapping runs

	monitorCtx    context.Context
	monitorCancel context.CancelFunc

	stopCh  chan struct{}
	doneCh  chan struct{}
	stopped atomic.Bool

	// Test hooks
	onRecoveryComplete func(instanceID string, workspaces []string)
	onRecoveryFailure  func(err error)
}

// NewRecoveryManager constructs an initialized RecoveryManager.
func NewRecoveryManager(
	store WorkspaceLister,
	materializer WorkspaceMaterializer,
	coreClient CoreLivenessChecker,
	readiness ReadinessSetter,
	logger logging.Logger,
	opts ...RecoveryOption,
) *RecoveryManager {
	if logger == nil {
		logger = logging.NewJSONLogger(nil, logging.LevelInfo, "recovery-manager")
	}

	rm := &RecoveryManager{
		store:               store,
		materializer:        materializer,
		coreClient:          coreClient,
		readiness:           readiness,
		logger:              logger,
		cfg:                 DefaultRecoveryConfig(),
		recoveredWorkspaces: make(map[string]bool),
		stopCh:              make(chan struct{}),
		doneCh:              make(chan struct{}),
	}

	for _, opt := range opts {
		opt(rm)
	}

	return rm
}

// SetOnRecoveryComplete installs a test callback invoked after successful recovery.
func (rm *RecoveryManager) SetOnRecoveryComplete(fn func(instanceID string, workspaces []string)) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.onRecoveryComplete = fn
}

// SetOnRecoveryFailure installs a test callback invoked after a failed recovery attempt.
func (rm *RecoveryManager) SetOnRecoveryFailure(fn func(err error)) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.onRecoveryFailure = fn
}

// IsRecovering reports whether a full recovery cycle is currently executing.
func (rm *RecoveryManager) IsRecovering() bool {
	return rm.isRecovering.Load()
}

// IsRecoveryComplete reports whether the RecoveryManager has successfully finished
// materializing all persisted workspaces for the currently active server instance.
func (rm *RecoveryManager) IsRecoveryComplete() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.recoveryComplete
}

// IsWorkspaceReady reports whether workspaceID is known to have been materialized
// into the currently active Rust Core Engine instance.
func (rm *RecoveryManager) IsWorkspaceReady(workspaceID string) bool {
	if rm.isRecovering.Load() {
		return false
	}
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	if !rm.recoveryComplete {
		return false
	}
	return rm.recoveredWorkspaces[workspaceID]
}

// RecoveredInstanceID returns the Core Engine instance ID that was most recently recovered.
func (rm *RecoveryManager) RecoveredInstanceID() string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.lastInstanceID
}

// RecoverAll executes a complete recovery sweep across all persisted workspaces:
// 1. Probes Core Engine for liveness and active starting instance ID.
// 2. Marks readiness false and invalidates recovery-complete state.
// 3. Enumerates all workspaces from store.
// 4. Concurrently materializes each workspace with bounded concurrency.
// 5. Validates instance consistency after materialization (verifying Rust did not restart mid-recovery).
// 6. If all workspaces succeed and instance is verified, marks them ready and restores readiness.
// 7. If any workspace fails or instance changed, keeps readiness false and returns error.
func (rm *RecoveryManager) RecoverAll(ctx context.Context) error {
	rm.recoveryMu.Lock()
	defer rm.recoveryMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	rm.isRecovering.Store(true)
	defer rm.isRecovering.Store(false)

	if rm.readiness != nil {
		rm.readiness.SetReady(false)
	}

	rm.mu.Lock()
	rm.recoveryComplete = false
	rm.mu.Unlock()

	// 1. Verify Core Engine availability and record initial instance ID
	probeCtx, probeCancel := context.WithTimeout(ctx, rm.probeTimeout())
	startInstanceID, err := rm.coreClient.CheckLiveness(probeCtx)
	probeCancel()
	if err != nil {
		rm.logger.Warn("Core engine not reachable during recovery", "error", err.Error())
		rm.notifyFailure(err)
		return fmt.Errorf("%w: %v", ErrCoreNotAvailable, err)
	}

	// 2. Enumerate persisted workspaces from durable store
	workspaces, err := rm.store.ListWorkspaces(ctx)
	if err != nil {
		rm.logger.Error("Failed to enumerate workspaces from store during recovery", "error", err.Error())
		rm.notifyFailure(err)
		return fmt.Errorf("failed to enumerate workspaces: %w", err)
	}

	rm.logger.Info("Starting recovery for persisted workspaces",
		"workspace_count", len(workspaces),
		"instance_id", startInstanceID,
	)

	// 3. Handle empty database: zero workspaces to recover
	if len(workspaces) == 0 {
		// Verify instance consistency before declaring empty-database recovery complete
		finalProbeCtx, finalProbeCancel := context.WithTimeout(ctx, rm.probeTimeout())
		finalInstanceID, err := rm.coreClient.CheckLiveness(finalProbeCtx)
		finalProbeCancel()
		if err != nil {
			rm.logger.Warn("Core engine became unreachable during empty-database recovery validation", "error", err.Error())
			rm.notifyFailure(err)
			return fmt.Errorf("%w: %v", ErrCoreNotAvailable, err)
		}
		if finalInstanceID != startInstanceID {
			err := fmt.Errorf("core engine restarted during empty-database recovery: instance changed from %q to %q", startInstanceID, finalInstanceID)
			rm.logger.Warn(err.Error())
			rm.notifyFailure(err)
			return err
		}

		rm.mu.Lock()
		rm.lastInstanceID = startInstanceID
		rm.recoveryComplete = true
		rm.recoveredWorkspaces = make(map[string]bool)
		rm.mu.Unlock()

		if rm.readiness != nil {
			rm.readiness.SetReady(true)
		}
		rm.logger.Info("No persisted workspaces to recover; startup recovery complete", "instance_id", startInstanceID)
		rm.notifyComplete(startInstanceID, workspaces)
		return nil
	}

	// 4. Materialize workspaces with bounded concurrency
	concurrency := rm.cfg.MaxConcurrency
	if concurrency <= 0 {
		concurrency = 4
	}
	sem := make(chan struct{}, concurrency)

	var (
		errMu       sync.Mutex
		firstErr    error
		successList []string
		wg          sync.WaitGroup
	)

	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

workerLoop:
	for _, ws := range workspaces {
		select {
		case <-workerCtx.Done():
			break workerLoop
		case sem <- struct{}{}:
		}

		errMu.Lock()
		hasFailed := firstErr != nil
		errMu.Unlock()
		if hasFailed {
			<-sem
			break workerLoop
		}

		wg.Add(1)
		go func(targetWS string) {
			defer wg.Done()
			defer func() { <-sem }()

			_, matErr := rm.materializer.Materialize(workerCtx, targetWS)
			errMu.Lock()
			defer errMu.Unlock()
			if matErr != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("failed to materialize workspace %q: %w", targetWS, matErr)
					workerCancel()
				}
				return
			}
			successList = append(successList, targetWS)
		}(ws)
	}

	wg.Wait()

	if err := workerCtx.Err(); err != nil && firstErr == nil {
		firstErr = err
	}

	if firstErr != nil {
		rm.logger.Error("Workspace recovery failed", "error", firstErr.Error())
		rm.notifyFailure(firstErr)
		return firstErr
	}

	// 5. FIX 2: Validate server instance consistency after materialization
	finalProbeCtx, finalProbeCancel := context.WithTimeout(ctx, rm.probeTimeout())
	finalInstanceID, err := rm.coreClient.CheckLiveness(finalProbeCtx)
	finalProbeCancel()
	if err != nil {
		err := fmt.Errorf("core engine became unreachable after workspace materialization: %w", err)
		rm.logger.Warn("Core engine unreachable during post-recovery validation", "error", err.Error())
		rm.notifyFailure(err)
		return err
	}
	if finalInstanceID != startInstanceID {
		err := fmt.Errorf("core engine restarted during recovery: instance changed from %q to %q", startInstanceID, finalInstanceID)
		rm.logger.Warn("Server instance changed during recovery; materialization invalidated",
			"start_instance_id", startInstanceID,
			"final_instance_id", finalInstanceID,
		)
		rm.notifyFailure(err)
		return err
	}

	// 6. Update recovery state atomically
	rm.mu.Lock()
	rm.lastInstanceID = startInstanceID
	rm.recoveryComplete = true
	rm.recoveredWorkspaces = make(map[string]bool, len(successList))
	for _, ws := range successList {
		rm.recoveredWorkspaces[ws] = true
	}
	rm.mu.Unlock()

	if rm.readiness != nil {
		rm.readiness.SetReady(true)
	}

	rm.logger.Info("Successfully recovered all workspaces into Core Engine",
		"workspace_count", len(successList),
		"instance_id", startInstanceID,
	)
	rm.notifyComplete(startInstanceID, successList)
	return nil
}

// StartMonitor launches a background supervisor goroutine that periodically probes
// the Core Engine, detects process restarts (via instance ID changes or reconnection),
// and triggers automatic recovery.
func (rm *RecoveryManager) StartMonitor(ctx context.Context) {
	rm.mu.Lock()
	if rm.monitorCancel != nil {
		rm.mu.Unlock()
		return
	}
	mCtx, mCancel := context.WithCancel(ctx)
	rm.monitorCtx = mCtx
	rm.monitorCancel = mCancel
	rm.mu.Unlock()

	go rm.monitorLoop(mCtx)
}

func (rm *RecoveryManager) monitorLoop(ctx context.Context) {
	defer close(rm.doneCh)

	ticker := time.NewTicker(rm.cfg.CheckInterval)
	defer ticker.Stop()

	backoff := rm.cfg.InitialBackoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-rm.stopCh:
			return
		case <-ticker.C:
		}

		probeCtx, probeCancel := context.WithTimeout(ctx, rm.probeTimeout())
		instanceID, err := rm.coreClient.CheckLiveness(probeCtx)
		probeCancel()

		if err != nil {
			rm.logger.Debug("Core engine probe unsuccessful", "error", err.Error())
			if rm.readiness != nil {
				rm.readiness.SetReady(false)
			}
			continue
		}

		rm.mu.RLock()
		recoveredInstance := rm.lastInstanceID
		isComplete := rm.recoveryComplete
		rm.mu.RUnlock()

		// Fix 1: If probe succeeded against the already-recovered instance, restore readiness
		if instanceID != "" && instanceID == recoveredInstance && isComplete {
			if rm.readiness != nil && !rm.readiness.IsReady() {
				rm.logger.Info("Core engine liveness restored on already-recovered instance; restoring readiness",
					"instance_id", instanceID,
				)
				rm.readiness.SetReady(true)
			}
			backoff = rm.cfg.InitialBackoff
			continue
		}

		// Instance ID changed or recovery was never completed for this instance:
		// readiness must remain false while we execute recovery
		if rm.readiness != nil {
			rm.readiness.SetReady(false)
		}

		rm.logger.Warn("Core engine instance change or incomplete recovery detected; initiating recovery",
			"recovered_instance_id", recoveredInstance,
			"current_instance_id", instanceID,
			"recovery_complete", isComplete,
		)

		recCtx, recCancel := context.WithTimeout(ctx, 30*time.Second)
		recErr := rm.RecoverAll(recCtx)
		recCancel()

		if recErr != nil {
			rm.logger.Error("Automatic recovery attempt failed, will retry with backoff",
				"error", recErr.Error(),
				"backoff", backoff,
			)
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
				backoff = minDuration(backoff*2, rm.cfg.MaxBackoff)
			case <-ctx.Done():
				t.Stop()
				return
			case <-rm.stopCh:
				t.Stop()
				return
			}
		} else {
			backoff = rm.cfg.InitialBackoff
		}
	}
}

// Stop cleanly terminates the monitor loop and in-flight recovery operations.
// Stop is safe to call repeatedly.
func (rm *RecoveryManager) Stop() {
	if rm.stopped.CompareAndSwap(false, true) {
		rm.mu.Lock()
		started := rm.monitorCancel != nil
		if started {
			rm.monitorCancel()
		}
		rm.mu.Unlock()

		close(rm.stopCh)
		if started {
			select {
			case <-rm.doneCh:
			case <-time.After(2 * time.Second):
			}
		}
	} else {
		rm.mu.RLock()
		started := rm.monitorCancel != nil
		rm.mu.RUnlock()
		if started {
			select {
			case <-rm.doneCh:
			case <-time.After(2 * time.Second):
			}
		}
	}
}

func (rm *RecoveryManager) notifyComplete(instanceID string, workspaces []string) {
	rm.mu.RLock()
	cb := rm.onRecoveryComplete
	rm.mu.RUnlock()
	if cb != nil {
		cb(instanceID, workspaces)
	}
}

func (rm *RecoveryManager) notifyFailure(err error) {
	rm.mu.RLock()
	cb := rm.onRecoveryFailure
	rm.mu.RUnlock()
	if cb != nil {
		cb(err)
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (rm *RecoveryManager) probeTimeout() time.Duration {
	if rm.cfg.ProbeTimeout <= 0 {
		return 2 * time.Second
	}
	return rm.cfg.ProbeTimeout
}
