package k8s

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "github.com/aaradhychinche-alt/WhatBreaks/gen/go/wb/core/v1"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/coreclient"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/discovery"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/lifecycle"
	"github.com/aaradhychinche-alt/WhatBreaks/internal/logging"
)

var (
	ErrCollectorRunning   = errors.New("k8s collector: already running")
	ErrCollectorStopped   = errors.New("k8s collector: stopped")
	ErrMissingClient      = errors.New("k8s collector: client is required")
	ErrCoreClientNotBound = errors.New("k8s collector: core client is not configured")
)

// Collector coordinates read-only Kubernetes observations, normalizes them into WhatBreaks
// Evidence records, and optionally submits them to the Rust Core Engine.
type Collector struct {
	clusterID         string
	workspaceID       string
	collectorID       string
	clusterWide       bool
	namespaces        []string
	client            Client
	coreClient        *coreclient.Client
	logger            logging.Logger
	reconcileInterval time.Duration
	normalizer        *Normalizer
	tracker           *lifecycle.SimpleTracker
	scopePolicy       *ScopePolicy

	mu            sync.RWMutex
	running       bool
	stopped       bool
	acceptingWork bool
	alive         bool
	ready         bool
	cancelWatch   context.CancelFunc
	stopCh        chan struct{}
}

// New constructs a Kubernetes Collector instance with provided options.
func New(opts ...Option) (*Collector, error) {
	c := &Collector{
		collectorID:       DefaultCollectorID,
		reconcileInterval: 30 * time.Second,
		tracker:           lifecycle.NewSimpleTracker(),
		stopCh:            make(chan struct{}),
		scopePolicy:       DefaultScopePolicy(),
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.scopePolicy == nil {
		c.scopePolicy = DefaultScopePolicy()
	}

	if c.client == nil {
		return nil, ErrMissingClient
	}
	if c.logger == nil {
		c.logger = logging.NewStandardLogger(nil, logging.LevelInfo)
	}

	c.normalizer = NewNormalizer(c.clusterID, c.workspaceID, c.collectorID)
	return c, nil
}

// ---------------------------------------------------------------------------
// Lifecycle Methods (Compatible with internal/lifecycle)
// ---------------------------------------------------------------------------

// Start begins observation collection and marks the collector ready.
func (c *Collector) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return ErrCollectorRunning
	}
	if c.stopped {
		c.mu.Unlock()
		return ErrCollectorStopped
	}

	c.running = true
	c.alive = true
	c.acceptingWork = true
	c.mu.Unlock()

	c.logger.Info("Starting Kubernetes Collector",
		"cluster_id", c.clusterID,
		"workspace_id", c.workspaceID,
		"cluster_wide", c.clusterWide,
		"namespaces", c.namespaces,
	)

	// Verify Kubernetes API connectivity
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()

	if err := c.client.Ping(pingCtx); err != nil {
		c.logger.Warn("Kubernetes API ping check returned error", "error", err.Error())
		// If ping endpoint (/readyz) is unavailable on older/custom clusters, proceed with initial list
	}

	c.mu.Lock()
	c.ready = true
	c.mu.Unlock()

	c.logger.Info("Kubernetes Collector initialized and ready")
	return nil
}

// Stop gracefully stops the collector, cancelling watches and draining work.
func (c *Collector) Stop(ctx context.Context) error {
	c.mu.Lock()
	if !c.running || c.stopped {
		c.mu.Unlock()
		return nil
	}

	c.ready = false
	c.acceptingWork = false
	c.stopped = true

	if c.cancelWatch != nil {
		c.cancelWatch()
	}
	close(c.stopCh)
	c.mu.Unlock()

	c.logger.Info("Kubernetes Collector stopping, draining active tasks")

	// Wait for any in-flight observation runs to finish
	if err := c.tracker.WaitForIdle(ctx); err != nil {
		c.logger.Warn("Wait for idle completed with error", "error", err.Error())
	}

	c.mu.Lock()
	c.alive = false
	c.running = false
	c.mu.Unlock()

	c.logger.Info("Kubernetes Collector stopped")
	return nil
}

// Close is an alias for Stop with a default 10s timeout, matching the lifecycle port contract.
func (c *Collector) Close(ctx context.Context) error {
	return c.Stop(ctx)
}

// StopAcceptingWork immediately flips readiness to false.
func (c *Collector) StopAcceptingWork(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ready = false
	c.acceptingWork = false
	return nil
}

// IsAlive reports whether the collector process is running.
func (c *Collector) IsAlive() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.alive
}

// IsReady reports whether the collector is healthy and connected to Kubernetes.
func (c *Collector) IsReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// WaitForIdle waits for in-flight collection tasks to complete.
func (c *Collector) WaitForIdle(ctx context.Context) error {
	return c.tracker.WaitForIdle(ctx)
}

// WorkspaceID returns the tenant workspace UUID configured for this collector.
func (c *Collector) WorkspaceID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.workspaceID
}

// ClusterID returns the cluster identifier configured for this collector.
func (c *Collector) ClusterID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.clusterID
}

// Ping verifies connectivity to the underlying Kubernetes API server.
func (c *Collector) Ping(ctx context.Context) error {
	c.mu.RLock()
	client := c.client
	c.mu.RUnlock()
	if client == nil {
		return ErrMissingClient
	}
	return client.Ping(ctx)
}

// ---------------------------------------------------------------------------
// Core Collection Sweep (Read-Only)
// ---------------------------------------------------------------------------

// Collect executes a complete read-only sweep of Kubernetes infrastructure,
// returning normalized, immutable WhatBreaks Evidence records.
// If any required scope fails, Collect returns the gathered evidence alongside ErrRequiredScopeFailed.
func (c *Collector) Collect(ctx context.Context) ([]*corev1.Evidence, error) {
	colResult, err := c.CollectDetailed(ctx)
	if err != nil {
		return nil, err
	}
	if colResult.HasRequiredFailures() {
		return colResult.Evidence, fmt.Errorf("%w: %s", discovery.ErrRequiredScopeFailed, strings.Join(colResult.RequiredFailures, ", "))
	}
	return colResult.Evidence, nil
}

// CollectDetailed executes an infrastructure discovery sweep and returns structured telemetry
// distinguishing complete, partial, and failed scope collections.
func (c *Collector) CollectDetailed(ctx context.Context) (*discovery.CollectionResult, error) {
	c.mu.RLock()
	if c.stopped {
		c.mu.RUnlock()
		return nil, ErrCollectorStopped
	}
	policy := c.scopePolicy
	if policy == nil {
		policy = DefaultScopePolicy()
	}
	c.mu.RUnlock()

	var colResult *discovery.CollectionResult

	err := c.tracker.TrackWork(ctx, "k8s-collect-sweep", func(trackCtx context.Context) error {
		var allEvidence []*corev1.Evidence
		successfulScopesMap := make(map[string]bool)
		failedScopesMap := make(map[string]bool)
		var scopeErrors []discovery.ScopeError

		recordSuccess := func(scope string) {
			if !failedScopesMap[scope] {
				successfulScopesMap[scope] = true
			}
		}

		recordFailure := func(scope string, resource string, err error) {
			failedScopesMap[scope] = true
			delete(successfulScopesMap, scope)
			isRequired := policy.IsRequired(scope)
			scopeErrors = append(scopeErrors, discovery.ScopeError{
				Scope:    scope,
				Resource: resource,
				Error:    err.Error(),
				Required: isRequired,
			})
			c.logger.Warn("Failed to list Kubernetes resource scope",
				"scope", scope,
				"resource", resource,
				"required", isRequired,
				"error", err.Error(),
			)
		}

		isContextErr := func(err error) bool {
			return trackCtx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		}

		// 1. Cluster-scoped resources (if ClusterWide is enabled)
		if c.clusterWide {
			// Namespaces
			namespaces, err := c.client.ListNamespaces(trackCtx)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeNamespaces, "Namespace", err)
			} else {
				recordSuccess(ScopeNamespaces)
				for i := range namespaces {
					allEvidence = append(allEvidence, c.normalizer.NormalizeNamespace(&namespaces[i])...)
				}
			}

			// Nodes
			nodes, err := c.client.ListNodes(trackCtx)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeNodes, "Node", err)
			} else {
				recordSuccess(ScopeNodes)
				for i := range nodes {
					allEvidence = append(allEvidence, c.normalizer.NormalizeNode(&nodes[i])...)
				}
			}

			// PersistentVolumes
			pvs, err := c.client.ListPVs(trackCtx)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopePersistentVolumes, "PersistentVolume", err)
			} else {
				recordSuccess(ScopePersistentVolumes)
				for i := range pvs {
					allEvidence = append(allEvidence, c.normalizer.NormalizePV(&pvs[i])...)
				}
			}
		}

		// 2. Determine target namespaces for namespaced resources
		targetNamespaces := c.namespaces
		if len(targetNamespaces) == 0 {
			// If no specific namespaces configured, default to cluster-wide list (namespace="")
			targetNamespaces = []string{""}
		}

		// 3. Collect namespaced resources
		for _, ns := range targetNamespaces {
			select {
			case <-trackCtx.Done():
				return trackCtx.Err()
			default:
			}

			// Deployments
			deployments, err := c.client.ListDeployments(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeDeployments, fmt.Sprintf("Deployment (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeDeployments)
				for i := range deployments {
					allEvidence = append(allEvidence, c.normalizer.NormalizeDeployment(&deployments[i])...)
				}
			}

			// ReplicaSets
			replicaSets, err := c.client.ListReplicaSets(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeReplicaSets, fmt.Sprintf("ReplicaSet (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeReplicaSets)
				for i := range replicaSets {
					allEvidence = append(allEvidence, c.normalizer.NormalizeReplicaSet(&replicaSets[i])...)
				}
			}

			// Services
			services, err := c.client.ListServices(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeServices, fmt.Sprintf("Service (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeServices)
				for i := range services {
					allEvidence = append(allEvidence, c.normalizer.NormalizeService(&services[i])...)
				}
			}

			// Ingresses
			ingresses, err := c.client.ListIngresses(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeIngresses, fmt.Sprintf("Ingress (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeIngresses)
				for i := range ingresses {
					allEvidence = append(allEvidence, c.normalizer.NormalizeIngress(&ingresses[i])...)
				}
			}

			// Pods
			pods, err := c.client.ListPods(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopePods, fmt.Sprintf("Pod (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopePods)
				for i := range pods {
					allEvidence = append(allEvidence, c.normalizer.NormalizePod(&pods[i])...)
				}
			}

			// ConfigMaps
			configMaps, err := c.client.ListConfigMaps(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeConfigMaps, fmt.Sprintf("ConfigMap (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeConfigMaps)
				for i := range configMaps {
					allEvidence = append(allEvidence, c.normalizer.NormalizeConfigMap(&configMaps[i])...)
				}
			}

			// ServiceAccounts
			serviceAccounts, err := c.client.ListServiceAccounts(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeServiceAccounts, fmt.Sprintf("ServiceAccount (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeServiceAccounts)
				for i := range serviceAccounts {
					allEvidence = append(allEvidence, c.normalizer.NormalizeServiceAccount(&serviceAccounts[i])...)
				}
			}

			// Secrets (METADATA ONLY - SECRET SAFETY GUARANTEE)
			secrets, err := c.client.ListSecretsMetadata(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeSecrets, fmt.Sprintf("SecretMetadata (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeSecrets)
				for i := range secrets {
					allEvidence = append(allEvidence, c.normalizer.NormalizeSecret(&secrets[i])...)
				}
			}

			// PVCs
			pvcs, err := c.client.ListPVCs(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopePersistentVolumeClaims, fmt.Sprintf("PersistentVolumeClaim (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopePersistentVolumeClaims)
				for i := range pvcs {
					allEvidence = append(allEvidence, c.normalizer.NormalizePVC(&pvcs[i])...)
				}
			}

			// EndpointSlices (Optional scope under discovery.k8s.io/v1)
			endpointSlices, err := c.client.ListEndpointSlices(trackCtx, ns)
			if err != nil {
				if isContextErr(err) {
					if trackCtx.Err() != nil {
						return trackCtx.Err()
					}
					return err
				}
				recordFailure(ScopeEndpointSlices, fmt.Sprintf("EndpointSlice (namespace: %q)", ns), err)
			} else {
				recordSuccess(ScopeEndpointSlices)
				servicesByName := make(map[string]*Service, len(services))
				for i := range services {
					servicesByName[services[i].ObjectMeta.Name] = &services[i]
				}
				for i := range endpointSlices {
					svcName := ""
					if endpointSlices[i].ObjectMeta.Labels != nil {
						svcName = endpointSlices[i].ObjectMeta.Labels["kubernetes.io/service-name"]
					}
					if svcName == "" {
						for _, owner := range endpointSlices[i].ObjectMeta.OwnerReferences {
							if strings.EqualFold(owner.Kind, "service") && owner.Name != "" {
								svcName = owner.Name
								break
							}
						}
					}
					var associatedSvc *Service
					if svcName != "" {
						associatedSvc = servicesByName[svcName]
					}
					allEvidence = append(allEvidence, c.normalizer.NormalizeEndpointSlice(&endpointSlices[i], associatedSvc)...)
				}
			}
		}

		var succScopes []string
		for s := range successfulScopesMap {
			succScopes = append(succScopes, s)
		}
		sort.Strings(succScopes)

		var failedScopes, reqFailures, optFailures []string
		for s := range failedScopesMap {
			failedScopes = append(failedScopes, s)
			if policy.IsRequired(s) {
				reqFailures = append(reqFailures, s)
			} else {
				optFailures = append(optFailures, s)
			}
		}
		sort.Strings(failedScopes)
		sort.Strings(reqFailures)
		sort.Strings(optFailures)

		status := discovery.CollectionStatusComplete
		complete := true
		if len(reqFailures) > 0 {
			status = discovery.CollectionStatusFailed
			complete = false
		} else if len(optFailures) > 0 {
			status = discovery.CollectionStatusPartial
			complete = false
		}

		colResult = &discovery.CollectionResult{
			Evidence:         allEvidence,
			Status:           status,
			Complete:         complete,
			ScopeErrors:      scopeErrors,
			SuccessfulScopes: succScopes,
			FailedScopes:     failedScopes,
			RequiredFailures: reqFailures,
			OptionalFailures: optFailures,
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return colResult, nil
}

// SubmitToCore sends collected Evidence to the Rust Core Engine DiscoveryService.
func (c *Collector) SubmitToCore(
	ctx context.Context,
	evidenceList []*corev1.Evidence,
) (*corev1.RunDiscoveryResponse, error) {
	c.mu.RLock()
	client := c.coreClient
	c.mu.RUnlock()

	if client == nil {
		return nil, ErrCoreClientNotBound
	}

	if len(evidenceList) == 0 {
		return &corev1.RunDiscoveryResponse{}, nil
	}

	c.logger.Info("Submitting evidence batch to Core Engine",
		"evidence_count", len(evidenceList),
		"cluster_id", c.clusterID,
	)

	return client.RunDiscoveryWithEvidence(ctx, evidenceList)
}
