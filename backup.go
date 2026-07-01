package wavespan

import (
	"context"

	wavespanv1 "github.com/yannick/wavespan-sdk/internal/gen/wavespan/v1"
)

// Backup types are aliased from the generated package so callers name them without importing internal/gen.
// A backup is a consistent, point-in-time (HLC-frontier) export of the cluster to an object store,
// coordinated by whichever node serves the call and fanned out to every node (design/backup phase 3).
type (
	// BackupSpec is the request for a new backup: what to include, which planes, an optional parent
	// (incremental base), and the destination object store.
	BackupSpec = wavespanv1.BackupSpec
	// Selection narrows a backup to a subset of data; the zero value (empty) means everything.
	Selection = wavespanv1.Selection
	// Destination describes an object-store target; the zero value uses the node's configured default.
	Destination = wavespanv1.Destination
	// CredentialRef references (or, as an escape hatch, inlines) object-store credentials. Prefer
	// SecretName; inline keys travel only over the mTLS data port and are never persisted or logged.
	CredentialRef = wavespanv1.CredentialRef
	// BackupState is a backup's full status snapshot (status, phase, per-node progress, coverage gaps).
	BackupState = wavespanv1.BackupState
	// BackupSummary is a compact catalog entry returned by List.
	BackupSummary = wavespanv1.BackupSummary
	// NodeProgress is one node's export progress within a backup.
	NodeProgress = wavespanv1.NodeProgress
	// DestinationInfo is a configured destination's non-secret descriptor (never carries credentials).
	DestinationInfo = wavespanv1.DestinationInfo
	// BackupStatusCode is a backup's lifecycle status; compare against the Backup* constants.
	BackupStatusCode = wavespanv1.BackupStatus
	// BackupPlane selects the logical and/or physical export plane.
	BackupPlane = wavespanv1.BackupPlane
)

// Backup lifecycle statuses.
const (
	BackupRunning  BackupStatusCode = wavespanv1.BackupStatus_BACKUP_RUNNING
	BackupComplete BackupStatusCode = wavespanv1.BackupStatus_BACKUP_COMPLETE
	BackupPartial  BackupStatusCode = wavespanv1.BackupStatus_BACKUP_PARTIAL // some ranges had no live holder
	BackupFailed   BackupStatusCode = wavespanv1.BackupStatus_BACKUP_FAILED
)

// Export planes: logical (row-level, portable) and/or physical (checkpoint, incremental-capable).
const (
	BackupPlaneLogical  BackupPlane = wavespanv1.BackupPlane_BACKUP_PLANE_LOGICAL
	BackupPlanePhysical BackupPlane = wavespanv1.BackupPlane_BACKUP_PLANE_PHYSICAL
)

// BackupClient is the ergonomic client for cluster backups (design/backup phase 3). The node serving
// the call coordinates the backup and fans work out to every node; results carry ResponseMeta. Obtain
// one via [Client.Backup]. Backups run server-side — Begin returns as soon as the backup is admitted;
// poll Status (or List) for progress and completion.
type BackupClient struct{ c *Client }

// Backup returns a client for the cluster backup control plane.
func (c *Client) Backup() *BackupClient { return &BackupClient{c: c} }

// Begin records a durable intent, pins a cluster HLC frontier, and drives the phased backup
// (assign→prepare→export→commit). It returns the allocated backup id; the backup continues
// server-side, so poll [BackupClient.Status] for progress. A nil spec backs up everything to the
// node's default destination.
func (b *BackupClient) Begin(ctx context.Context, spec *BackupSpec) (string, error) {
	resp, err := b.c.backup.BeginBackup(ctx, &wavespanv1.BeginBackupRequest{Spec: spec})
	if err != nil {
		return "", wrapErr("BeginBackup", err)
	}
	return resp.GetBackupId(), nil
}

// Status reports a backup's current state: overall status/phase, per-node progress, percent complete,
// and any coverage gaps (ranges with no live holder at the frontier).
func (b *BackupClient) Status(ctx context.Context, backupID string) (*BackupState, error) {
	resp, err := b.c.backup.BackupStatus(ctx, &wavespanv1.BackupStatusRequest{BackupId: backupID})
	if err != nil {
		return nil, wrapErr("BackupStatus", err)
	}
	return resp, nil
}

// List returns the known backups from the cluster's catalog (most-recent state per backup id).
func (b *BackupClient) List(ctx context.Context) ([]*BackupSummary, error) {
	resp, err := b.c.backup.ListBackups(ctx, &wavespanv1.ListBackupsRequest{})
	if err != nil {
		return nil, wrapErr("ListBackups", err)
	}
	return resp.GetBackups(), nil
}

// Delete removes a backup's catalog intent and its objects. It is chain-aware: deleting a backup that
// has live incremental children fails unless force=true (which cascades to the dependent children).
func (b *BackupClient) Delete(ctx context.Context, backupID string, force bool) (bool, error) {
	resp, err := b.c.backup.DeleteBackup(ctx, &wavespanv1.DeleteBackupRequest{BackupId: backupID, Force: force})
	if err != nil {
		return false, wrapErr("DeleteBackup", err)
	}
	return resp.GetDeleted(), nil
}

// ListDestinations reports the node's configured backup destinations (the default plus any named
// targets) as non-secret descriptors — credentials are never returned.
func (b *BackupClient) ListDestinations(ctx context.Context) (*wavespanv1.ListDestinationsResult, error) {
	resp, err := b.c.backup.ListDestinations(ctx, &wavespanv1.ListDestinationsRequest{})
	if err != nil {
		return nil, wrapErr("ListDestinations", err)
	}
	return resp, nil
}
