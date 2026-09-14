// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schemaver

import (
	"context"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// diagnosticSyncer lets the existing schema loader poll storage without joining
// DDL synchronization. It owns no etcd resources and never acknowledges a job.
type diagnosticSyncer struct{}

var _ Syncer = (*diagnosticSyncer)(nil)

// NewDiagnosticSyncer returns a syncer for periodic, local schema loading only.
func NewDiagnosticSyncer() Syncer { return &diagnosticSyncer{} }

func (*diagnosticSyncer) Init(context.Context) error    { return nil }
func (*diagnosticSyncer) Restart(context.Context) error { return nil }

// Success means reporting is unnecessary, not that a version was published.
func (*diagnosticSyncer) UpdateSelfVersion(context.Context, int64, int64) error { return nil }

// Nil channels disable the watch and lease-recovery select cases, leaving the
// schema loader's ticker and cancellation active. Closed channels would spin.
func (*diagnosticSyncer) GlobalVersionCh() clientv3.WatchChan    { return nil }
func (*diagnosticSyncer) Done() <-chan struct{}                  { return nil }
func (*diagnosticSyncer) WatchGlobalSchemaVer(context.Context)   {}
func (*diagnosticSyncer) SetServerInfoSyncer(*serverinfo.Syncer) {}
func (*diagnosticSyncer) SyncJobSchemaVerLoop(context.Context)   {}
func (*diagnosticSyncer) Close()                                 {}

func (*diagnosticSyncer) OwnerUpdateGlobalVersion(context.Context, int64) error {
	return diagnosticmode.ErrDDLNotAllowed
}
func (*diagnosticSyncer) WaitVersionSynced(context.Context, int64, int64, bool) (*SyncSummary, error) {
	return nil, diagnosticmode.ErrDDLNotAllowed
}
