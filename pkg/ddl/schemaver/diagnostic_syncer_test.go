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

package schemaver_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestDiagnosticSyncer(t *testing.T) {
	// The implementation has no dependency on the process mode or etcd.
	s := schemaver.NewDiagnosticSyncer()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, s.Init(ctx))
	require.NoError(t, s.Restart(ctx))
	for _, jobID := range []int64{0, 42} {
		require.NoError(t, s.UpdateSelfVersion(ctx, jobID, 100))
	}
	require.Nil(t, s.Done())
	require.Nil(t, s.GlobalVersionCh())
	s.WatchGlobalSchemaVer(ctx)
	s.SetServerInfoSyncer(nil)
	s.SyncJobSchemaVerLoop(ctx)
	require.ErrorIs(t, s.OwnerUpdateGlobalVersion(ctx, 100), diagnosticmode.ErrDDLNotAllowed)
	summary, err := s.WaitVersionSynced(ctx, 42, 100, false)
	require.Nil(t, summary)
	require.ErrorIs(t, err, diagnosticmode.ErrDDLNotAllowed)
	s.Close()
	s.Close()
}

func TestDiagnosticNodeDoesNotBlockVersionSync(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	for _, mdl := range []bool{false, true} {
		name := "lease"
		if mdl {
			name = "MDL"
		}
		t.Run(name, func(t *testing.T) {
			if !mdl && kerneltype.IsNextGen() {
				t.Skip("nextgen requires MDL")
			}
			previous := vardef.IsMDLEnabled()
			vardef.SetEnableMDL(mdl)
			defer vardef.SetEnableMDL(previous)
			integration.BeforeTestExternal(t)
			cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
			defer cluster.Terminate(t)
			client := cluster.RandClient()
			ctx := context.Background()
			normalInfo := serverinfo.NewSyncer("normal", func() uint64 { return 1 }, client, nil)
			require.NoError(t, normalInfo.NewSessionAndStoreServerInfo(ctx))
			defer normalInfo.RevokeSession()
			defer normalInfo.RemoveServerInfo()
			normal := schemaver.NewEtcdSyncer(client, "normal")
			normal.SetServerInfoSyncer(normalInfo)
			require.NoError(t, normal.Init(ctx))
			defer normal.Close()
			if mdl {
				watchCtx, cancel := context.WithCancel(ctx)
				done := make(chan struct{})
				go func() { defer close(done); normal.SyncJobSchemaVerLoop(watchCtx) }()
				defer func() { cancel(); <-done }()
			}
			defer diagnosticmode.SetForTest(true)()
			diagnosticInfo := serverinfo.NewSyncer("diagnostic", func() uint64 { return 2 }, client, nil)
			diagnostic := schemaver.NewDiagnosticSyncer()
			before, err := client.Get(ctx, "", clientv3.WithPrefix())
			require.NoError(t, err)
			require.NoError(t, diagnosticInfo.NewSessionAndStoreServerInfo(ctx))
			require.NoError(t, diagnostic.Init(ctx))
			// No acknowledgement, even after a reload; the diagnostic node remains stale.
			require.NoError(t, diagnostic.UpdateSelfVersion(ctx, 42, 1))
			after, err := client.Get(ctx, "", clientv3.WithPrefix())
			require.NoError(t, err)
			require.Equal(t, before.Kvs, after.Kvs)
			// A registered follower really does block completion before acknowledging.
			waitingCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
			_, err = normal.WaitVersionSynced(waitingCtx, 42, 100, false)
			cancel()
			require.Error(t, err)
			require.NoError(t, normal.UpdateSelfVersion(ctx, 42, 100))
			readyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			summary, err := normal.WaitVersionSynced(readyCtx, 42, 100, false)
			require.NoError(t, err)
			require.Equal(t, 1, summary.ServerCount)
			diagnostic.Close()
			diagnosticInfo.RemoveServerInfo()
		})
	}
}
