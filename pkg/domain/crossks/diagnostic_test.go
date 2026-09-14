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

package crossks

import (
	"context"
	"testing"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestDiagnosticCrossKeyspaceRuntime(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 2})
	defer cluster.Terminate(t)
	client := cluster.Client(0)
	cluster.TakeClient(0)
	observer := cluster.Client(1)
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer store.Close()
	// The cross-keyspace loader reads only the existing system database.
	// Prepare that metadata without invoking bootstrap or DDL.
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMeta)
	require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
		return meta.NewMutator(txn).CreateSysDatabaseByID("mysql", metadef.SystemDatabaseID)
	}))
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/beforeGetStore",
		func(fn *func(string) (kv.Storage, error)) {
			*fn = func(string) (kv.Storage, error) { return store, nil }
		})
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/injectETCDCli",
		func(cli **clientv3.Client, _ string) { *cli = client })
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/domain/crossks/skipCloseStore", func(closeStore *bool) { *closeStore = false })
	defer diagnosticmode.SetForTest(true)()
	before, err := observer.Get(context.Background(), "", clientv3.WithPrefix())
	require.NoError(t, err)
	mgr := NewManager(store)
	runtime, err := mgr.createSessionManager(keyspace.System, func(string, validatorapi.Validator) pools.Factory {
		return func() (pools.Resource, error) { panic("diagnostic schema reload must not borrow DDL sessions") }
	})
	require.NoError(t, err)
	require.IsType(t, schemaver.NewDiagnosticSyncer(), runtime.schemaVerSyncer)
	require.Nil(t, runtime.ddlClient)
	require.NotNil(t, runtime.svrInfoSyncer.GetLocalServerInfo())
	require.NoError(t, runtime.isSyncer.Reload())
	runtime.close()
	after, err := observer.Get(context.Background(), "", clientv3.WithPrefix())
	require.NoError(t, err)
	require.Equal(t, before.Kvs, after.Kvs)
	leases, err := observer.Leases(context.Background())
	require.NoError(t, err)
	require.Empty(t, leases.Leases)
}
