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

package issyncer

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticSchemaReload(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	defer diagnosticmode.SetForTest(true)()
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer store.Close()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMeta)
	createSchema := func(id int64, name string) {
		require.NoError(t, kv.RunInNewTxn(ctx, store, true, func(_ context.Context, txn kv.Transaction) error {
			m := meta.NewMutator(txn)
			if err := m.CreateDatabase(&model.DBInfo{ID: id, Name: ast.NewCIStr(name), State: model.StatePublic}); err != nil {
				return err
			}
			ver, err := m.GenSchemaVersion()
			if err != nil {
				return err
			}
			return m.SetSchemaDiff(&model.SchemaDiff{Version: ver, Type: model.ActionCreateSchema, SchemaID: id})
		}))
	}
	createSchema(100, "initial")
	cache := infoschema.NewCache(store, 16)
	lease := 100 * time.Millisecond
	s := New(store, cache, lease, nil, isvalidator.New(lease), nil)
	s.InitRequiredFields(nil, schemaver.NewDiagnosticSyncer(), nil, nil)
	require.NoError(t, s.Reload())
	_, ok := s.InfoSchema().SchemaByName(ast.NewCIStr("initial"))
	require.True(t, ok)
	require.EqualValues(t, 1, s.InfoSchema().SchemaMetaVersion())
	require.Nil(t, s.minJobIDRefresher)
	require.Nil(t, s.sysSessionPool)
	// If MDL work is reached, these intentionally absent dependencies expose it.
	s.MDLCheckLoop(context.Background())
	s.mdlCheckCh = make(chan struct{}, 1)
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/ErrorMockReloadFailed", "return(true)")
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); s.SyncLoop(loopCtx) }()
	defer func() { cancel(); <-done }()
	createSchema(101, "updated")
	require.Error(t, s.Reload())
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/issyncer/ErrorMockReloadFailed")
	// No watch channel exists: only the ticker can discover the new schema.
	require.Eventually(t, func() bool {
		_, exists := s.InfoSchema().SchemaByName(ast.NewCIStr("updated"))
		return exists
	}, 5*time.Second, 10*time.Millisecond)
	require.EqualValues(t, 2, s.InfoSchema().SchemaMetaVersion())
	require.Empty(t, s.mdlCheckCh)
}
