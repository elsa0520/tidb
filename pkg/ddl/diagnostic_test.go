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

package ddl

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestDiagnosticDDLStart(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer store.Close()
	defer diagnosticmode.SetForTest(true)()
	d, _ := newDDL(context.Background(), WithStore(store), WithInfoCache(infoschema.NewCache(store, 16)))
	require.IsType(t, schemaver.NewDiagnosticSyncer(), d.SchemaSyncer())
	require.NoError(t, d.SchemaSyncer().Init(context.Background()))
	for _, mode := range []StartMode{Bootstrap, Upgrade, BR} {
		require.ErrorIs(t, d.Start(mode, nil), diagnosticmode.ErrDDLNotAllowed)
	}
	require.NoError(t, d.Start(Normal, nil))
	require.Nil(t, d.sessPool)
	require.Nil(t, d.executor.sessPool)
	require.Nil(t, d.jobSubmitter.sessPool)
	require.Nil(t, d.sysTblMgr)
	require.Nil(t, d.GetMinJobIDRefresher())
	require.Nil(t, d.delRangeMgr)
	require.ErrorIs(t, d.EnableDDL(), diagnosticmode.ErrDDLNotAllowed)
	require.ErrorIs(t, d.SwitchMDL(true), diagnosticmode.ErrDDLNotAllowed)
	require.ErrorIs(t, d.SwitchMDL(false), diagnosticmode.ErrDDLNotAllowed)
	require.NoError(t, d.DisableDDL())
	require.NoError(t, d.Stop())
	require.NoError(t, d.Stop())
}
