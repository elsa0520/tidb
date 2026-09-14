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

package infosync

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestDiagnosticMinStartTS(t *testing.T) {
	if !intest.InTest {
		t.Skip("requires intest")
	}
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	client := cluster.RandClient()
	ctx := context.Background()
	const key = "/tidb/server/minstartts/diagnostic"
	_, err := client.Put(ctx, key, "123")
	require.NoError(t, err)
	defer diagnosticmode.SetForTest(true)()
	is := &InfoSyncer{etcdCli: client, unprefixedEtcdCli: client, minStartTSPath: key, minStartTS: 456}
	// A nil session must never be accessed, even by a direct storage call.
	require.NoError(t, is.storeMinStartTS(ctx, nil))
	is.ReportMinStartTS(nil, nil)
	is.RemoveMinStartTS()
	resp, err := client.Get(ctx, key)
	require.NoError(t, err)
	require.Len(t, resp.Kvs, 1)
	require.Equal(t, "123", string(resp.Kvs[0].Value))
	require.Equal(t, uint64(456), is.GetMinStartTS())
}
