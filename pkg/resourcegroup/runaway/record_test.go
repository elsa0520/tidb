// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package runaway

import (
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type stubOwnerManager struct{ owner.Manager }

func TestNewRunawayRecordGCTicker(t *testing.T) {
	t.Cleanup(diagnosticmode.SetForTest(false))
	ticker, tickerCh := newRunawayRecordGCTicker(time.Hour)
	require.NotNil(t, ticker)
	require.NotNil(t, tickerCh)
	ticker.Stop()

	t.Run("diagnostic mode", func(t *testing.T) {
		t.Cleanup(diagnosticmode.SetForTest(true))
		diagnosticTicker, diagnosticTickerCh := newRunawayRecordGCTicker(time.Hour)
		require.Nil(t, diagnosticTicker)
		require.Nil(t, diagnosticTickerCh)
	})
}

func TestNewRunawayManagerInDiagnosticMode(t *testing.T) {
	t.Cleanup(diagnosticmode.SetForTest(true))
	manager := NewRunawayManager(nil, "127.0.0.1:4000", nil, make(chan struct{}), nil, nil)
	require.False(t, manager.watchListStarted)
	require.Nil(t, manager.DeriveChecker("default", "select 1", "sql-digest", "plan-digest", time.Now()))
	manager.Stop()

	t.Run("expired watches do not enqueue cleanup", func(t *testing.T) {
		manager := NewRunawayManager(nil, "test", nil, make(chan struct{}), nil, nil)
		t.Cleanup(manager.Stop)
		for i := range maxWatchRecordChannelSize + 1 {
			manager.AddWatch(&QuarantineRecord{ID: int64(i + 1), EndTime: time.Now().Add(-time.Hour)})
			// Check each send so a regression fails before the queue fills and blocks.
			require.Empty(t, manager.staleQuarantineRecord)
		}
		require.Empty(t, manager.GetWatchList())
	})

	t.Run("duplicate watches do not enqueue cleanup", func(t *testing.T) {
		manager := NewRunawayManager(nil, "test", nil, make(chan struct{}), nil, nil)
		t.Cleanup(manager.Stop)
		for i := range maxWatchRecordChannelSize + 2 {
			manager.AddWatch(&QuarantineRecord{
				ID: int64(i + 1), ResourceGroupName: "default", WatchText: "select 1", EndTime: NullTime,
			})
			require.Empty(t, manager.staleQuarantineRecord)
		}
		watches := manager.GetWatchList()
		require.Len(t, watches, 1)
		require.EqualValues(t, 1, watches[0].ID)
	})

	t.Run("cache mutations do not start callbacks", func(t *testing.T) {
		manager := NewRunawayManager(nil, "test", nil, make(chan struct{}), nil, nil)
		t.Cleanup(manager.Stop)
		// ttlcache invokes registered callbacks in separate goroutines, even without Start.
		require.Nil(t, manager.insertionCancel)
		require.Nil(t, manager.evictionCancel)
		var record *QuarantineRecord
		for i := range maxWatchRecordChannelSize + 2 {
			record = &QuarantineRecord{
				ID: int64(i + 1), ResourceGroupName: "default", WatchText: "select 1",
				Source: ManualSource, EndTime: NullTime,
			}
			manager.AddWatch(record)
		}
		watches := manager.GetWatchList()
		require.Len(t, watches, 1)
		require.Equal(t, record.ID, watches[0].ID)
		manager.removeWatch(record)
		require.Empty(t, manager.GetWatchList())
		require.Empty(t, manager.staleQuarantineRecord)
	})
}

func (stubOwnerManager) IsOwner() bool { return true }

type stubDDL struct {
	ddl.DDL
	om owner.Manager
}

func (s stubDDL) OwnerManager() owner.Manager { return s.om }

func TestRecordKey(t *testing.T) {
	// Initialize test data
	key1 := recordKey{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
	}
	// key2 is identical to key1
	key2 := recordKey{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
	}
	key3 := recordKey{
		ResourceGroupName: "group2",
	}

	// Test MapKey method
	recordMap := make(map[recordKey]*Record)
	record1 := &Record{
		ResourceGroupName: "group1",
		SQLDigest:         "digest1",
		PlanDigest:        "plan1",
	}
	// put key1 into recordMap
	recordMap[key1] = record1
	assert.Len(t, recordMap, 1, "recordMap should have 1 element")
	assert.Equal(t, "group1", recordMap[key1].ResourceGroupName, "Repeats should not be updated")
	assert.Equal(t, 0, recordMap[key1].Repeats, "Repeats should be incremented")
	// key2 is identical to key1, so we can use key2 to get the record
	assert.NotNil(t, recordMap[key1], "key1 should exist in recordMap")
	assert.NotNil(t, recordMap[key2], "key2 should exist in recordMap")
	assert.Nil(t, recordMap[key3], "key3 should not exist in recordMap")

	// put key2 into recordMap and update Repeats
	record2 := &Record{
		ResourceGroupName: "group1",
		Repeats:           1,
	}
	recordMap[key2] = record2
	assert.Len(t, recordMap, 1, "recordMap should have 1 element")
	assert.Equal(t, 1, recordMap[key1].Repeats, "Repeats should be updated")
	// change ResourceGroupName of key2 will not affect key1
	key2.ResourceGroupName = "group2"
	record3 := &Record{
		ResourceGroupName: "group2",
	}
	recordMap[key2] = record3
	assert.Len(t, recordMap, 2, "recordMap should have 1 element")
	assert.Equal(t, "group1", recordMap[key1].ResourceGroupName, "Repeats should not be updated")
	assert.Equal(t, "group2", recordMap[key2].ResourceGroupName, "ResourceGroupName should be updated")
}

func TestDeleteExpiredRowsSkipsUnavailableRunawayTable(t *testing.T) {
	assertDeleteExpiredRowsStaysQuiet := func(t *testing.T, infoCache *infoschema.InfoCache) {
		t.Helper()

		core, recorded := observer.New(zap.ErrorLevel)
		restore := log.ReplaceGlobals(
			zap.New(core),
			&log.ZapProperties{Core: core, Level: zap.NewAtomicLevelAt(zap.InfoLevel)},
		)
		defer restore()

		rm := &Manager{
			ddl:       stubDDL{om: stubOwnerManager{}},
			infoCache: infoCache,
		}
		rm.deleteExpiredRows(time.Second)

		require.Empty(t, recorded.FilterMessage("delete system table failed").All())
	}

	t.Run("nil latest infoschema", func(t *testing.T) {
		assertDeleteExpiredRowsStaysQuiet(t, infoschema.NewCache(nil, 1))
	})

	t.Run("missing runaway table", func(t *testing.T) {
		infoCache := infoschema.NewCache(nil, 1)
		infoCache.Insert(infoschema.MockInfoSchema(nil), uint64(time.Now().Unix()))
		assertDeleteExpiredRowsStaysQuiet(t, infoCache)
	})
}
