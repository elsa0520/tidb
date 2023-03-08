// Copyright 2022 PingCAP, Inc.
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

package core

import (
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// RuntimeFilterGenerator One plan one generator
type RuntimeFilterGenerator struct {
	rfIdGenerator                 *util.IdGenerator
	columnUniqueIdToRF            map[int64][]*RuntimeFilter
	parentPhysicalPlan            PhysicalPlan
	childIdxForParentPhysicalPlan int
}

// GenerateRuntimeFilter is the root method.
// It traverses the entire tree in preorder.
// It constructs RF when encountering hash join, and allocate RF when encountering table scan.
// It realizes the planning of RF in the entire plan tree.
// todo for example
func (generator *RuntimeFilterGenerator) GenerateRuntimeFilter(plan PhysicalPlan) {
	switch physicalPlan := plan.(type) {
	case *PhysicalHashJoin:
		generator.generateRuntimeFilterInterval(physicalPlan)
	case *PhysicalTableScan:
		generator.assignRuntimeFilter(physicalPlan)
	case *PhysicalTableReader:
		generator.parentPhysicalPlan = plan
		generator.childIdxForParentPhysicalPlan = 0
		generator.GenerateRuntimeFilter(physicalPlan.tablePlan)
		physicalTableReader := plan.(*PhysicalTableReader)
		if physicalTableReader.StoreType == kv.TiFlash {
			physicalTableReader.TablePlans = flattenPushDownPlan(physicalTableReader.tablePlan)
		}
	}

	// todo consider left build right probe
	for i, children := range plan.Children() {
		generator.parentPhysicalPlan = plan
		generator.childIdxForParentPhysicalPlan = i
		generator.GenerateRuntimeFilter(children)
	}
}

func (generator *RuntimeFilterGenerator) generateRuntimeFilterInterval(plan PhysicalPlan) {
	hashJoinPlan := plan.(*PhysicalHashJoin)
	// precondition: the storage type of hash join must be TiFlash
	if hashJoinPlan.storeTp != kv.TiFlash {
		logutil.BgLogger().Warn("RF only support TiFlash compute engine while storage type of hash join node is not TiFlash",
			zap.Int("PhysicalHashJoinId", hashJoinPlan.id),
			zap.String("StoreTP", hashJoinPlan.storeTp.Name()))
		return
	}
	// check hash join pattern
	if !generator.matchRFJoinType(hashJoinPlan) {
		return
	}
	// check eq predicate pattern
	for _, eqPredicate := range hashJoinPlan.EqualConditions {
		if generator.matchEQPredicate(eqPredicate, hashJoinPlan.RightIsBuildSide()) {
			// construct runtime filter
			newRFList, targetColumnUniqueId := NewRuntimeFilter(generator.rfIdGenerator, eqPredicate, hashJoinPlan)
			// update generator rf list
			rfList := generator.columnUniqueIdToRF[targetColumnUniqueId]
			if rfList == nil {
				generator.columnUniqueIdToRF[targetColumnUniqueId] = newRFList
			} else {
				// todo how to create a ref for a list
				//rfList = append(rfList, newRFList...)
				generator.columnUniqueIdToRF[targetColumnUniqueId] = append(generator.columnUniqueIdToRF[targetColumnUniqueId], newRFList...)
			}
		}
	}
}

func (generator *RuntimeFilterGenerator) assignRuntimeFilter(plan PhysicalPlan) {
	// only full scan
	physicalTableScan := plan.(*PhysicalTableScan)
	if !physicalTableScan.isFullScan() {
		return
	}
	// match rf for current scan node
	var rfListForCurrentScan []*RuntimeFilter
	for _, scanOutputColumn := range physicalTableScan.schema.Columns {
		currentColumnRFList := generator.columnUniqueIdToRF[scanOutputColumn.UniqueID]
		if len(currentColumnRFList) != 0 {
			rfListForCurrentScan = append(rfListForCurrentScan, currentColumnRFList...)
		}
	}
	// no runtime filter assign to this scan node
	if len(rfListForCurrentScan) == 0 {
		return
	}

	cacheBuildNodeIdToRFMode := map[int]RuntimeFilterMode{}
	var currentRFExprList []expression.Expression
	for i, runtimeFilter := range rfListForCurrentScan {
		// compute rf mode
		var rfMode RuntimeFilterMode
		if cacheBuildNodeIdToRFMode[runtimeFilter.buildNode.id] != 0 {
			rfMode = cacheBuildNodeIdToRFMode[runtimeFilter.buildNode.id]
		} else {
			rfMode = generator.calculateRFMode(runtimeFilter.buildNode, physicalTableScan)
			cacheBuildNodeIdToRFMode[runtimeFilter.buildNode.id] = rfMode
		}
		// todo support global RF
		switch rfMode {
		case variable.Global:
			rfListForCurrentScan = append(rfListForCurrentScan[:i], rfListForCurrentScan[i+1:]...)
			logutil.BgLogger().Debug("Now we don't support global RF. Remove it",
				zap.Int("BuildNodeId", runtimeFilter.buildNode.id),
				zap.Int("TargetNodeId", physicalTableScan.id))
			continue
		}
		runtimeFilter.rfMode = rfMode
		// assign rf to current node
		runtimeFilter.assign(physicalTableScan)
		currentRFExprList = append(currentRFExprList, runtimeFilter.rfExpr)
	}

	if len(currentRFExprList) == 0 {
		return
	}
	// supply selection if there is no predicates above target scan node
	if parent, ok := generator.parentPhysicalPlan.(*PhysicalSelection); !ok {
		// StatsInfo: Just set a placeholder value here, and this value will not be used in subsequent optimizations
		sel := PhysicalSelection{hasRFConditions: true}.Init(plan.SCtx(), plan.statsInfo(), plan.SelectBlockOffset())
		sel.fromDataSource = true
		sel.SetChildren(plan)
		generator.parentPhysicalPlan.SetChild(generator.childIdxForParentPhysicalPlan, sel)
	} else {
		parent.hasRFConditions = true
	}

	// todo
	// filter predicate selectivity, A scan node does not need many RFs, and the same column does not need many RFs
}

func (generator *RuntimeFilterGenerator) matchRFJoinType(hashJoinPlan *PhysicalHashJoin) bool {
	if hashJoinPlan.RightIsBuildSide() {
		// case1: build side is on the right
		if hashJoinPlan.JoinType == LeftOuterJoin || hashJoinPlan.JoinType == AntiSemiJoin ||
			hashJoinPlan.JoinType == LeftOuterSemiJoin || hashJoinPlan.JoinType == AntiLeftOuterSemiJoin {
			logutil.BgLogger().Debug("Join type does not match RF pattern when build side is on the right",
				zap.Int32("PlanNodeId", int32(hashJoinPlan.id)),
				zap.String("JoinType", hashJoinPlan.JoinType.String()))
			return false
		}
	} else {
		// case2: build side is on the left
		if hashJoinPlan.JoinType == RightOuterJoin {
			logutil.BgLogger().Debug("Join type does not match RF pattern when build side is on the left",
				zap.Int32("PlanNodeId", int32(hashJoinPlan.id)),
				zap.String("JoinType", hashJoinPlan.JoinType.String()))
			return false
		}
	}
	return true
}

func (generator *RuntimeFilterGenerator) matchEQPredicate(eqPredicate *expression.ScalarFunction,
	rightIsBuildSide bool) bool {
	var targetColumn, srcColumn *expression.Column
	if rightIsBuildSide {
		targetColumn = eqPredicate.GetArgs()[0].(*expression.Column)
		srcColumn = eqPredicate.GetArgs()[1].(*expression.Column)
	} else {
		targetColumn = eqPredicate.GetArgs()[1].(*expression.Column)
		srcColumn = eqPredicate.GetArgs()[0].(*expression.Column)
	}
	// match target column
	// condition1: the target column must be real column
	// condition2: the target column has not undergone any transformation
	// todo: cast expr
	if targetColumn.IsHidden || targetColumn.OrigName == "" {
		logutil.BgLogger().Debug("Target column does not match RF pattern",
			zap.String("EQPredicate", eqPredicate.String()),
			zap.String("TargetColumn", targetColumn.String()),
			zap.Bool("IsHidden", targetColumn.IsHidden),
			zap.String("OrigName", targetColumn.OrigName))
		return false
	}
	// match data type
	srcColumnType := srcColumn.GetType().GetType()
	if srcColumnType == mysql.TypeJSON || srcColumnType == mysql.TypeBlob ||
		srcColumnType == mysql.TypeLongBlob || srcColumnType == mysql.TypeMediumBlob ||
		srcColumnType == mysql.TypeTinyBlob || srcColumn.GetType().Hybrid() || srcColumn.GetType().IsArray() {
		logutil.BgLogger().Debug("Src column type does not match RF pattern",
			zap.String("EQPredicate", eqPredicate.String()),
			zap.String("SrcColumn", srcColumn.String()),
			zap.String("SrcColumnType", srcColumn.GetType().String()))
		return false
	}
	return true
}

func (generator *RuntimeFilterGenerator) calculateRFMode(buildNode *PhysicalHashJoin, targetNode *PhysicalTableScan) variable.RuntimeFilterMode {
	if generator.belongsToSameFragment(buildNode, targetNode) {
		return variable.Local
	} else {
		return variable.Global
	}
}

func (generator *RuntimeFilterGenerator) belongsToSameFragment(currentNode PhysicalPlan, targetNode *PhysicalTableScan) bool {
	switch currentNode.(type) {
	case *PhysicalExchangeReceiver:
		// terminal traversal
		return false
	case *PhysicalTableScan:
		if currentNode.ID() == targetNode.id {
			return true
		}
		return false
	default:
		for _, childNode := range currentNode.Children() {
			if generator.belongsToSameFragment(childNode, targetNode) {
				return true
			}
		}
		return false
	}
}
