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
	"fmt"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
	"strings"
)

type RuntimeFilterType = variable.RuntimeFilterType
type RuntimeFilterMode = variable.RuntimeFilterMode

type RuntimeFilter struct {
	// runtime filter id, unique in one query plan
	id         int
	buildNode  *PhysicalHashJoin
	srcExpr    *expression.Column
	targetExpr *expression.Column
	rfExpr     expression.Expression
	rfType     RuntimeFilterType
	// The following properties need to be set after assigning a scan node to RF
	rfMode     RuntimeFilterMode
	targetNode *PhysicalTableScan
	// The explain id will be set when runtime filter clone()
	// It is only used for runtime filter pb
	buildNodeExplainId  string
	targetNodeExplainId string
}

func NewRuntimeFilter(rfIdGenerator *util.IdGenerator, eqPredicate *expression.ScalarFunction, buildNode *PhysicalHashJoin) ([]*RuntimeFilter, int64) {
	rightSideIsBuild := buildNode.RightIsBuildSide()
	var srcExpr, targetExpr *expression.Column
	if rightSideIsBuild {
		srcExpr = eqPredicate.GetArgs()[1].(*expression.Column)
		targetExpr = eqPredicate.GetArgs()[0].(*expression.Column)
	} else {
		srcExpr = eqPredicate.GetArgs()[0].(*expression.Column)
		targetExpr = eqPredicate.GetArgs()[1].(*expression.Column)
	}

	rfTypes := buildNode.ctx.GetSessionVars().GetRuntimeFilterTypes()
	result := make([]*RuntimeFilter, 0, len(rfTypes))
	for _, rfType := range rfTypes {
		rf := &RuntimeFilter{
			id:         rfIdGenerator.GetNextId(),
			buildNode:  buildNode,
			srcExpr:    srcExpr,
			targetExpr: targetExpr,
			rfType:     rfType,
		}
		err := rf.constructRFExpression(buildNode.ctx)
		if err == nil {
			result = append(result, rf)
			logutil.BgLogger().Debug("Create a new RF", zap.String("RuntimeFilter", rf.String()))
		} else {
			logutil.BgLogger().Warn("Failed to create rf expr ", zap.Error(err))
		}
	}
	return result, targetExpr.UniqueID
}

func (rf *RuntimeFilter) constructRFExpression(ctx sessionctx.Context) error {
	var err error
	switch rf.rfType {
	case variable.In:
		// The values of In predicate is only a placeholder.
		rf.rfExpr, err = expression.NewFunctionBase(ctx, ast.In, types.NewFieldType(mysql.TypeLonglong), rf.targetExpr, expression.NewOne())
		if err != nil {
			logutil.BgLogger().Warn("Failed to create rf expression", zap.Error(err))
			return err
		}
		return nil
	case variable.MinMax:
		// todo
	}
	return nil
}

func (rf *RuntimeFilter) assign(targetNode *PhysicalTableScan) {
	rf.targetNode = targetNode
	if len(rf.targetNode.runtimeFilterList) == 0 {
		// todo use session variables instead
		rf.targetNode.maxWaitTimeMs = 10000
	}
	rf.buildNode.runtimeFilterList = append(rf.buildNode.runtimeFilterList, rf)
	rf.targetNode.runtimeFilterList = append(rf.targetNode.runtimeFilterList, rf)
	logutil.BgLogger().Debug("Assign RF to target node",
		zap.String("RuntimeFilter", rf.String()))
}

func (rf *RuntimeFilter) ExplainInfo(isBuildNode bool) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "%d[%s]", rf.id, rf.rfType)
	if isBuildNode {
		fmt.Fprintf(&builder, " <- %s", rf.srcExpr.String())
	} else {
		fmt.Fprintf(&builder, " -> %s", rf.targetExpr.String())
	}
	return builder.String()
}

func (rf *RuntimeFilter) String() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "id=%d", rf.id)
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "buildNodeId=%d", rf.buildNode.id)
	builder.WriteString(", ")
	if rf.targetNode == nil {
		fmt.Fprintf(&builder, "targetNodeId=nil")
	} else {
		fmt.Fprintf(&builder, "targetNodeId=%d", rf.targetNode.id)
	}
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "srcColumn=%s", rf.srcExpr.String())
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "targetColumn=%s", rf.targetExpr.String())
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "rfType=%s", rf.rfType)
	builder.WriteString(", ")
	if rf.rfMode == 0 {
		fmt.Fprintf(&builder, "rfMode=nil")
	} else {
		fmt.Fprintf(&builder, "rfMode=%s", rf.rfMode)
	}
	builder.WriteString(", ")
	fmt.Fprintf(&builder, "rfExpr=%s", rf.rfExpr.String())
	builder.WriteString(". ")
	return builder.String()
}

func (rf *RuntimeFilter) Clone() (*RuntimeFilter, error) {
	cloned := new(RuntimeFilter)
	cloned.id = rf.id
	// Because build node only needs to get its executor id attribute when converting to pb format,
	// so we only copy explain id here
	if rf.buildNode == nil {
		cloned.buildNodeExplainId = rf.buildNodeExplainId
	} else {
		cloned.buildNodeExplainId = rf.buildNode.ExplainID().String()
	}
	if rf.targetNode == nil {
		cloned.targetNodeExplainId = rf.targetNodeExplainId
	} else {
		cloned.targetNodeExplainId = rf.targetNode.ExplainID().String()
	}

	cloned.srcExpr = rf.srcExpr.Clone().(*expression.Column)
	cloned.targetExpr = rf.targetExpr.Clone().(*expression.Column)
	cloned.rfType = rf.rfType
	cloned.rfMode = rf.rfMode
	cloned.rfExpr = rf.rfExpr.Clone()
	return cloned, nil
}

func RuntimeFilterListToPB(runtimeFilterList []*RuntimeFilter, sc *stmtctx.StatementContext, client kv.Client) ([]*tipb.RuntimeFilter, error) {
	var result []*tipb.RuntimeFilter
	for _, runtimeFilter := range runtimeFilterList {
		rfPB, err := runtimeFilter.ToPB(sc, client)
		if rfPB == nil {
			return nil, err
		}
		result = append(result, rfPB)
	}
	return result, nil
}

func (rf *RuntimeFilter) ToPB(sc *stmtctx.StatementContext, client kv.Client) (*tipb.RuntimeFilter, error) {
	pc := expression.NewPBConverter(client, sc)
	srcExprPB := pc.ExprToPB(rf.srcExpr)
	if srcExprPB == nil {
		return nil, ErrInternal.GenWithStack("failed to transform src expr %s to pb in runtime filter", rf.srcExpr.String())
	}
	targetExprPB := pc.ExprToPB(rf.targetExpr)
	if targetExprPB == nil {
		return nil, ErrInternal.GenWithStack("failed to transform target expr %s to pb in runtime filter", rf.targetExpr.String())
	}
	rfExprPB := pc.ExprToPB(rf.rfExpr)
	if rfExprPB == nil {
		return nil, ErrInternal.GenWithStack("failed to transform rf expr %s to pb in runtime filter", rf.rfExpr.String())
	}
	rfTypePB := tipb.RuntimeFilterType_IN
	switch rf.rfType {
	case variable.In:
		rfTypePB = tipb.RuntimeFilterType_IN
	case variable.MinMax:
		rfTypePB = tipb.RuntimeFilterType_MIN_MAX
	}
	rfModePB := tipb.RuntimeFilterMode_LOCAL
	switch rf.rfMode {
	case variable.Local:
		rfModePB = tipb.RuntimeFilterMode_LOCAL
	}
	result := &tipb.RuntimeFilter{
		Id:               int32(rf.id),
		SourceExpr:       srcExprPB,
		TargetExpr:       targetExprPB,
		SourceExecutorId: rf.buildNodeExplainId,
		TargetExecutorId: rf.targetNodeExplainId,
		RfExpr:           rfExprPB,
		RfType:           rfTypePB,
		RfMode:           rfModePB,
	}
	return result, nil
}
