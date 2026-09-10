// Copyright 2023 PingCAP, Inc.
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

package domain

import (
	"context"
	"net"
	"strconv"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/constants"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	"go.uber.org/zap"
)

func (do *Domain) initResourceGroupsController(ctx context.Context, pdClient pd.Client, uniqueID uint64) error {
	if !shouldStartResourceControlBackgroundTasks() {
		logutil.BgLogger().Info("don't run resource group controller", zap.String("reason", "diagnostic mode"))
		tikv.UnsetResourceControlInterceptor()
		return do.initRunawayManager(nil)
	}
	if pdClient == nil {
		logutil.BgLogger().Warn("cannot setup up resource controller, not using tikv storage")
		// return nil as unistore doesn't support it
		return nil
	}

	keyspaceID := constants.NullKeyspaceID
	if codec := do.Store().GetCodec(); codec != nil {
		keyspaceID = uint32(codec.GetKeyspaceID())
	}
	control, err := rmclient.NewResourceGroupController(ctx, uniqueID, pdClient, nil, keyspaceID, newResourceGroupsControllerOptions()...)
	if err != nil {
		return err
	}
	control.Start(ctx)
	if err := do.initRunawayManager(control); err != nil {
		return err
	}
	do.SetResourceGroupsController(control)
	tikv.SetResourceControlInterceptor(control)
	return nil
}

func (do *Domain) initRunawayManager(control *rmclient.ResourceGroupsController) error {
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	serverAddr := net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))
	do.runawayManager = runaway.NewRunawayManager(control, serverAddr,
		do.sysSessionPool, do.exit, do.infoCache, do.ddl)
	return nil
}
