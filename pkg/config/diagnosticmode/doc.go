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

// Package diagnosticmode stores whether the TiDB process runs in diagnostic
// mode.
//
// Diagnostic mode is initialized from a command-line flag during startup and
// cannot be changed afterward. Code that needs to select diagnostic behavior
// should call Enabled.
//
// Schema synchronization in this mode polls existing metadata without publishing
// server information, topology or schema versions. DDL.Start does not initialize
// DDL execution resources. min-start-ts reporting is disabled, so long-running
// reads lose the GC protection supplied by that report. The independent server
// ID lease and normal timestamp acquisition remain enabled.
//
// This component support does not yet reject all SQL submissions or isolate the
// separate DXF and bootstrap/upgrade paths. It is not a general read-only mode.
package diagnosticmode
