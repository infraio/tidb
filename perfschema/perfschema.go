// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package perfschema

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/table"
)

// StatementInstrument defines the methods for statement instrumentation points
type StatementInstrument interface {
	RegisterStatement(category, name string, elem interface{})

	StartStatement(sql string, connID uint64, callerName EnumCallerName, elem interface{}) *StatementState

	EndStatement(state *StatementState)
}

// PerfSchema defines the methods to be invoked by the executor
type PerfSchema interface {

	// For statement instrumentation only.
	StatementInstrument

	// GetDBMeta returns db info for PerformanceSchema.
	GetDBMeta() *model.DBInfo
	// GetTable returns table instance for name.
	GetTable(name string) (table.Table, bool)
}

type perfSchema struct {
	store   kv.Storage
	dbInfo  *model.DBInfo
	tables  map[string]*model.TableInfo
	mTables map[string]table.Table // MemoryTables for perfSchema

	// Used for TableStmtsHistory
	historyHandles []int64
	historyCursor  int
}

var _ PerfSchema = (*perfSchema)(nil)

// PerfHandle is the only access point for the in-memory performance schema information
var (
	PerfHandle PerfSchema
)

// NewPerfHandle creates a new perfSchema on store.
func NewPerfHandle(store kv.Storage) PerfSchema {
	schema := PerfHandle.(*perfSchema)
	schema.store = store
	schema.historyHandles = make([]int64, 0, stmtsHistoryElemMax)
	_ = schema.initialize()
	// Existing instrument names are the same as MySQL 5.7
	PerfHandle.RegisterStatement("sql", "alter_table", (*ast.AlterTableStmt)(nil))
	PerfHandle.RegisterStatement("sql", "begin", (*ast.BeginStmt)(nil))
	PerfHandle.RegisterStatement("sql", "commit", (*ast.CommitStmt)(nil))
	PerfHandle.RegisterStatement("sql", "create_db", (*ast.CreateDatabaseStmt)(nil))
	PerfHandle.RegisterStatement("sql", "create_index", (*ast.CreateIndexStmt)(nil))
	PerfHandle.RegisterStatement("sql", "create_table", (*ast.CreateTableStmt)(nil))
	PerfHandle.RegisterStatement("sql", "deallocate", (*ast.DeallocateStmt)(nil))
	PerfHandle.RegisterStatement("sql", "delete", (*ast.DeleteStmt)(nil))
	PerfHandle.RegisterStatement("sql", "do", (*ast.DoStmt)(nil))
	PerfHandle.RegisterStatement("sql", "drop_db", (*ast.DropDatabaseStmt)(nil))
	PerfHandle.RegisterStatement("sql", "drop_table", (*ast.DropTableStmt)(nil))
	PerfHandle.RegisterStatement("sql", "drop_index", (*ast.DropIndexStmt)(nil))
	PerfHandle.RegisterStatement("sql", "execute", (*ast.ExecuteStmt)(nil))
	PerfHandle.RegisterStatement("sql", "explain", (*ast.ExplainStmt)(nil))
	PerfHandle.RegisterStatement("sql", "insert", (*ast.InsertStmt)(nil))
	PerfHandle.RegisterStatement("sql", "prepare", (*ast.PrepareStmt)(nil))
	PerfHandle.RegisterStatement("sql", "rollback", (*ast.RollbackStmt)(nil))
	PerfHandle.RegisterStatement("sql", "select", (*ast.SelectStmt)(nil))
	PerfHandle.RegisterStatement("sql", "set", (*ast.SetStmt)(nil))
	PerfHandle.RegisterStatement("sql", "show", (*ast.ShowStmt)(nil))
	PerfHandle.RegisterStatement("sql", "truncate", (*ast.TruncateTableStmt)(nil))
	PerfHandle.RegisterStatement("sql", "union", (*ast.UnionStmt)(nil))
	PerfHandle.RegisterStatement("sql", "update", (*ast.UpdateStmt)(nil))
	PerfHandle.RegisterStatement("sql", "use", (*ast.UseStmt)(nil))
	return PerfHandle
}

func init() {
	schema := &perfSchema{}
	PerfHandle = schema
}
