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
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
)

// statementInfo defines statement instrument information.
type statementInfo struct {
	// The registered statement key
	key uint64
	// The name of the statement instrument to register
	name string
}

// StatementState provides temporary storage to a statement runtime statistics.
// TODO:
// 1. support statement digest.
// 2. support prepared statement.
type StatementState struct {
	// Connection identifier
	connID uint64
	// Statement information
	info *statementInfo
	// Statement type
	stmtType reflect.Type
	// Source file and line number
	source string
	// Timer name
	timerName enumTimerName
	// Timer start
	timerStart int64
	// Timer end
	timerEnd int64
	// Locked time
	lockTime int64
	// SQL statement string
	sqlText string
	// Current schema name
	schemaName string
	// Number of errors
	errNum uint32
	// Number of warnings
	warnNum uint32
	// Rows affected
	rowsAffected uint64
	// Rows sent
	rowsSent uint64
	// Rows examined
	rowsExamined uint64
	// Metric, temporary tables created on disk
	createdTmpDiskTables uint32
	// Metric, temproray tables created
	createdTmpTables uint32
	// Metric, number of select full join
	selectFullJoin uint32
	// Metric, number of select full range join
	selectFullRangeJoin uint32
	// Metric, number of select range
	selectRange uint32
	// Metric, number of select range check
	selectRangeCheck uint32
	// Metric, number of select scan
	selectScan uint32
	// Metric, number of sort merge passes
	sortMergePasses uint32
	// Metric, number of sort merge
	sortRange uint32
	// Metric, number of sort rows
	sortRows uint32
	// Metric, number of sort scans
	sortScan uint32
	// Metric, no index used flag
	noIndexUsed uint8
	// Metric, no good index used flag
	noGoodIndexUsed uint8
}

const (
	// Maximum allowed number of elements in table events_statements_history.
	// TODO: make it configurable?
	stmtsHistoryElemMax int = 1024
)

var (
	stmtInfos = make(map[reflect.Type]*statementInfo)
)

func (ps *perfSchema) RegisterStatement(category, name string, elem interface{}) {
	instrumentName := fmt.Sprintf("%s%s/%s", statementInstrumentPrefix, category, name)
	key, err := ps.addInstrument(instrumentName)
	if err != nil {
		// just ignore, do nothing else.
		log.Errorf("Unable to register instrument %s", instrumentName)
		return
	}

	stmtInfos[reflect.TypeOf(elem)] = &statementInfo{
		key:  key,
		name: instrumentName,
	}
}

func (ps *perfSchema) StartStatement(sql string, connID uint64, callerName EnumCallerName, elem interface{}) *StatementState {
	stmtType := reflect.TypeOf(elem)
	info, ok := stmtInfos[stmtType]
	if !ok {
		// just ignore, do nothing else.
		log.Errorf("No instrument registered for statement %s", stmtType)
		return nil
	}

	// check and apply the configuration parameter in table setup_timers.
	timerName, err := ps.getTimerName(flagStatement)
	if err != nil {
		// just ignore, do nothing else.
		log.Error("Unable to check setup_timers table")
		return nil
	}
	var timerStart int64
	switch timerName {
	case timerNameNanosec:
		timerStart = time.Now().UnixNano()
	case timerNameMicrosec:
		timerStart = time.Now().UnixNano() / int64(time.Microsecond)
	case timerNameMillisec:
		timerStart = time.Now().UnixNano() / int64(time.Millisecond)
	default:
		return nil
	}

	// TODO: check and apply the additional configuration parameters in:
	// - table setup_actors
	// - table setup_setup_consumers
	// - table setup_instruments
	// - table setup_objects

	var source string
	source, ok = callerNames[callerName]
	if !ok {
		_, fileName, fileLine, ok := runtime.Caller(1)
		if !ok {
			// just ignore, do nothing else.
			log.Error("Unable to get runtime.Caller(1)")
			return nil
		}
		source = fmt.Sprintf("%s:%d", fileName, fileLine)
		callerNames[callerName] = source
	}

	return &StatementState{
		connID:     connID,
		info:       info,
		stmtType:   stmtType,
		source:     source,
		timerName:  timerName,
		timerStart: timerStart,
		sqlText:    sql,
	}
}

func (ps *perfSchema) EndStatement(state *StatementState) {
	if state == nil {
		return
	}

	switch state.timerName {
	case timerNameNanosec:
		state.timerEnd = time.Now().UnixNano()
	case timerNameMicrosec:
		state.timerEnd = time.Now().UnixNano() / int64(time.Microsecond)
	case timerNameMillisec:
		state.timerEnd = time.Now().UnixNano() / int64(time.Millisecond)
	default:
		return
	}

	log.Debugf("EndStatement: sql %s, connection id %d, type %s", state.sqlText, state.connID, state.stmtType)

	record := state2Record(state)
	err := ps.updateEventsStmtsCurrent(state.connID, record)
	if err != nil {
		log.Error("Unable to update events_statements_current table")
	}
	err = ps.appendEventsStmtsHistory(record)
	if err != nil {
		log.Error("Unable to append to events_statements_history table")
	}
}

func state2Record(state *StatementState) []interface{} {
	return []interface{}{
		state.connID,             // THREAD_ID
		state.info.key,           // EVENT_ID
		nil,                      // END_EVENT_ID
		state.info.name,          // EVENT_NAME
		state.source,             // SOURCE
		uint64(state.timerStart), // TIMER_START
		uint64(state.timerEnd),   // TIMER_END
		nil, // TIMER_WAIT
		uint64(state.lockTime),             // LOCK_TIME
		state.sqlText,                      // SQL_TEXT
		nil,                                // DIGEST
		nil,                                // DIGEST_TEXT
		state.schemaName,                   // CURRENT_SCHEMA
		nil,                                // OBJECT_TYPE
		nil,                                // OBJECT_SCHEMA
		nil,                                // OBJECT_NAME
		nil,                                // OBJECT_INSTANCE_BEGIN
		nil,                                // MYSQL_ERRNO,
		nil,                                // RETURNED_SQLSTATE
		nil,                                // MESSAGE_TEXT
		uint64(state.errNum),               // ERRORS
		uint64(state.warnNum),              // WARNINGS
		state.rowsAffected,                 // ROWS_AFFECTED
		state.rowsSent,                     // ROWS_SENT
		state.rowsExamined,                 // ROWS_EXAMINED
		uint64(state.createdTmpDiskTables), // CREATED_TMP_DISK_TABLES
		uint64(state.createdTmpTables),     // CREATED_TMP_TABLES
		uint64(state.selectFullJoin),       // SELECT_FULL_JOIN
		uint64(state.selectFullRangeJoin),  // SELECT_FULL_RANGE_JOIN
		uint64(state.selectRange),          // SELECT_RANGE
		uint64(state.selectRangeCheck),     // SELECT_RANGE_CHECK
		uint64(state.selectScan),           // SELECT_SCAN
		uint64(state.sortMergePasses),      // SORT_MERGE_PASSES
		uint64(state.sortRange),            // SORT_RANGE
		uint64(state.sortRows),             // SORT_ROWS
		uint64(state.sortScan),             // SORT_SCAN
		uint64(state.noIndexUsed),          // NO_INDEX_USED
		uint64(state.noGoodIndexUsed),      // NO_GOOD_INDEX_USED
		nil, // NESTING_EVENT_ID
		nil, // NESTING_EVENT_TYPE
		nil, // NESTING_EVENT_LEVEL
	}
}

func (ps *perfSchema) updateEventsStmtsCurrent(connID uint64, record []interface{}) error {
	// Try AddRecord
	tbl := ps.mTables[TableStmtsCurrent]
	_, err := tbl.AddRecord(nil, record)
	if err == nil {
		return nil
	}
	if terror.ErrorNotEqual(err, kv.ErrKeyExists) {
		return errors.Trace(err)
	}
	// Update it
	handle := int64(connID)
	err = tbl.UpdateRecord(nil, handle, nil, record, nil)
	return errors.Trace(err)
}

func (ps *perfSchema) appendEventsStmtsHistory(record []interface{}) error {
	tbl := ps.mTables[TableStmtsHistory]
	if len(ps.historyHandles) < stmtsHistoryElemMax {
		h, err := tbl.AddRecord(nil, record)
		if err != nil {
			return errors.Trace(err)
		}
		ps.historyHandles = append(ps.historyHandles, h)
		return nil
	}
	// If histroy is full, replace old data
	if ps.historyCursor >= len(ps.historyHandles) {
		ps.historyCursor = 0
	}
	h := ps.historyHandles[ps.historyCursor]
	ps.historyCursor++
	err := tbl.UpdateRecord(nil, h, nil, record, nil)
	return errors.Trace(err)
}
