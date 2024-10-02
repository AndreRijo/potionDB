package components

import (
	"potionDB/crdt/crdt"
	"potionDB/crdt/proto"
	"sqlToKeyValue/src/parser"
	"sqlToKeyValue/src/sql"
	"strconv"
	"strings"
)

const (
	SQL_BUCKET_BASE = "sql_"
)

type SQLProcessor struct {
	tm       *TransactionManager
	tables   map[string]TableMetadata
	sqlPChan chan parser.ViewSQLListener
}

type TableMetadata struct {
	tableName         string
	concurrencyPolicy sql.CTPolicy
	types             map[string]proto.CRDTType
	defaults          map[string]crdt.UpdateArguments
	invariants        map[string]sql.Invariant
	columns           []string                  //Used for inserts without columnNames
	rowKeys           map[string]crdt.KeyParams //Maybe only save the key instead of KeyParams?
	secondaryIndexes  map[string][]crdt.KeyParams
}

const (
	SQLPQueueSize = 100
)

func InitializeSQLProcessor(tm *TransactionManager) (sqlP *SQLProcessor) {
	sqlP = &SQLProcessor{tm: tm, tables: make(map[string]TableMetadata), sqlPChan: make(chan parser.ViewSQLListener, SQLPQueueSize)}
	go sqlP.handleRequests()
	return
}

func (sqlP *SQLProcessor) handleRequests() {
	for {
		/*
			req := <-sqlP.sqlPChan
			switch typedReq := req.(type) {

			}
		*/
	}
}

func (sqlP *SQLProcessor) ProcessCreateTable(listener *sql.ListenerCreateTable) {
	tableMeta := TableMetadata{
		tableName:         listener.TableName,
		concurrencyPolicy: listener.ConcurrencyPolicy,
		types:             make(map[string]proto.CRDTType, len(listener.Types)),
		defaults:          make(map[string]crdt.UpdateArguments, len(listener.Defaults)),
		invariants:        listener.Invariants,
		columns:           listener.Columns,
	}
	for columnName, sqlType := range listener.Types {
		tableMeta.types[columnName] = DataTypeSQLToCRDTType(sqlType, listener.Invariants[columnName])
	}
	for columnName, stringValue := range listener.Defaults {
		tableMeta.defaults[columnName] = SqlValueToCRDTUpdate(stringValue, tableMeta.types[columnName])
	}
	sqlP.tables[listener.TableName] = tableMeta
}

func (sqlP *SQLProcessor) ProcessCreateIndex(listener *sql.ListenerCreateIndex) {

}

func (sqlP *SQLProcessor) ProcessCreateView(listener *sql.MyViewSQLListener) {

}

func (sqlP *SQLProcessor) ProcessQuery(listener *sql.ListenerQuery) {
	read := crdt.GetValuesArguments{}
	tableMeta := sqlP.tables[listener.TableName]
	if listener.IsSelectAll {
		read.Keys = tableMeta.columns
	} else {
		read.Keys = listener.ColumnNames
		if !sqlP.areColumnsValid(listener.ColumnNames, tableMeta.types) {
			//TODO: Give error
			return
		}
	}
	//TODO: Combine conditions... maybe need to check what is already done for views
	//Albeit maybe need something different... I think I need to look at what is done and come with something new
	//Something by steps... e.g., first primary key processing
}

func (sqlP *SQLProcessor) areColumnsValid(queryColumns []string, tableColumns map[string]proto.CRDTType) bool {
	if len(queryColumns) > len(tableColumns) {
		return false
	}
	for _, column := range queryColumns {
		if _, has := tableColumns[column]; !has {
			return false
		}
	}
	return true
}

func (sqlP *SQLProcessor) ProcessInsert(listener *sql.ListenerInsert) {
	tableMeta := sqlP.tables[listener.TableName]
	var columnNames []string
	if listener.ColumnNames == nil {
		columnNames = tableMeta.columns //Insert into all fields
	} else {
		columnNames = listener.ColumnNames
	}
	rowUpd := crdt.EmbMapUpdateAll{Upds: make(map[string]crdt.UpdateArguments)}
	for i, value := range listener.Values {
		columnName := columnNames[i]
		rowUpd.Upds[columnName] = SqlValueToCRDTUpdate(value, tableMeta.types[columnName])
	}
	if len(columnNames) < len(tableMeta.columns) { //Not all columns inserted, maybe one of those has a default
		for column, defaultV := range tableMeta.defaults {
			if rowUpd.Upds[column] == nil {
				rowUpd.Upds[column] = defaultV
			}
		}
	}
	//TODO: Issue an update to TM
}

func (sqlP *SQLProcessor) ProcessUpdate(listener *sql.ListenerUpdate) {

}

func (sqlP *SQLProcessor) ProcessDelete(listener *sql.ListenerDelete) {

}

func (sqlP *SQLProcessor) ProcessDropTable(listener *sql.ListenerDropTable) {

}

//Auxiliary methods for converting between SQL and CRDTs

func DataTypeSQLToCRDTType(sqlDatatype sql.SQLDatatype, invariant sql.Invariant) proto.CRDTType {
	switch sqlDatatype {
	case sql.COUNTER:
		if invariant != nil {
			_, ok := invariant.(sql.CheckConstraint)
			if ok {
				return proto.CRDTType_FATCOUNTER
			}
		}
		return proto.CRDTType_COUNTER
	case sql.INTEGER:
		return proto.CRDTType_LWWREG
	case sql.BOOLEAN:
		return proto.CRDTType_FLAG_EW
	case sql.VARCHAR:
		return proto.CRDTType_LWWREG
	case sql.DATE:
		return proto.CRDTType_LWWREG
	}
	return proto.CRDTType_LWWREG
}

func SqlValueToCRDTUpdate(sqlValue string, crdtType proto.CRDTType) crdt.UpdateArguments {
	switch crdtType {
	case proto.CRDTType_COUNTER | proto.CRDTType_FATCOUNTER:
		value, _ := strconv.Atoi(sqlValue)
		return crdt.Increment{Change: int32(value)}
	case proto.CRDTType_FLAG_EW:
		defaultBool := strings.ToLower(sqlValue)
		if defaultBool == "true" {
			return crdt.EnableFlag{}
		}
		return crdt.DisableFlag{}
	default:
		return crdt.SetValue{NewValue: sqlValue}
	}
}

/*
func SQLUpdateToCRDTUpdate(dataTypeString string, sqlValue parser.IConstantContext) crdt.UpdateArguments {
	switch dataTypeString {
	case "counter":
		value, _ := strconv.Atoi(sqlValue.INT().GetText())
		return crdt.Increment{Change: int32(value)}
	case "integer":
		return crdt.SetValue{NewValue: sqlValue.INT().GetText()}
	case "boolean":
		defaultBool := strings.ToLower(sqlValue.BOOL().GetText())
		if defaultBool == "true" {
			return crdt.EnableFlag{}
		}
		return crdt.DisableFlag{}
	case "varchar":
		allStrings := sqlValue.AllSTRING()
		fullString := allStrings[0].GetText()
		for _, str := range allStrings[1:] {
			fullString += " " + str.GetText()
		}
		return crdt.SetValue{NewValue: fullString}
	case "date":
		return crdt.SetValue{NewValue: sqlValue.DATE().GetText()}
	}
	return nil
}
*/
