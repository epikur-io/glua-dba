package dba

import (
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jimsmart/schema"
	_ "github.com/lib/pq"
	lua "github.com/yuin/gopher-lua"
	dbx "gopkg.in/go-ozzo/ozzo-dbx.v1"
)

const dbaVersion string = "1.0.7"

const luaDBATypeName string = "dba"
const luaTxTypeName string = "dba_tx"
const luaQueryTypeName = "dba_query"

var dbaSupportedDrivers []string = []string{"mysql", "postgres", "mssql"}

var api = map[string]lua.LGFunction{
	"new":             dba_New,
	"newFromExisting": dba_NewFromExisting,
}

func Preload(L *lua.LState) {
	L.PreloadModule("dba", Loader)
}

// Loader is the module loader function.
func Loader(L *lua.LState) int {
	t := L.NewTable()

	dmt := L.NewTypeMetatable(luaDBATypeName)
	//L.SetGlobal(luaDBATypeName, dmt)
	L.SetField(dmt, "new", L.NewFunction(dba_New))
	L.SetField(dmt, "newFromExisting", L.NewFunction(dba_NewFromExisting))
	L.SetField(dmt, "__index", L.SetFuncs(L.NewTable(), dbaMethods))

	qmt := L.NewTypeMetatable(luaQueryTypeName)
	//L.SetGlobal(luaQueryTypeName, qmt)
	L.SetField(qmt, "new", L.NewFunction(query_New))
	L.SetField(qmt, "__index", L.SetFuncs(L.NewTable(), dbaQueryMethods))

	tmt := L.NewTypeMetatable(luaTxTypeName)
	L.SetField(tmt, "new", L.NewFunction(query_New))
	L.SetField(tmt, "__index", L.SetFuncs(L.NewTable(), dbaTxMethods))

	L.SetField(t, luaDBATypeName, dmt)
	L.SetField(t, luaQueryTypeName, qmt)
	L.SetField(t, luaTxTypeName, tmt)

	t.RawSetH(lua.LString("version"), lua.LString(dbaVersion))
	L.SetFuncs(t, api)
	L.Push(t)
	
	return 1
}


// Registers my person type to given L.
func RegisterDBA(L *lua.LState) {
	// !TODO: add transaction functionality to lua
	dmt := L.NewTypeMetatable(luaDBATypeName)
	L.SetGlobal(luaDBATypeName, dmt)
	L.SetField(dmt, "new", L.NewFunction(dba_New))
	L.SetField(dmt, "__index", L.SetFuncs(L.NewTable(), dbaMethods))

	qmt := L.NewTypeMetatable(luaQueryTypeName)
	L.SetGlobal(luaQueryTypeName, qmt)
	L.SetField(qmt, "new", L.NewFunction(query_New))
	L.SetField(qmt, "__index", L.SetFuncs(L.NewTable(), dbaQueryMethods))

	tmt := L.NewTypeMetatable(luaTxTypeName)
	L.SetGlobal(luaTxTypeName, tmt)
	L.SetField(tmt, "new", L.NewFunction(query_New))
	L.SetField(tmt, "__index", L.SetFuncs(L.NewTable(), dbaTxMethods))
}

var dbaMethods = map[string]lua.LGFunction{
	"connect":    dba_Connect,
	"query":      dba_Query,
	"insert":     dba_Insert,
	"tableMeta":  dba_TableMeta,
	"tableNames": dba_TableNames,
	"viewNames":  dba_ViewNames,
	"viewMeta":   dba_ViewMeta,
	//"update":  dba_Update,
	"begin": dba_Begin,
	//"escape": dba_Escape,
	"close": dba_Close,
}

var dbaTxMethods = map[string]lua.LGFunction{
	"query": tx_Query,
	//"insert":   tx_Insert,
	//"bind":     tx_Bind,
	"commit":   tx_Commit,
	"rollback": tx_Rollback,
}

var dbaQueryMethods = map[string]lua.LGFunction{
	"run":     query_Run,
	"execute": query_Exec,
	"bind":    query_Bind,
	"sql":     query_Sql,
}

type DBA struct {
	Type         string
	DSN          string
	DBX          *dbx.DB
	SilentErrors bool
}

type Tx struct {
	DBA *DBA
	tx  *dbx.Tx
}

type Query struct {
	DBA *DBA
	Qry *dbx.Query
}

func checkDBA(L *lua.LState) *DBA {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*DBA); ok {
		return v
	}
	L.ArgError(1, "[DBA] object expected")
	return nil
}

func checkTx(L *lua.LState) *Tx {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*Tx); ok {
		return v
	}
	L.ArgError(1, "[DBA] ransaction object expected")
	return nil
}

func checkQuery(L *lua.LState) *Query {
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*Query); ok {
		return v
	}
	L.ArgError(1, "[DBA] Query object expected")
	return nil
}

func dba_New(L *lua.LState) int {
	dba := DBA{}
	ud := L.NewUserData()
	ud.Value = &dba
	L.SetMetatable(ud, L.GetTypeMetatable(luaDBATypeName))
	L.Push(ud)
	return 1
}

func dba_NewFromExisting(L *lua.LState) int {
	dbType := L.CheckString(1)
	rdb := L.CheckUserData(2)
	pdb, ok := rdb.Value.(*sql.DB)
	if !ok {
		L.ArgError(2, "[DBA] Invalid database object!")
		return 0
	}
	dba := DBA{}
	dba.DBX = dbx.NewFromDB(pdb, dbType)
	dba.Type = dbType
	dba.DSN = ""
	ud := L.NewUserData()
	ud.Value = &dba
	L.SetMetatable(ud, L.GetTypeMetatable(luaDBATypeName))
	L.Push(ud)
	return 1
}

func query_New(L *lua.LState) int {
	q := Query{}
	ud := L.NewUserData()
	ud.Value = &q
	L.SetMetatable(ud, L.GetTypeMetatable(luaDBATypeName))
	L.Push(ud)
	return 1
}

func dba_Connect(L *lua.LState) int {
	dba := checkDBA(L)
	dtype := L.CheckString(2)
	dsn := L.CheckString(3)
	var err error
	dba.DBX, err = dbx.Open(dtype, dsn)
	if err != nil {
		L.Push(lua.LString(fmt.Sprint("[DBA] Could not connect to database (type: ", dtype, ") using DSN: ", dsn)))
		return 1
	}
	err = dba.DBX.DB().Ping()
	if err != nil {
		L.Push(lua.LString(fmt.Sprint("[DBA] Could not connect to database (type: ", dtype, ") using DSN: ", dsn, "; Ping failed: ", err.Error())))
		return 1
	}
	L.Push(lua.LBool(true))
	dba.Type = dtype
	dba.DSN = dsn
	return 1
}

func dba_TableNames(L *lua.LState) int {
	dba := checkDBA(L)
	tbls, err := schema.TableNames(dba.DBX.DB())
	lTbl := L.NewTable()
	if err != nil {
		L.Push(lTbl)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	for _, t := range tbls {
		lTbl.Append(lua.LString(t))
	}
	L.Push(lTbl)
	L.Push(lua.LNil)
	return 2
}

func dba_ViewNames(L *lua.LState) int {
	dba := checkDBA(L)
	tbls, err := schema.ViewNames(dba.DBX.DB())
	lTbl := L.NewTable()
	if err != nil {
		L.Push(lTbl)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	for _, t := range tbls {
		lTbl.Append(lua.LString(t))
	}
	L.Push(lTbl)
	L.Push(lua.LNil)
	return 2
}

func dba_TableMeta(L *lua.LState) int {
	dba := checkDBA(L)
	tname := L.CheckString(2)
	tbls, err := schema.Table(dba.DBX.DB(), tname)
	lTbl := L.NewTable()
	if err != nil {
		L.Push(lTbl)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	for _, t := range tbls {
		tx := L.NewTable()
		tx.RawSetString("name", lua.LString(t.Name()))
		tx.RawSetString("db_type", lua.LString(t.DatabaseTypeName()))
		perc, scale, dsOk := t.DecimalSize()
		txDs := L.NewTable()
		txDs.RawSetString("percision", lua.LNumber(perc))
		txDs.RawSetString("scale", lua.LNumber(scale))
		txDs.RawSetString("ok", lua.LBool(dsOk))
		tx.RawSetString("decimal_size", txDs)
		dLen, dOk := t.Length()
		txLn := L.NewTable()
		txLn.RawSetString("value", lua.LNumber(dLen))
		txLn.RawSetString("ok", lua.LBool(dOk))
		tx.RawSetString("length", txLn)
		dNul, dnOk := t.Nullable()
		txNl := L.NewTable()
		txNl.RawSetString("value", lua.LBool(dNul))
		txNl.RawSetString("ok", lua.LBool(dnOk))
		tx.RawSetString("nullable", txNl)
		lTbl.Append(tx)
	}
	L.Push(lTbl)
	L.Push(lua.LNil)
	return 2
}

func dba_ViewMeta(L *lua.LState) int {
	dba := checkDBA(L)
	tname := L.CheckString(2)
	tbls, err := schema.View(dba.DBX.DB(), tname)
	lTbl := L.NewTable()
	if err != nil {
		L.Push(lTbl)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	for _, t := range tbls {
		tx := L.NewTable()
		tx.RawSetString("name", lua.LString(t.Name()))
		tx.RawSetString("db_type", lua.LString(t.DatabaseTypeName()))
		perc, scale, dsOk := t.DecimalSize()
		txDs := L.NewTable()
		txDs.RawSetString("percision", lua.LNumber(perc))
		txDs.RawSetString("scale", lua.LNumber(scale))
		txDs.RawSetString("ok", lua.LBool(dsOk))
		tx.RawSetString("decimal_size", txDs)
		dLen, dOk := t.Length()
		txLn := L.NewTable()
		txLn.RawSetString("value", lua.LNumber(dLen))
		txLn.RawSetString("ok", lua.LBool(dOk))
		tx.RawSetString("length", txLn)
		dNul, dnOk := t.Nullable()
		txNl := L.NewTable()
		txNl.RawSetString("value", lua.LBool(dNul))
		txNl.RawSetString("ok", lua.LBool(dnOk))
		tx.RawSetString("nullable", txNl)
		lTbl.Append(tx)
	}
	L.Push(lTbl)
	L.Push(lua.LNil)
	return 2
}

func dba_Close(L *lua.LState) int {
	dba := checkDBA(L)
	if dba.DBX == nil {
		L.RaiseError(fmt.Sprint("[DBA] No database was opened. Closing database failed."))
		return 0
	}
	err := dba.DBX.Close()
	if err != nil {
		L.RaiseError(err.Error())
		return 0
	}
	return 0
}

func dba_Insert(L *lua.LState) int {
	dba := checkDBA(L)
	table := L.CheckString(2)
	data := L.CheckTable(3)
	params := dbx.Params{}
	data.ForEach(func(k lua.LValue, v lua.LValue) {
		if k.Type().String() == "string" {
			params[k.String()] = v.String()
		}
	})
	stmt := dba.DBX.Insert(table, params)
	_, err := stmt.Execute()
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	L.Push(lua.LNil)
	return 1
}

func dba_Update(L *lua.LState) int {
	// !TODO
	return 0
}

func dba_Query(L *lua.LState) int {
	dba := checkDBA(L)
	sql := L.CheckString(2)
	if dba.DBX == nil {
		L.RaiseError(fmt.Sprint("[DBA] No DBA object available!"))
		return 1
	}
	qry := Query{}
	qry.DBA = dba
	qry.Qry = dba.DBX.NewQuery(sql)
	ud := L.NewUserData()
	ud.Value = &qry
	L.SetMetatable(ud, L.GetTypeMetatable(luaQueryTypeName))
	L.Push(ud)
	return 1
}

func query_Bind(L *lua.LState) int {
	qry := checkQuery(L)
	params := L.CheckTable(2)
	data := dbx.Params{}
	params.ForEach(func(key lua.LValue, val lua.LValue) {
		k := key.String()
		d := val.String()
		if k != "" {
			data[k] = d
		}
	})
	qry.Qry.Bind(data)
	return 0
}

func query_Run(L *lua.LState) int {
	qry := checkQuery(L)
	lres := L.CreateTable(0, 0)

	rows, err := qry.Qry.Rows()
	if err != nil {
		L.Push(lres)
		L.Push(lua.LString(err.Error()))
		return 2
	}

	trow := dbx.NullStringMap{}
	for rows.Next() {
		rows.Rows.ColumnTypes()
		if err := rows.ScanMap(trow); err != nil {
			continue
		}
		lres.Append(toLuaType(L, trow))
	}
	L.Push(lres)
	L.Push(lua.LNil)
	return 2

}

func query_Sql(L *lua.LState) int {
	qry := checkQuery(L)
	sql := qry.Qry.SQL()
	L.Push(lua.LString(sql))
	return 1
}

func query_Exec(L *lua.LState) int {
	qry := checkQuery(L)
	res, err := qry.Qry.Execute()
	if err != nil {
		L.Push(L.NewTable())
		L.Push(lua.LString(err.Error()))
		return 2
	}
	affected, aerr := res.RowsAffected()
	lastID, lerr := res.LastInsertId()
	if lerr != nil {
		lastID = -1
	}
	if aerr != nil {
		affected = -1
	}
	rt := L.NewTable()
	rt.RawSetH(lua.LString("lastInserted"), lua.LNumber(float64(lastID)))
	rt.RawSetH(lua.LString("affected"), lua.LNumber(float64(affected)))
	L.Push(rt)
	L.Push(lua.LNil)
	return 2
}

func dba_Begin(L *lua.LState) int {
	dba := checkDBA(L)
	tx := Tx{}
	tx.DBA = dba
	trx, err := dba.DBX.Begin()
	if err != nil {
		L.Push(lua.LNil)
		L.Push(lua.LString(err.Error()))
		return 2
	}
	tx.tx = trx
	ud := L.NewUserData()
	ud.Value = tx
	L.SetMetatable(ud, L.GetTypeMetatable(luaTxTypeName))
	L.Push(ud)
	L.Push(lua.LNil)
	return 2
}

func tx_Commit(L *lua.LState) int {
	tx := checkTx(L)
	err := tx.tx.Commit()
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	L.Push(lua.LNil)
	return 1
}

func tx_Rollback(L *lua.LState) int {
	tx := checkTx(L)
	err := tx.tx.Rollback()
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	L.Push(lua.LNil)
	return 1
}

func tx_Query(L *lua.LState) int {
	tx := checkTx(L)
	sql := L.CheckString(2)
	qry := Query{}
	qry.DBA = tx.DBA
	qry.Qry = tx.tx.NewQuery(sql)
	ud := L.NewUserData()
	ud.Value = &qry
	L.SetMetatable(ud, L.GetTypeMetatable(luaQueryTypeName))
	L.Push(ud)
	return 1
}

// Helper / Utils

func toLuaType(L *lua.LState, value interface{}) lua.LValue {
	switch converted := value.(type) {
	case string:
		return lua.LString(converted)
	case bool:
		return lua.LBool(converted)
	case float64:
	case int:
		return lua.LNumber(converted)
	case sql.NullString:
		if converted.Valid {
			val, err := converted.Value()
			if err == nil {
				cval := toLuaType(L, val)
				return cval
			}
		}
		return lua.LString("")
	case dbx.NullStringMap:
		tbl := L.CreateTable(0, len(converted))
		for key, row := range converted {
			tbl.RawSetH(lua.LString(key), toLuaType(L, row))
		}
		return tbl
	case []dbx.NullStringMap:
		tbl := L.CreateTable(0, len(converted))
		for _, row := range converted {
			tbl.Append(toLuaType(L, row))
		}
		return tbl
	case []interface{}:
		arr := L.CreateTable(len(converted), 0)
		for _, item := range converted {
			arr.Append(toLuaType(L, item))
		}
		return arr
	case map[string]interface{}:
		tbl := L.CreateTable(0, len(converted))
		for key, item := range converted {
			//if s, ok := key; ok {
			tbl.RawSetH(lua.LString(key), toLuaType(L, item))
			//}
		}
		return tbl
	case map[interface{}]interface{}:
		tbl := L.CreateTable(0, len(converted))
		for key, item := range converted {
			if s, ok := key.(string); ok {
				tbl.RawSetH(lua.LString(s), toLuaType(L, item))
			}
		}
		return tbl

	case []map[string]interface{}:
		tbl := L.CreateTable(0, len(converted))
		for _, row := range converted {
			tbl.Append(toLuaType(L, row))
		}
		return tbl
	}
	return lua.LNil
}
