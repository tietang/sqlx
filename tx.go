package sqlx

import (
    "database/sql"
    "fmt"
    "github.com/tietang/sqlx/reflectx"
    "reflect"
)

// Tx is an sqlx wrapper around sql.Tx with extra functionality
type Tx struct {
    *sql.Tx
    driverName string
    unsafe     bool
    Mapper     *reflectx.Mapper
}

// DriverName returns the driverName used by the DB which began this transaction.
func (tx *Tx) DriverName() string {
    return tx.driverName
}

// Rebind a query within a transaction's bindvar type.
func (tx *Tx) Rebind(query string) string {
    return Rebind(BindType(tx.driverName), query)
}

// Unsafe returns a version of Tx which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (tx *Tx) Unsafe() *Tx {
    return &Tx{Tx: tx.Tx, driverName: tx.driverName, unsafe: true, Mapper: tx.Mapper}
}

// BindNamed binds a query within a transaction's bindvar type.
func (tx *Tx) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
    return bindNamedMapper(BindType(tx.driverName), query, arg, tx.Mapper)
}

// NamedQuery within a transaction.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedQuery(query string, arg interface{}) (*Rows, error) {
    return NamedQuery(tx, query, arg)
}

// NamedExec a named query within a transaction.
// Any named placeholder parameters are replaced with fields from arg.
func (tx *Tx) NamedExec(query string, arg interface{}) (sql.Result, error) {
    return NamedExec(tx, query, arg)
}

// Select within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) Select(dest interface{}, query string, args ...interface{}) error {
    return Select(tx, dest, query, args...)
}

// Queryx within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) Queryx(query string, args ...interface{}) (*Rows, error) {
    r, err := tx.Tx.Query(query, args...)
    if err != nil {
        return nil, err
    }
    return &Rows{Rows: r, unsafe: tx.unsafe, Mapper: tx.Mapper}, err
}

// QueryRowx within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) QueryRowx(query string, args ...interface{}) *Row {
    rows, err := tx.Tx.Query(query, args...)
    return &Row{rows: rows, err: err, unsafe: tx.unsafe, Mapper: tx.Mapper}
}

// Get within a transaction.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (tx *Tx) Get(dest interface{}, query string, args ...interface{}) error {
    return Get(tx, dest, query, args...)
}

// MustExec runs MustExec within a transaction.
// Any placeholder parameters are replaced with supplied args.
func (tx *Tx) MustExec(query string, args ...interface{}) sql.Result {
    return MustExec(tx, query, args...)
}

// Preparex  a statement within a transaction.
func (tx *Tx) Preparex(query string) (*Stmt, error) {
    return Preparex(tx, query)
}

// Stmtx returns a version of the prepared statement which runs within a transaction.  Provided
// stmt can be either *sql.Stmt or *sqlx.Stmt.
func (tx *Tx) Stmtx(stmt interface{}) *Stmt {
    var s *sql.Stmt
    switch v := stmt.(type) {
    case Stmt:
        s = v.Stmt
    case *Stmt:
        s = v.Stmt
    case *sql.Stmt:
        s = v
    default:
        panic(fmt.Sprintf("non-statement type %v passed to Stmtx", reflect.ValueOf(stmt).Type()))
    }
    return &Stmt{Stmt: tx.Stmt(s), Mapper: tx.Mapper}
}

// NamedStmt returns a version of the prepared statement which runs within a transaction.
func (tx *Tx) NamedStmt(stmt *NamedStmt) *NamedStmt {
    return &NamedStmt{
        QueryString: stmt.QueryString,
        Params:      stmt.Params,
        Stmt:        tx.Stmtx(stmt.Stmt),
    }
}

// PrepareNamed returns an sqlx.NamedStmt
func (tx *Tx) PrepareNamed(query string) (*NamedStmt, error) {
    return prepareNamed(tx, query)
}
