package sqlx

import (
    "database/sql"
    "fmt"
    "github.com/tietang/sqlx/reflectx"
    "strings"
)

// DB is a wrapper around sql.DB which keeps track of the driverName upon Open,
// used mostly to automatically bind named queries using the right bindvars.
type DB struct {
    *sql.DB
    driverName string
    unsafe     bool
    Mapper     *reflectx.Mapper
}

// NewDb returns a new sqlx DB wrapper for a pre-existing *sql.DB.  The
// driverName of the original database is required for named query support.
func NewDb(db *sql.DB, driverName string) *DB {
    return &DB{DB: db, driverName: driverName, Mapper: mapper()}
}

// DriverName returns the driverName passed to the Open function for this DB.
func (db *DB) DriverName() string {
    return db.driverName
}

// Open is the same as sql.Open, but returns an *sqlx.DB instead.
func Open(driverName, dataSourceName string) (*DB, error) {
    db, err := sql.Open(driverName, dataSourceName)
    if err != nil {
        return nil, err
    }
    return &DB{DB: db, driverName: driverName, Mapper: mapper()}, err
}

// MustOpen is the same as sql.Open, but returns an *sqlx.DB instead and panics on error.
func MustOpen(driverName, dataSourceName string) *DB {
    db, err := Open(driverName, dataSourceName)
    if err != nil {
        panic(err)
    }
    return db
}

// MapperFunc sets a new mapper for this db using the default sqlx struct tag
// and the provided mapper function.
func (db *DB) MapperFunc(mf func(string) string) {
    db.Mapper = reflectx.NewMapperFunc("db", mf)
}

// Rebind transforms a query from QUESTION to the DB driver's bindvar type.
func (db *DB) Rebind(query string) string {
    return Rebind(BindType(db.driverName), query)
}

// Unsafe returns a version of DB which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
// sqlx.Stmt and sqlx.Tx which are created from this DB will inherit its
// safety behavior.
func (db *DB) Unsafe() *DB {
    return &DB{DB: db.DB, driverName: db.driverName, unsafe: true, Mapper: db.Mapper}
}

// BindNamed binds a query using the DB driver's bindvar type.
func (db *DB) BindNamed(query string, arg interface{}) (string, []interface{}, error) {
    return bindNamedMapper(BindType(db.driverName), query, arg, db.Mapper)
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQuery(query string, arg interface{}) (*Rows, error) {
    return NamedQuery(db, query, arg)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExec(query string, arg interface{}) (sql.Result, error) {
    return NamedExec(db, query, arg)
}

// Select using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {
    return Select(db, dest, query, args...)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) Get(dest interface{}, query string, args ...interface{}) error {
    return Get(db, dest, query, args...)
}

// MustBegin starts a transaction, and panics on error.  Returns an *sqlx.Tx instead
// of an *sql.Tx.
func (db *DB) MustBegin() *Tx {
    tx, err := db.Beginx()
    if err != nil {
        panic(err)
    }
    return tx
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Beginx() (*Tx, error) {
    tx, err := db.DB.Begin()
    if err != nil {
        return nil, err
    }
    return &Tx{Tx: tx, driverName: db.driverName, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// Queryx queries the database and returns an *sqlx.Rows.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Queryx(query string, args ...interface{}) (*Rows, error) {
    r, err := db.DB.Query(query, args...)
    if err != nil {
        return nil, err
    }
    return &Rows{Rows: r, unsafe: db.unsafe, Mapper: db.Mapper}, err
}

// QueryRowx queries the database and returns an *sqlx.Row.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) QueryRowx(query string, args ...interface{}) *Row {
    rows, err := db.DB.Query(query, args...)
    return &Row{rows: rows, err: err, unsafe: db.unsafe, Mapper: db.Mapper}
}

// MustExec (panic) runs MustExec using this database.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) MustExec(query string, args ...interface{}) sql.Result {
    return MustExec(db, query, args...)
}

// Preparex returns an sqlx.Stmt instead of a sql.Stmt
func (db *DB) Preparex(query string) (*Stmt, error) {
    return Preparex(db, query)
}

// PrepareNamed returns an sqlx.NamedStmt
func (db *DB) PrepareNamed(query string) (*NamedStmt, error) {
    return prepareNamed(db, query)
}

func (db *DB) Insert(dest interface{}) (sql.Result, error) {
    name, columnNames, columnValues, err := Map(dest, nil)
    if err != nil {
        return nil, err
    }
    return db.insertTable(name, columnNames, columnValues)
}

func (db *DB) InsertTable(tableName string, dest interface{}) (sql.Result, error) {
    _, columnNames, columnValues, err := Map(dest, nil)
    if err != nil {
        return nil, err
    }
    return db.insertTable(tableName, columnNames, columnValues)
}

func (db *DB) insertTable(tableName string, columnNames []string, columnValues []interface{}) (sql.Result, error) {
    names := strings.Join(columnNames, ",")
    placeholders := strings.Repeat("?,", len(columnNames))
    placeholders = placeholders[:len(placeholders)-1]
    query := fmt.Sprintf("insert into %s(%s) values(%s)", tableName, names, placeholders)
    fmt.Println(query)
    return MustExec(db, query, columnValues...), nil
}
