package sqlx

import (
    "database/sql"
    "database/sql/driver"
    "github.com/tietang/sqlx/reflectx"

    "reflect"
    "strings"
    "sync"
)

// Although the NameMapper is convenient, in practice it should not
// be relied on except for application code.  If you are writing a library
// that uses sqlx, you should be aware that the name mappings you expect
// can be overridden by your user's application.

// NameMapper is used to map column names to struct field names.  By default,
// it uses strings.ToLower to lowercase struct field names.  It can be set
// to whatever you want, but it is encouraged to be set before sqlx is used
// as name-to-field mappings are cached after first use on a type.
var NameMapper = strings.ToLower
var origMapper = reflect.ValueOf(NameMapper)

// Rather than creating on init, this is created when necessary so that
// importers have time to customize the NameMapper.
var mpr *reflectx.Mapper

// mprMu protects mpr.
var mprMu sync.Mutex

// mapper returns a valid mapper using the configured NameMapper func.
func mapper() *reflectx.Mapper {
    mprMu.Lock()
    defer mprMu.Unlock()

    if mpr == nil {
        mpr = reflectx.NewMapperFunc("db", NameMapper)
    } else if origMapper != reflect.ValueOf(NameMapper) {
        // if NameMapper has changed, create a new mapper
        mpr = reflectx.NewMapperFunc("db", NameMapper)
        origMapper = reflect.ValueOf(NameMapper)
    }
    return mpr
}

// isScannable takes the reflect.Type and the actual dest value and returns
// whether or not it's Scannable.  Something is scannable if:
//   * it is not a struct
//   * it implements sql.Scanner
//   * it has no exported fields
func isScannable(t reflect.Type) bool {
    if reflect.PtrTo(t).Implements(_scannerInterface) {
        return true
    }
    if t.Kind() != reflect.Struct {
        return true
    }

    // it's not important that we use the right mapper for this particular object,
    // we're only concerned on how many exported fields this struct has
    m := mapper()
    if len(m.TypeMap(t).Index) == 0 {
        return true
    }
    return false
}

// ColScanner is an interface used by MapScan and SliceScan
type ColScanner interface {
    Columns() ([]string, error)
    Scan(dest ...interface{}) error
    Err() error
}

// Queryer is an interface used by Get and Select
type Queryer interface {
    Query(query string, args ...interface{}) (*sql.Rows, error)
    Queryx(query string, args ...interface{}) (*Rows, error)
    QueryRowx(query string, args ...interface{}) *Row
}

// Execer is an interface used by MustExec and LoadFile
type Execer interface {
    Exec(query string, args ...interface{}) (sql.Result, error)
}

// Binder is an interface for something which can bind queries (Tx, DB)
type binder interface {
    DriverName() string
    Rebind(string) string
    BindNamed(string, interface{}) (string, []interface{}, error)
}

// Ext is a union interface which can bind, query, and exec, used by
// NamedQuery and NamedExec.
type Ext interface {
    binder
    Queryer
    Execer
}

// Preparer is an interface used by Preparex.
type Preparer interface {
    Prepare(query string) (*sql.Stmt, error)
}

// determine if any of our extensions are unsafe
func isUnsafe(i interface{}) bool {
    switch v := i.(type) {
    case Row:
        return v.unsafe
    case *Row:
        return v.unsafe
    case Rows:
        return v.unsafe
    case *Rows:
        return v.unsafe
    case NamedStmt:
        return v.Stmt.unsafe
    case *NamedStmt:
        return v.Stmt.unsafe
    case Stmt:
        return v.unsafe
    case *Stmt:
        return v.unsafe
    case qStmt:
        return v.unsafe
    case *qStmt:
        return v.unsafe
    case DB:
        return v.unsafe
    case *DB:
        return v.unsafe
    case Tx:
        return v.unsafe
    case *Tx:
        return v.unsafe
    case sql.Rows, *sql.Rows:
        return false
    default:
        return false
    }
}

func mapperFor(i interface{}) *reflectx.Mapper {
    switch i := i.(type) {
    case DB:
        return i.Mapper
    case *DB:
        return i.Mapper
    case Tx:
        return i.Mapper
    case *Tx:
        return i.Mapper
    default:
        return mapper()
    }
}

var _scannerInterface = reflect.TypeOf((*sql.Scanner)(nil)).Elem()
var _valuerInterface = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
