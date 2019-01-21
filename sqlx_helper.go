package sqlx

import (
    "database/sql"
    "errors"
    "fmt"
    "github.com/tietang/sqlx/reflectx"
    "io/ioutil"
    "path/filepath"
    "reflect"
)

// SliceScan a row, returning a []interface{} with values similar to MapScan.
// This function is primarily intended for use where the number of columns
// is not known.  Because you can pass an []interface{} directly to Scan,
// it's recommended that you do that as it will not have to allocate new
// slices per row.
func SliceScan(r ColScanner) ([]interface{}, error) {
    // ignore r.started, since we needn't use reflect for anything.
    columns, err := r.Columns()
    if err != nil {
        return []interface{}{}, err
    }

    values := make([]interface{}, len(columns))
    for i := range values {
        values[i] = new(interface{})
    }

    err = r.Scan(values...)

    if err != nil {
        return values, err
    }

    for i := range columns {
        values[i] = *(values[i].(*interface{}))
    }

    return values, r.Err()
}

// MapScan scans a single Row into the dest map[string]interface{}.
// Use this to get results for SQL that might not be under your control
// (for instance, if you're building an interface for an SQL server that
// executes SQL from input).  Please do not use this as a primary interface!
// This will modify the map sent to it in place, so reuse the same map with
// care.  Columns which occur more than once in the result will overwrite
// each other!
func MapScan(r ColScanner, dest map[string]interface{}) error {
    // ignore r.started, since we needn't use reflect for anything.
    columns, err := r.Columns()
    if err != nil {
        return err
    }

    values := make([]interface{}, len(columns))
    for i := range values {
        values[i] = new(interface{})
    }

    err = r.Scan(values...)
    if err != nil {
        return err
    }

    for i, column := range columns {
        dest[column] = *(values[i].(*interface{}))
    }

    return r.Err()
}

// structOnlyError returns an error appropriate for type when a non-scannable
// struct is expected but something else is given
func structOnlyError(t reflect.Type) error {
    isStruct := t.Kind() == reflect.Struct
    isScanner := reflect.PtrTo(t).Implements(_scannerInterface)
    if !isStruct {
        return fmt.Errorf("expected %s but got %s", reflect.Struct, t.Kind())
    }
    if isScanner {
        return fmt.Errorf("structscan expects a struct dest but the provided struct type %s implements scanner", t.Name())
    }
    return fmt.Errorf("expected a struct, but struct %s has no exported fields", t.Name())
}

// scanAll scans all rows into a destination, which must be a slice of any
// type.  If the destination slice type is a Struct, then StructScan will be
// used on each row.  If the destination is some other kind of base type, then
// each row must only have one column which can scan into that type.  This
// allows you to do something like:
//
//    rows, _ := db.Query("select id from people;")
//    var ids []int
//    scanAll(rows, &ids, false)
//
// and ids will be a list of the id results.  I realize that this is a desirable
// interface to expose to users, but for now it will only be exposed via changes
// to `Get` and `Select`.  The reason that this has been implemented like this is
// this is the only way to not duplicate reflect work in the new API while
// maintaining backwards compatibility.
func scanAll(rows *Rows, dest interface{}, structOnly bool) error {
    var v, vp reflect.Value

    value := reflect.ValueOf(dest)

    // json.Unmarshal returns errors for these
    if value.Kind() != reflect.Ptr {
        return errors.New("must pass a pointer, not a value, to StructScan destination")
    }
    if value.IsNil() {
        return errors.New("nil pointer passed to StructScan destination")
    }
    direct := reflect.Indirect(value)

    if v.Elem().Kind() == reflect.Slice || v.Elem().Kind() == reflect.Map {
        return fetchRows(rows.Rows, dest)
    }

    return fetchRow(rows.Rows, dest)

    slice, err := baseType(value.Type(), reflect.Slice)
    if err != nil {
        return err
    }

    isPtr := slice.Elem().Kind() == reflect.Ptr
    base := reflectx.Deref(slice.Elem())
    scannable := isScannable(base)

    if structOnly && scannable {
        return structOnlyError(base)
    }

    columns, err := rows.Columns()
    if err != nil {
        return err
    }

    // if it's a base type make sure it only has 1 column;  if not return an error
    if scannable && len(columns) > 1 {
        return fmt.Errorf("non-struct dest type %s with >1 columns (%d)", base.Kind(), len(columns))
    }

    if !scannable {
        var values []interface{}
        //var m *reflectx.Mapper

        //switch rows.(type) {
        //case *Rows:
        m := rows.Mapper
        //default:
        //    m = mapper()
        //}

        fields := m.TraversalsByName(base, columns)
        // if we are not unsafe and are missing fields, return an error
        if f, err := missingFields(fields); err != nil && !isUnsafe(rows) {
            return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
        }
        values = make([]interface{}, len(columns))

        for rows.Next() {
            // create a new struct type (which returns PtrTo) and indirect it
            vp = reflect.New(base)
            v = reflect.Indirect(vp)

            err = fieldsByTraversal(v, fields, values, true)
            if err != nil {
                return err
            }

            // scan into the struct field pointers and append to our results
            err = rows.Scan(values...)
            if err != nil {
                return err
            }

            if isPtr {
                direct.Set(reflect.Append(direct, vp))
            } else {
                direct.Set(reflect.Append(direct, v))
            }
        }
    } else {
        for rows.Next() {
            vp = reflect.New(base)
            err = rows.Scan(vp.Interface())
            if err != nil {
                return err
            }
            // append
            if isPtr {
                direct.Set(reflect.Append(direct, vp))
            } else {
                direct.Set(reflect.Append(direct, reflect.Indirect(vp)))
            }
        }
    }

    return rows.Err()
}

// FIXME: StructScan was the very first bit of API in sqlx, and now unfortunately
// it doesn't really feel like it's named properly.  There is an incongruency
// between this and the way that StructScan (which might better be ScanStruct
// anyway) works on a rows object.

// StructScan all rows from an sql.Rows or an sqlx.Rows into the dest slice.
// StructScan will scan in the entire rows result, so if you do not want to
// allocate structs for the entire result, use Queryx and see sqlx.Rows.StructScan.
// If rows is sqlx.Rows, it will use its mapper, otherwise it will use the default.
func StructScan(rows *Rows, dest interface{}) error {
    return scanAll(rows, dest, true)

}

// reflect helpers

func baseType(t reflect.Type, expected reflect.Kind) (reflect.Type, error) {
    t = reflectx.Deref(t)
    if t.Kind() != expected {
        return nil, fmt.Errorf("expected %s but got %s", expected, t.Kind())
    }
    return t, nil
}

// fieldsByName fills a values interface with fields from the passed value based
// on the traversals in int.  If ptrs is true, return addresses instead of values.
// We write this instead of using FieldsByName to save allocations and map lookups
// when iterating over many rows.  Empty traversals will get an interface pointer.
// Because of the necessity of requesting ptrs or values, it's considered a bit too
// specialized for inclusion in reflectx itself.
func fieldsByTraversal(v reflect.Value, traversals [][]int, values []interface{}, ptrs bool) error {
    v = reflect.Indirect(v)
    if v.Kind() != reflect.Struct {
        return errors.New("argument not a struct")
    }

    for i, traversal := range traversals {
        if len(traversal) == 0 {
            values[i] = new(interface{})
            continue
        }
        f := reflectx.FieldByIndexes(v, traversal)
        if ptrs {
            values[i] = f.Addr().Interface()
        } else {
            values[i] = f.Interface()
        }
    }
    return nil
}

func missingFields(transversals [][]int) (field int, err error) {
    for i, t := range transversals {
        if len(t) == 0 {
            return i, errors.New("missing field")
        }
    }
    return 0, nil
}



// Connect to a database and verify with a ping.
func Connect(driverName, dataSourceName string) (*DB, error) {
    db, err := Open(driverName, dataSourceName)
    if err != nil {
        return nil, err
    }
    err = db.Ping()
    if err != nil {
        db.Close()
        return nil, err
    }
    return db, nil
}

// MustConnect connects to a database and panics on error.
func MustConnect(driverName, dataSourceName string) *DB {
    db, err := Connect(driverName, dataSourceName)
    if err != nil {
        panic(err)
    }
    return db
}

// Preparex prepares a statement.
func Preparex(p Preparer, query string) (*Stmt, error) {
    s, err := p.Prepare(query)
    if err != nil {
        return nil, err
    }
    return &Stmt{Stmt: s, unsafe: isUnsafe(p), Mapper: mapperFor(p)}, err
}

// Select executes a query using the provided Queryer, and StructScans each row
// into dest, which must be a slice.  If the slice elements are scannable, then
// the result set must have only one column.  Otherwise, StructScan is used.
// The *sql.Rows are closed automatically.
// Any placeholder parameters are replaced with supplied args.
func Select(q Queryer, dest interface{}, query string, args ...interface{}) error {
    rows, err := q.Queryx(query, args...)
    if err != nil {
        return err
    }
    // if something happens here, we want to make sure the rows are Closed
    defer rows.Close()
    return scanAll(rows, dest, false)
}

// Get does a QueryRow using the provided Queryer, and scans the resulting row
// to dest.  If dest is scannable, the result must only have one column.  Otherwise,
// StructScan is used.  Get will return sql.ErrNoRows like row.Scan would.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func Get(q Queryer, dest interface{}, query string, args ...interface{}) error {
    r := q.QueryRowx(query, args...)

    return r.scanAny(dest, false)
}

// LoadFile exec's every statement in a file (as a single call to Exec).
// LoadFile may return a nil *sql.Result if errors are encountered locating or
// reading the file at path.  LoadFile reads the entire file into memory, so it
// is not suitable for loading large data dumps, but can be useful for initializing
// schemas or loading indexes.
//
// FIXME: this does not really work with multi-statement files for mattn/go-sqlite3
// or the go-mysql-driver/mysql drivers;  pq seems to be an exception here.  Detecting
// this by requiring something with DriverName() and then attempting to split the
// queries will be difficult to get right, and its current driver-specific behavior
// is deemed at least not complex in its incorrectness.
func LoadFile(e Execer, path string) (*sql.Result, error) {
    realpath, err := filepath.Abs(path)
    if err != nil {
        return nil, err
    }
    contents, err := ioutil.ReadFile(realpath)
    if err != nil {
        return nil, err
    }
    res, err := e.Exec(string(contents))
    return &res, err
}

// MustExec execs the query using e and panics if there was an error.
// Any placeholder parameters are replaced with supplied args.
func MustExec(e Execer, query string, args ...interface{}) sql.Result {
    res, err := e.Exec(query, args...)
    if err != nil {
        panic(err)
    }
    return res
}

