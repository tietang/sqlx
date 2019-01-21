package sqlx

import (
    "database/sql"
    "errors"
    "fmt"
    "github.com/tietang/sqlx/reflectx"
    "reflect"
)

type rowsi interface {
    Close() error
    Columns() ([]string, error)
    Err() error
    Next() bool
    Scan(...interface{}) error
}

// Rows is a wrapper around sql.Rows which caches costly reflect operations
// during a looped StructScan
type Rows struct {
    *sql.Rows
    unsafe bool
    Mapper *reflectx.Mapper
    // these fields cache memory use for a rows during iteration w/ structScan
    started bool
    fields  [][]int
    values  []interface{}
}

// SliceScan using this Rows.
func (r *Rows) SliceScan() ([]interface{}, error) {
    return SliceScan(r)
}

// MapScan using this Rows.
func (r *Rows) MapScan(dest map[string]interface{}) error {
    return MapScan(r, dest)
}

// StructScan is like sql.Rows.Scan, but scans a single Row into a single Struct.
// Use this and iterate over Rows manually when the memory load of Select() might be
// prohibitive.  *Rows.StructScan caches the reflect work of matching up column
// positions to fields to avoid that overhead per scan, which means it is not safe
// to run StructScan on the same Rows instance with different struct types.
func (r *Rows) StructScan(dest interface{}) error {
    v := reflect.ValueOf(dest)

    if v.Kind() != reflect.Ptr {
        return errors.New("must pass a pointer, not a value, to StructScan destination")
    }

    v = v.Elem()

    if !r.started {
        columns, err := r.Columns()
        if err != nil {
            return err
        }
        m := r.Mapper

        r.fields = m.TraversalsByName(v.Type(), columns)
        // if we are not unsafe and are missing fields, return an error
        if f, err := missingFields(r.fields); err != nil && !r.unsafe {
            return fmt.Errorf("missing destination name %s in %T", columns[f], dest)
        }
        r.values = make([]interface{}, len(columns))
        r.started = true
    }

    err := fieldsByTraversal(v, r.fields, r.values, true)
    if err != nil {
        return err
    }
    // scan into the struct field pointers and append to our results
    err = r.Scan(r.values...)
    if err != nil {
        return err
    }
    return r.Err()
}
