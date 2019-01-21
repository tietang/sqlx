package sqlx

import (
    "database/sql"
    "github.com/tietang/sqlx/reflectx"
)

// Stmt is an sqlx wrapper around sql.Stmt with extra functionality
type Stmt struct {
    *sql.Stmt
    unsafe bool
    Mapper *reflectx.Mapper
}

// Unsafe returns a version of Stmt which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
func (s *Stmt) Unsafe() *Stmt {
    return &Stmt{Stmt: s.Stmt, unsafe: true, Mapper: s.Mapper}
}

// Select using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) Select(dest interface{}, args ...interface{}) error {
    return Select(&qStmt{s}, dest, "", args...)
}

// Get using the prepared statement.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (s *Stmt) Get(dest interface{}, args ...interface{}) error {
    return Get(&qStmt{s}, dest, "", args...)
}

// MustExec (panic) using this statement.  Note that the query portion of the error
// output will be blank, as Stmt does not expose its query.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) MustExec(args ...interface{}) sql.Result {
    return MustExec(&qStmt{s}, "", args...)
}

// QueryRowx using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) QueryRowx(args ...interface{}) *Row {
    qs := &qStmt{s}
    return qs.QueryRowx("", args...)
}

// Queryx using this statement.
// Any placeholder parameters are replaced with supplied args.
func (s *Stmt) Queryx(args ...interface{}) (*Rows, error) {
    qs := &qStmt{s}
    return qs.Queryx("", args...)
}

// qStmt is an unexposed wrapper which lets you use a Stmt as a Queryer & Execer by
// implementing those interfaces and ignoring the `query` argument.
type qStmt struct{ *Stmt }

func (q *qStmt) Query(query string, args ...interface{}) (*sql.Rows, error) {
    return q.Stmt.Query(args...)
}

func (q *qStmt) Queryx(query string, args ...interface{}) (*Rows, error) {
    r, err := q.Stmt.Query(args...)
    if err != nil {
        return nil, err
    }
    return &Rows{Rows: r, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}, err
}

func (q *qStmt) QueryRowx(query string, args ...interface{}) *Row {
    rows, err := q.Stmt.Query(args...)
    return &Row{rows: rows, err: err, unsafe: q.Stmt.unsafe, Mapper: q.Stmt.Mapper}
}

func (q *qStmt) Exec(query string, args ...interface{}) (sql.Result, error) {
    return q.Stmt.Exec(args...)
}
