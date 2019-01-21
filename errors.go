package sqlx

import (
    "errors"
)

// Common error messages.
var (
    ErrExpectingPointer                    = errors.New(`argument must be an address`)
    ErrExpectingSlicePointer               = errors.New(`argument must be a slice address`)
    ErrExpectingSliceMapStruct             = errors.New(`argument must be a slice address of maps or structs`)
    ErrExpectingMapOrStruct                = errors.New(`argument must be either a map or a struct`)
    ErrExpectingPointerToEitherMapOrStruct = errors.New(`expecting a pointer to either a map or a struct`)
)
var (
    errDeprecatedJSONBTag = errors.New(`Tag "jsonb" is deprecated. See "PostgreSQL: jsonb tag" at https://github.com/upper/db/releases/tag/v3.4.0`)
)
