package postgres

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

// pgErrCodeUniqueViolation is the SQLSTATE for unique_violation in Postgres
// (and the SQL standard). The Store uses ON CONFLICT to absorb idempotent
// inserts, but a true concurrent race on a non-idempotency key would still
// surface as unique_violation; we map it to notify.ErrConflict at the call
// site via isUniqueViolation.
const pgErrCodeUniqueViolation = "23505"

// isUniqueViolation reports whether err carries the standard
// unique_violation SQLSTATE.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgErrCodeUniqueViolation
}
