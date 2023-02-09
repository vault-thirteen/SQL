package postgresql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/vault-thirteen/unicode"
	"go.uber.org/multierr"
)

// PostgreSQL constants.
const (
	DsnPrefix                    = "postgresql://"
	DsnUsernamePasswordDelimiter = ":"
	DsnUsernameHostDelimiter     = "@"
	DsnHostPortDelimiter         = ":"
	DsnHostDatabaseDelimiter     = "/"
	DsnParametersPrefix          = "?"
)

// Symbols.
const (
	SingleQuote      = `'`
	SingleQuoteTwice = SingleQuote + SingleQuote
	Underscore       = '_'
)

// Error message formats.
const (
	// ErrFBadSymbol is an error message format for a bad symbol.
	ErrFBadSymbol = "bad symbol: '%v'"
)

// SQL query templates.
const (
	QueryFTableExists = `SELECT EXISTS
(
	SELECT 1
	FROM information_schema.tables
	WHERE
		table_schema = $1 AND
		table_name = $2
);`

	QueryFProcedureExists = `SELECT EXISTS
(
    SELECT 1
    FROM pg_catalog.pg_proc
    JOIN pg_namespace
    ON pg_catalog.pg_proc.pronamespace = pg_namespace.oid
    WHERE
        pg_proc.proname = $1 AND
        pg_namespace.nspname = $2
);`
)

// MakeDsn function returns a connection string for PostgreSQL database
// according to the documentation located at:
// "https://www.postgresql.org/docs/10/libpq-connect.html".
// Format reference:
// postgresql://[user[:password]@][netloc][:port][,...][/dbname][?param1=value1&...]
func MakeDsn(
	host string, // Obligatory parameter.
	port string, // Obligatory parameter.
	database string, // Optional parameter.
	username string, // Optional parameter.
	password string, // Optional parameter.

	// Key-value list without the '?' prefix.
	// Optional parameter.
	parameters string,
) (dsn string) {
	dsn = DsnPrefix

	if len(username) > 0 {
		if len(password) > 0 {
			dsn += username + DsnUsernamePasswordDelimiter +
				password + DsnUsernameHostDelimiter
		} else {
			dsn += username + DsnUsernameHostDelimiter
		}
	}

	dsn += host + DsnHostPortDelimiter + port

	if len(database) > 0 {
		dsn += DsnHostDatabaseDelimiter + database
	}

	if len(parameters) > 0 {
		dsn += DsnParametersPrefix + parameters
	}

	return dsn
}

// TableExists function checks whether the specified table exists.
func TableExists(
	connection *sql.DB,
	schemaName string,
	tableName string,
) (result bool, err error) {
	var statement *sql.Stmt
	statement, err = connection.Prepare(QueryFTableExists)
	if err != nil {
		return false, err
	}

	defer func() {
		derr := statement.Close()
		err = multierr.Combine(err, derr)
	}()

	var row *sql.Row
	var tableExists bool
	row = statement.QueryRow(schemaName, tableName)
	err = row.Scan(&tableExists)
	if err != nil {
		return false, err
	}

	return tableExists, nil
}

// TableNameIsGood checks whether the specified table name is a good identifier.
func TableNameIsGood(
	tableName string,
) (bool, error) {
	return IdentifierIsGood(tableName)
}

// ProcedureNameIsGood checks whether the specified procedure name is a good
// identifier.
func ProcedureNameIsGood(
	procedureName string,
) (bool, error) {
	return IdentifierIsGood(procedureName)
}

// IdentifierIsGood checks whether the specified identifier is good.
func IdentifierIsGood(
	identifierName string,
) (bool, error) {
	for _, letter := range identifierName {
		if (!unicode.SymbolIsLatLetter(letter)) &&
			(!unicode.SymbolIsNumber(letter)) &&
			(letter != Underscore) {
			return false, fmt.Errorf(ErrFBadSymbol, string(letter))
		}
	}

	return true, nil
}

// ProcedureExists function checks whether the specified procedure exists.
func ProcedureExists(
	connection *sql.DB,
	schemaName string,
	procedureName string,
) (procedureExists bool, err error) {
	var statement *sql.Stmt
	statement, err = connection.Prepare(QueryFProcedureExists)
	if err != nil {
		return false, err
	}

	defer func() {
		derr := statement.Close()
		err = multierr.Combine(err, derr)
	}()

	var row *sql.Row
	row = statement.QueryRow(procedureName, schemaName)
	err = row.Scan(&procedureExists)
	if err != nil {
		return false, err
	}

	return procedureExists, nil
}

// ScreenSingleQuotes function does the single quotes screening.
func ScreenSingleQuotes(
	src string,
) (dst string) {
	return strings.ReplaceAll(src, SingleQuote, SingleQuoteTwice)
}
