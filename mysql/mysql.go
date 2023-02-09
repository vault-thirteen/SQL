// mysql.go.

package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/vault-thirteen/auxie/unicode"
	"github.com/vault-thirteen/errorz"
)

const ErrfBadSymbol = "Bad Symbol: '%v'."

// SQL Queries.
const (
	QueryfTableExists = `SELECT COUNT(*)
FROM information_schema.tables
WHERE
	(table_schema = ?) AND
	(table_name = ?);`
	QueryfTableColumnNames = `SELECT
	COLUMN_NAME AS col
FROM INFORMATION_SCHEMA.COLUMNS
WHERE
	(table_schema = ?) AND
    (table_name = ?)
ORDER BY col ASC;`
)

// Symbols.
const (
	SingleBacktickQuote = "`"
	DoubleBacktickQuote = SingleBacktickQuote + SingleBacktickQuote
)

// TableExists Function checks whether the specified Table exists.
func TableExists(
	connection *sql.DB,
	schemaName string,
	tableName string,
) (result bool, err error) {
	var statement *sql.Stmt
	statement, err = connection.Prepare(QueryfTableExists)
	if err != nil {
		return
	}
	defer func() {
		var derr error
		derr = statement.Close()
		err = errorz.Combine(err, derr)
	}()
	var row *sql.Row = statement.QueryRow(schemaName, tableName)
	var tablesCount int
	err = row.Scan(&tablesCount)
	if err != nil {
		return
	}
	if tablesCount == 1 {
		result = true
	}
	return
}

func TableNameIsGood(
	tableName string,
) (bool, error) {
	return IdentifierIsGood(tableName)
}

func IdentifierIsGood(
	identifierName string,
) (bool, error) {
	for _, letter := range identifierName {
		if (!unicode.SymbolIsLatLetter(letter)) &&
			(!unicode.SymbolIsNumber(letter)) &&
			(letter != '_') {
			return false, fmt.Errorf(ErrfBadSymbol, string(letter))
		}
	}

	return true, nil
}

// ScreenSingleBacktickQuotes Function does the Single Quotes Screening.
func ScreenSingleBacktickQuotes(
	src string,
) (dst string) {
	return strings.ReplaceAll(src, SingleBacktickQuote, DoubleBacktickQuote)
}

// GetTableColumnNames Function lists the Table's Column Names sorted
// alphabetically (from A to Z).
func GetTableColumnNames(
	connection *sql.DB,
	schemaName string,
	tableName string,
) (columnNames []string, err error) {
	var statement *sql.Stmt
	statement, err = connection.Prepare(QueryfTableColumnNames)
	if err != nil {
		return
	}
	defer func() {
		var derr error = statement.Close()
		err = errorz.Combine(err, derr)
	}()
	var rows *sql.Rows
	rows, err = statement.Query(schemaName, tableName)
	if err != nil {
		return
	}
	defer func() {
		var derr error = rows.Close()
		err = errorz.Combine(err, derr)
	}()
	var columnName string
	for rows.Next() {
		err = rows.Scan(&columnName)
		if err != nil {
			return
		}
		columnNames = append(columnNames, columnName)
	}
	return
}
