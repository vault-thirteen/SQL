// mysql_test.go.

package mysql

import (
	"database/sql"
	"testing"

	"github.com/vault-thirteen/tester"
)

// This Test depends on the Test Environment.
// Please ensure that all the Parameters are correct before using it.
func Test_TableExists(t *testing.T) {
	var aTest *tester.Test = tester.New(t)

	// Prepare the Database for the Test.
	var err error = createTestTable()
	aTest.MustBeNoError(err)

	// Connect the Database.
	var sqlConnection *sql.DB
	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	aTest.MustBeNoError(err)
	defer func() {
		err = sqlConnection.Close()
		aTest.MustBeNoError(err)
	}()

	// Test #1. Table exists.
	var tableExists bool
	tableExists, err = TableExists(
		sqlConnection,
		TestDatabaseDatabase,
		TableNameExistent,
	)
	aTest.MustBeNoError(err)
	aTest.MustBeEqual(tableExists, true)

	// Test #2. Table does not exist.
	tableExists, err = TableExists(
		sqlConnection,
		TestDatabaseDatabase,
		TableNameNotExistent,
	)
	aTest.MustBeNoError(err)
	aTest.MustBeEqual(tableExists, false)
}

func Test_IdentifierIsGood(t *testing.T) {
	var aTest *tester.Test = tester.New(t)

	// Test #1.
	var identifierName string = "xB_9"
	var result bool
	var err error
	result, err = IdentifierIsGood(identifierName)
	aTest.MustBeNoError(err)
	aTest.MustBeEqual(result, true)

	// Test #2.
	identifierName = "xB_9куку"
	result, err = IdentifierIsGood(identifierName)
	aTest.MustBeAnError(err)
	aTest.MustBeEqual(result, false)

	// Test #3.
	identifierName = "xB_9!@"
	result, err = IdentifierIsGood(identifierName)
	aTest.MustBeAnError(err)
	aTest.MustBeEqual(result, false)

	// Test #4.
	identifierName = "DROP TABLE xyz;"
	result, err = IdentifierIsGood(identifierName)
	aTest.MustBeAnError(err)
	aTest.MustBeEqual(result, false)
}

func Test_ScreenSingleBacktickQuotes(t *testing.T) {

	var aTest *tester.Test
	var dst string
	var dstExpected string
	var src string

	aTest = tester.New(t)

	// Test #1.
	src = "John"
	dstExpected = "John"
	dst = ScreenSingleBacktickQuotes(src)
	aTest.MustBeEqual(dst, dstExpected)

	// Test #2.
	src = "D'Artagnan, " +
		`D"Artagnan, ` +
		"D`Artagnan, " +
		"D``Artagnan."
	dstExpected = "D'Artagnan, " +
		`D"Artagnan, ` +
		"D``Artagnan, " +
		"D````Artagnan."
	dst = ScreenSingleBacktickQuotes(src)
	aTest.MustBeEqual(dst, dstExpected)
}

// This Test depends on the Test Environment.
// Please ensure that all the Parameters are correct before using it.
func Test_GetTableColumnNames(t *testing.T) {
	var aTest *tester.Test = tester.New(t)

	// Prepare the Database for the Test.
	var err error = createTestTable()
	aTest.MustBeNoError(err)

	// Connect the Database.
	var sqlConnection *sql.DB
	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	aTest.MustBeNoError(err)
	defer func() {
		err = sqlConnection.Close()
		aTest.MustBeNoError(err)
	}()

	// Test #1. Table exists.
	var columnNames []string
	columnNames, err = GetTableColumnNames(
		sqlConnection,
		TestDatabaseDatabase,
		TableNameExistent,
	)
	aTest.MustBeNoError(err)
	aTest.MustBeEqual(
		columnNames,
		[]string{
			"Id",
			"Name",
			"Number",
		},
	)

	// Test #2. Table does not exist.
	columnNames, err = GetTableColumnNames(
		sqlConnection,
		TestDatabaseDatabase,
		TableNameNotExistent,
	)
	aTest.MustBeNoError(err)
	aTest.MustBeEqual(
		columnNames,
		[]string(nil),
	)
}
