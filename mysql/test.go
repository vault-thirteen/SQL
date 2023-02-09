// test.go.

package mysql

import (
	"database/sql"
	"fmt"

	"github.com/go-sql-driver/mysql"
	"github.com/vault-thirteen/errorz"
)

// Test Database Parameters.
const (
	TestDatabaseNet      = "tcp"
	TestDatabaseDriver   = "mysql"
	TestDatabaseHost     = "localhost"
	TestDatabasePort     = "3306"
	TestDatabaseDatabase = "test"
	TestDatabaseUser     = "test"
	TestDatabasePassword = "test"
)

// Table Names.
const (
	TableNameExistent    = "TableA"
	TableNameNotExistent = "xxxxxxxxx"
)

func makeTestDatabaseDsn() (dsn string) {
	const HostPortSeparator = ":"
	var configuration mysql.Config
	configuration = mysql.Config{
		Net:  TestDatabaseNet,
		Addr: TestDatabaseHost + HostPortSeparator + TestDatabasePort,
		Params: map[string]string{
			"allowNativePasswords": "true",
		},
		DBName: TestDatabaseDatabase,
		User:   TestDatabaseUser,
		Passwd: TestDatabasePassword,
	}
	return configuration.FormatDSN()
}

func connectToTestDatabase(
	dsn string,
) (sqlConnection *sql.DB, err error) {
	return sql.Open(TestDatabaseDriver, dsn)
}

func createTestTable() (err error) {
	const QueryfCreateTable = `CREATE TABLE IF NOT EXISTS %v
(
	Id serial,
	Name varchar(255),
	Number int
);`

	var sqlConnection *sql.DB
	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	if err != nil {
		return
	}
	defer func() {
		var derr = sqlConnection.Close()
		err = errorz.Combine(err, derr)
	}()

	// Create the Table.
	var query string = fmt.Sprintf(QueryfCreateTable, TableNameExistent)
	_, err = sqlConnection.Exec(query)
	if err != nil {
		return
	}
	return
}
