package postgresql

import (
	"database/sql"
	"fmt"

	"go.uber.org/multierr"

	// PostgreSQL driver.
	_ "github.com/lib/pq"
)

// Test database parameters.
const (
	TestDatabaseDriver     = "postgres"
	TestDatabaseHost       = "localhost"
	TestDatabasePort       = "5432"
	TestDatabaseDatabase   = "test"
	TestDatabaseUser       = "test"
	TestDatabasePassword   = "test"
	TestDatabaseParameters = "sslmode=disable"
)

// Schema Names.
const (
	// SchemaCommon is a common schema.
	SchemaCommon = "public"
)

// Table Names.
const (
	TableNameExistent    = "TableA"
	TableNameNotExistent = "xxxxxxxxx"
)

// Procedure Names.
const (
	ProcedureNameExistent    = "procedure_simulator"
	ProcedureNameNotExistent = "xxxxxxxxx"
)

func makeTestDatabaseDsn() (dsn string) {
	return MakeDsn(
		TestDatabaseHost,
		TestDatabasePort,
		TestDatabaseDatabase,
		TestDatabaseUser,
		TestDatabasePassword,
		TestDatabaseParameters,
	)
}

func connectToTestDatabase(
	dsn string,
) (sqlConnection *sql.DB, err error) {
	return sql.Open(TestDatabaseDriver, dsn)
}

func createTestTable() (err error) {
	const QueryfCreateTable = `CREATE TABLE IF NOT EXISTS %v
(
	"Id" serial
);`

	var sqlConnection *sql.DB
	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	if err != nil {
		return err
	}

	defer func() {
		derr := sqlConnection.Close()
		err = multierr.Combine(err, derr)
	}()

	// Create the Table.
	query := fmt.Sprintf(
		QueryfCreateTable,
		fmt.Sprintf(`%s."%s"`,
			SchemaCommon,
			TableNameExistent,
		),
	)

	_, err = sqlConnection.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func createTestProcedure() (err error) {
	const QueryfCreateProcedure = `CREATE OR REPLACE PROCEDURE %v()
LANGUAGE SQL
AS
$$
	SELECT 123
$$;`

	var sqlConnection *sql.DB
	sqlConnection, err = connectToTestDatabase(makeTestDatabaseDsn())
	if err != nil {
		return err
	}

	defer func() {
		derr := sqlConnection.Close()
		err = multierr.Combine(err, derr)
	}()

	// Create the Table.
	query := fmt.Sprintf(
		QueryfCreateProcedure,
		ProcedureNameExistent,
	)

	_, err = sqlConnection.Exec(query)
	if err != nil {
		return err
	}

	return nil
}
