package rh

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

// DatabaseConfig describes database connection parameters
type DatabaseConfig struct {
	Driver   string
	URL      string
	Username string
	Password string
}

// OpenDatabase will create a new database connection. This connection must be closed when it is no longer needed.
func OpenDatabase(config *DatabaseConfig) (*sqlx.DB, error) {
	var url string

	switch config.Driver {
	case "mysql":
		url = fmt.Sprintf("%s:%s@%s", config.Username, config.Password, config.URL)
	case "postgres":
		url = fmt.Sprintf("postgres://%s:%s@%s", config.Username, config.Password, config.URL)
	default:
		return nil, fmt.Errorf("unsupported database driver %+q in OpenDatabase", config.Driver)
	}

	database, err := sqlx.Connect(config.Driver, url)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to OpenDatabase, cannot connect to %+q", config.URL)
	}

	if err := database.Ping(); err != nil {
		return nil, errors.Wrapf(err, "failed to OpenDatabase, ping failed %+q", config.URL)
	}

	return database, nil
}

// GetExportMarker returns the export marker record for the given bucket and tableName. It will return an error if the record does not exist. Before the first import you must insert the record into the import_markers table.
func GetExportMarker(db *sqlx.DB, bucket, tableName string) (*ExportMarker, error) {
	var marker ExportMarker

	q := `
SELECT bucket, table_name, year, month, hour, day 
FROM   import_markers 
WHERE  bucket=$1
AND    table_name=$2
`
	if err := db.Get(&marker, q, bucket, tableName); err != nil {
		return nil, errors.Wrapf(err, "getExportMarker failed to Get for bucket %+q and tableName %+q", bucket, tableName)
	}

	return &marker, nil
}

// DeleteRecordsForMarker will delete database records before import to avoid duplicate values
func DeleteRecordsForMarker(db *sqlx.DB, marker *ExportMarker, timeColumn string) error {
	q := fmt.Sprintf("DELETE FROM %s WHERE %s >= $1 AND %s <= $2",
		marker.TableName,
		timeColumn,
		timeColumn,
	)

	timeRange := NewTimeRange(marker.Time())
	if _, err := db.Exec(q, timeRange.From, timeRange.To); err != nil {
		return err
	}

	return nil
}

// ImportFile will load Redshift table from the file in the S3 bucket. The file is identified by the marker.
func ImportFile(db *sqlx.DB, config *S3Config, marker *ExportMarker) error {
	q := fmt.Sprintf(`
COPY %s
FROM 's3://%s/%s'
WITH CREDENTIALS 'aws_iam_role=%s'
REGION '%s'
IGNOREHEADER 1
NULL AS 'NULL'
ESCAPE
`,
		marker.TableName,
		marker.Bucket,
		marker.FullPath(),
		config.Arn,
		config.Region,
	)

	if _, err := db.Exec(q); err != nil {
		return err
	}

	return nil
}

// UpdateExportMarker updates the marker record in import_markers table.
func UpdateExportMarker(db *sqlx.DB, marker *ExportMarker) error {
	q := `
UPDATE import_markers
SET    year = :year, month = :month, day = :day, hour = :hour
WHERE  bucket = :bucket
AND    table_name = :table_name
	`

	if _, err := db.NamedExec(q, marker); err != nil {
		return err
	}

	return nil
}
