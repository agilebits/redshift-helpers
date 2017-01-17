package rh

import (
	"fmt"
	"time"
)

// ExportMarker contains the time components that are used to construct the file path in S3 bucket.
type ExportMarker struct {
	Bucket    string `db:"bucket"`
	TableName string `db:"table_name"`
	Year      int    `db:"year"`
	Month     int    `db:"month"`
	Day       int    `db:"day"`
	Hour      int    `db:"hour"`
}

// FullPath returns full path for the export file in the bucket
func (em *ExportMarker) FullPath() string {
	return fmt.Sprintf("%s/%04d/%02d/%02d/%s", em.TableName, em.Year, em.Month, em.Day, em.FileName())
}

// FileName returns file name for the export file
func (em *ExportMarker) FileName() string {
	return fmt.Sprintf("%s-%04d%02d%02d%02d.txt", em.TableName, em.Year, em.Month, em.Day, em.Hour)
}

// Time converts ExportMarker values into a time in UTC timezone.
func (em *ExportMarker) Time() time.Time {
	return time.Date(em.Year, time.Month(em.Month), em.Day, em.Hour, 0, 0, 0, time.UTC)
}

// Next returns the export marker for the next hour
func (em *ExportMarker) Next() *ExportMarker {
	t := em.Time().Add(time.Hour)
	return &ExportMarker{
		Bucket:    em.Bucket,
		TableName: em.TableName,
		Year:      t.Year(),
		Month:     int(t.Month()),
		Day:       t.Day(),
		Hour:      t.Hour(),
	}
}
