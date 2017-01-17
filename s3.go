package rh

import (
	"fmt"
	"strconv"
	"strings"

	"bytes"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

// CreateS3Service will create a new AWS session and return a new S3 service using AWS Go SDK.
func CreateS3Service(config *S3Config) (*s3.S3, error) {
	sess, err := CreateAWSSession(config)
	if err != nil {
		return nil, errors.Wrap(err, "createS3Service failed to createAWSSession")
	}

	svc := s3.New(sess)
	return svc, nil
}

func findLastKey(svc *s3.S3, bucket, path string) (string, error) {
	params := &s3.ListObjectsInput{
		Bucket:    aws.String(bucket),
		Delimiter: aws.String("/"),
		Prefix:    aws.String(path),
		MaxKeys:   aws.Int64(100), // NOTE: we do not expect to have more than 31 keys per path
	}

	resp, err := svc.ListObjects(params)
	if err != nil {
		return "", errors.Wrapf(err, "findLastExport failed to fistLastKey for path %+q in bucket %+q", path, bucket)
	}

	result := ""
	if len(resp.CommonPrefixes) > 0 {
		for _, p := range resp.CommonPrefixes {
			s := strings.TrimPrefix(*p.Prefix, path)
			s = strings.TrimSuffix(s, "/")
			if s > result {
				result = s
			}
		}
	} else {
		for _, o := range resp.Contents {
			s := strings.TrimPrefix(*o.Key, path)
			s = strings.TrimSuffix(s, "/")
			if s > result {
				result = s
			}
		}
	}

	return result, nil
}

// FindLastExport will return the ExportMarker for the latest export file in the specified S3 bucket. The bucket is expected to have a hierarchical structure that starts with the tableName and includes subfolders for year, month, day, and hour:
//
// +-- tableName/
// +------ YYYY/
// +---------- MM/
// +-------------- DD/
// +------------------ tableName-YYYYMMDDHH.txt
//
func FindLastExport(svc *s3.S3, bucketName, tableName string) (*ExportMarker, error) {
	path := tableName + "/"
	year, err := findLastKey(svc, bucketName, path)
	if err != nil {
		return nil, errors.Wrap(err, "findLastExport failed to findLastKey for year")
	}

	if year == "" {
		return nil, nil
	}

	result := ExportMarker{
		Bucket:    bucketName,
		TableName: tableName,
		Year:      0,
		Month:     1,
		Day:       1,
		Hour:      0,
	}

	result.Year, err = strconv.Atoi(year)
	if err != nil {
		return nil, errors.Wrapf(err, "findLastExport failed to parse year %+q", year)
	}

	path = path + year + "/"
	month, err := findLastKey(svc, bucketName, path)
	if err != nil {
		return nil, errors.Wrap(err, "findLastExport failed to findLastKey for month")
	}

	if month == "" {
		return &result, nil
	}

	result.Month, err = strconv.Atoi(month)
	if err != nil {
		return nil, errors.Wrapf(err, "findLastExport failed to parse month %+q", month)
	}

	path = path + month + "/"
	day, err := findLastKey(svc, bucketName, path)
	if err != nil {
		return nil, errors.Wrap(err, "findLastExport failed to findLastKey for day")
	}

	if day == "" {
		return &result, nil
	}

	result.Day, err = strconv.Atoi(day)
	if err != nil {
		return nil, errors.Wrapf(err, "findLastExport failed to parse day %+q", day)
	}

	path = path + day + "/"
	file, err := findLastKey(svc, bucketName, path)
	if err != nil {
		return nil, errors.Wrap(err, "findLastExport failed to findLastKey for file")
	}

	if file == "" {
		return &result, nil
	}

	hour := strings.TrimPrefix(file, fmt.Sprintf("%s-%04d%02d%02d", tableName, result.Year, result.Month, result.Day))
	hour = strings.TrimSuffix(hour, ".txt")

	result.Hour, err = strconv.Atoi(hour)
	if err != nil {
		return nil, errors.Wrapf(err, "findLastExport failed to parse hour %+q from filename %+q", hour, file)
	}

	return &result, nil
}

// ExportRecords will save records into the .txt file in the S3 bucket. The `marker` will determine the file path for the .txt file.
func ExportRecords(svc *s3.S3, marker *ExportMarker, records []Txt) error {
	var buf bytes.Buffer

	if len(records) > 0 {
		header := records[0].TxtHeader()
		buf.Write([]byte(header))
		buf.Write([]byte("\n"))

		for _, r := range records {
			buf.Write([]byte(r.TxtValues()))
			buf.Write([]byte("\n"))
		}
	}

	params := &s3.PutObjectInput{
		Bucket:        aws.String(marker.Bucket),
		Key:           aws.String(marker.FullPath()),
		Body:          bytes.NewReader(buf.Bytes()),
		ContentLength: aws.Int64(int64(buf.Len())),
		ContentType:   aws.String("text/plain"),
	}

	if _, err := svc.PutObject(params); err != nil {
		return errors.Wrapf(err, "ExportRecords failed to PutObject %+q into bucket %+q", marker.FullPath(), marker.Bucket)
	}

	return nil
}

// GetExportFileLength will return information about the length of the .txt file in the S3 bucket. The `marker` will determine the file path for the .txt file.
func GetExportFileLength(svc *s3.S3, marker *ExportMarker) (int64, error) {
	params := &s3.HeadObjectInput{
		Bucket: aws.String(marker.Bucket),
		Key:    aws.String(marker.FullPath()),
	}

	resp, err := svc.HeadObject(params)
	if err != nil {
		return 0, errors.Wrapf(err, "GetExportFileLength failed to GetObject %+q in bucket %+q", marker.FullPath(), marker.Bucket)
	}

	return *resp.ContentLength, nil
}
