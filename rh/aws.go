package rh

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sts"
)

// S3Config describes access to the S3 bucket
type S3Config struct {
	CrossAccount bool
	Region       string
	Arn          string
	Bucket       string
}

// CreateAWSSession will return a new AWS session using AWS Go SDK.
func CreateAWSSession(config *S3Config) (*session.Session, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	if config.CrossAccount { // use cross-account access
		svc := sts.New(sess)

		params := &sts.AssumeRoleInput{
			RoleArn:         aws.String(config.Arn),          // Required
			RoleSessionName: aws.String("role-session-name"), // Required
			// DurationSeconds: aws.Int64(1),
			// ExternalId:      aws.String("externalIdType"),
			// Policy:          aws.String("sessionPolicyDocumentType"),
			// SerialNumber:    aws.String("serialNumberType"),
			// TokenCode:       aws.String("tokenCodeType"),
		}

		resp, err := svc.AssumeRole(params)
		if err != nil {
			return nil, err
		}

		creds := credentials.NewStaticCredentials(
			*resp.Credentials.AccessKeyId,
			*resp.Credentials.SecretAccessKey,
			*resp.Credentials.SessionToken)

		return session.New(&aws.Config{
			Region:      aws.String(config.Region),
			Credentials: creds,
		}), nil
	}

	return sess, nil
}
