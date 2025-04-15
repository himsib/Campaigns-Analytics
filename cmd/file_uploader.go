// Use these helper file to upload files to GCS or AWS S3
package cmd

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/storage"

	"github.com/DTSL/golang-libraries/envutils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

var campaignCSVBucket = map[envutils.Env]string{
	envutils.Testing:     "st-wa-media",
	envutils.Development: "st-wa-media",
	envutils.Staging:     "st-wa-media",
	envutils.Production:  "marketing-camp-export.sendinblue.com",
}

const (
	scheme     = "https://"
	uploadPath = "upload/"
)

var (
	secretAccessKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
	accessKeyID     = os.Getenv("AWS_ACCESS_KEY_ID")
)

var schemeAWS = map[envutils.Env]string{
	envutils.Testing:     "http://",
	envutils.Development: "http://",
	envutils.Staging:     "http://",
	envutils.Production:  "https://",
}

var campaignCSVBucketAWS = map[envutils.Env]string{ //sample urls
	envutils.Testing:     "api-export-st2.zocket.com",
	envutils.Development: "api-export-st2.zocket.com",
	envutils.Staging:     "api-export-st2.zocket.com",
	envutils.Production:  "api-export.zocket.com",
}

type fileUploader struct {
	bucket       string
	bucketS3     string
	schemeAWS    string
	bucketObject func(name string) *storage.ObjectHandle
}

// Create a diContainer instance to manage dependencies
func newFileUploader(dic *diContainer) (*fileUploader, error) {
	gcsClient, err := dic.gcloud.StorageClient()
	if err != nil {
		return nil, errors.Wrap(err, "gcs client")
	}
	b := campaignCSVBucket[dic.flags.environment]
	bs := campaignCSVBucketAWS[dic.flags.environment]
	scAWS := schemeAWS[dic.flags.environment]
	return &fileUploader{
		bucketObject: gcsClient.Bucket(b).Object,
		bucket:       b,
		bucketS3:     bs,
		schemeAWS:    scAWS,
	}, nil
}

func newFileUploaderDIProvider(dic *diContainer) func() (*fileUploader, error) {
	var h *fileUploader
	var mu sync.Mutex
	return func() (*fileUploader, error) {
		mu.Lock()
		defer mu.Unlock()
		var err error
		if h == nil {
			h, err = newFileUploader(dic)
		}
		return h, err
	}
}

func (f *fileUploader) UploadFile(ctx context.Context, fileName string) (string, error) {
	log.Println("Uploading file to GCS")
	if f.bucketObject != nil && uploadPath != "" && fileName != "" {
		wc := f.bucketObject(uploadPath + fileName).NewWriter(ctx)

		wc.ContentType = "text/csv"

		csvFile, err := os.Open(fileName)
		if err != nil {
			return "", errors.Wrap(err, "opening csv file")
		}

		_, err = io.Copy(wc, csvFile)
		if err != nil {
			return "", errors.Wrap(err, "io copy to storage")
		}

		err = wc.Close()
		if err != nil {
			return "", errors.Wrap(err, "write close")
		}
	} else {
		return "", errors.New("bucketObject is nil")
	}

	fileURL := fmt.Sprintf("%s%s/%s%s", scheme, f.bucket, uploadPath, fileName)
	return fileURL, nil
}

func (f *fileUploader) UploadFileToAWS(ctx context.Context, fileName string) (string, error) {
	log.Println("creds===>", secretAccessKey, accessKeyID)
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("eu-west-1"),
		Credentials: credentials.NewStaticCredentials(
			accessKeyID,
			secretAccessKey,
			"",
		),
	})
	if err != nil {
		return "", errors.Wrap(err, "creating AWS session")
	}
	upFile, err := os.Open(fileName)
	if err != nil {
		return "", errors.Wrap(err, "opening CSV file for AWS")
	}
	defer upFile.Close()

	_, err = s3.New(sess).PutObject(&s3.PutObjectInput{
		Bucket:      aws.String(f.bucketS3),
		Key:         aws.String(uploadPath + fileName),
		Body:        upFile,
		ContentType: aws.String("text/csv"),
	})
	if err != nil {
		return "", errors.Wrap(err, "failed to upload file AWS")
	}

	fileURL := fmt.Sprintf("%s%s/%s%s", f.schemeAWS, f.bucketS3, uploadPath, fileName)

	return fileURL, nil
}
