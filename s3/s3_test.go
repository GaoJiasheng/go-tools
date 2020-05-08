package main

import (
	"io"
	"os"
	"testing"

	"github.com/labstack/gommon/log"
	"github.com/minio/minio-go"
)

func TestS3Minio(t *testing.T) {
	accessKey := "YOUR-ACCESS-KEY"
	secretKey := "YOUR-SECRET-KEY"
	endpoint := "s3.amazon.com"
	useSSL := true

	bucketName := "smapcampain-sg-test"

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, accessKey, secretKey, useSSL)
	if err != nil {
		t.Error(err)
	}

	log.Printf("%#v\n", minioClient) // minioClient is now setup

	// Upload the zip file
	objectName := "test.file"
	filePath := "/tmp/test.file"
	contentType := "application/zip"

	n, err := minioClient.FPutObject("smapcampain-sg-test", objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		t.Error(err)
	}

	log.Printf("Successfully uploaded %s of size %d\n", objectName, n)

	// List Object list
	// Create a done channel to control 'ListObjects' go routine.
	doneCh := make(chan struct{})
	// Indicate to our routine to exit cleanly upon return.
	defer close(doneCh)

	isRecursive := true
	objectCh := minioClient.ListObjects("smapcampain-sg-test", "", isRecursive, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			t.Error(object.Err)
			return
		}
		t.Logf("%v, %v, %v", object.Key, object.Size, object.Owner)
	}

	// Get FObject
	localName := "/tmp/test.fget.file"
	if err := minioClient.FGetObject(bucketName, "test.file", localName, minio.GetObjectOptions{}); err != nil {
		t.Error(err)
	} else {
		t.Log("Successfully saved ", localName)
	}

	// Get Object
	reader, err := minioClient.GetObject(bucketName, "test.file", minio.GetObjectOptions{})
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	localFile, err := os.Create("/tmp/test.get.file")
	if err != nil {
		t.Error(err)
	}
	defer localFile.Close()

	stat, err := reader.Stat()
	if err != nil {
		t.Error(err)
	}

	if _, err := io.CopyN(localFile, reader, stat.Size); err != nil {
		t.Error(err)
	} else {
		t.Log("Successfully saved ", localFile.Name())
	}
}
