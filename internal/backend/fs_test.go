// Copyright 2023 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestGetAttributes(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	testBucket := filepath.Join(tempDir, "some-bucket")
	bucketAttrs := BucketAttrs{
		DefaultEventBasedHold: false,
		VersioningEnabled:     true,
	}
	data, _ := json.Marshal(bucketAttrs)
	err := os.WriteFile(testBucket+bucketMetadataSuffix, data, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	notABucket := filepath.Join(tempDir, "not-a-bucket")
	err = os.WriteFile(notABucket+bucketMetadataSuffix, []byte("this is not valid json"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name          string
		inputPath     string
		expectedAttrs BucketAttrs
		expectErr     bool
	}{
		{
			name:      "file not found",
			inputPath: filepath.Join(tempDir, "unknown-bucket"),
		},
		{
			name:          "existing bucket",
			inputPath:     testBucket,
			expectedAttrs: bucketAttrs,
		},
		{
			name:      "invalid file",
			inputPath: notABucket,
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			attrs, err := getBucketAttributes(test.inputPath)
			if test.expectErr && err == nil {
				t.Fatal("expected error, but got <nil>")
			}

			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if diff := cmp.Diff(attrs, test.expectedAttrs); diff != "" {
				t.Errorf("incorrect attributes returned\nwant: %#v\ngot:  %#v\ndiff: %s", test.expectedAttrs, attrs, diff)
			}
		})
	}
}

func TestCreateObjectStreamsContentToDisk(t *testing.T) {
	t.Parallel()

	rootDir := t.TempDir()
	storage, err := NewStorageFS(nil, rootDir)
	if err != nil {
		t.Fatal(err)
	}
	const content = "some nice content"
	obj, err := storage.CreateObject(StreamingObject{
		ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "dir/object"},
		Content:     noopSeekCloser{strings.NewReader(content)},
	}, NoConditions{})
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	got, err := io.ReadAll(obj.Content)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Errorf("wrong content\nwant %q\ngot  %q", content, got)
	}
	if obj.Size != int64(len(content)) || obj.Md5Hash == "" || obj.Crc32c == "" {
		t.Errorf("incomplete attributes: %+v", obj.ObjectAttrs)
	}

	// A hidden object written through the API is listed.
	hidden, err := storage.CreateObject(StreamingObject{
		ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "dir/.keep"},
		Content:     noopSeekCloser{strings.NewReader("")},
	}, NoConditions{})
	if err != nil {
		t.Fatal(err)
	}
	hidden.Close()

	// Leftover temporary files from a killed process, ours and those of earlier
	// versions (".<name><random>"), must not break listing.
	for _, name := range []string{tempObjectPrefix + "123", ".object8129590202668251472"} {
		leftover := filepath.Join(rootDir, "bucket", "dir", name)
		if err := os.WriteFile(leftover, []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	objs, err := storage.ListObjects("bucket", "", false)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, o := range objs {
		names = append(names, o.Name)
	}
	if diff := cmp.Diff([]string{"dir/.keep", "dir/object"}, names); diff != "" {
		t.Errorf("wrong objects listed (-want +got):\n%s", diff)
	}
	entries, err := os.ReadDir(filepath.Join(rootDir, "bucket", "dir"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), tempObjectPrefix) && e.Name() != tempObjectPrefix+"123" {
			t.Errorf("temporary file left behind: %s", e.Name())
		}
	}
}

func TestCreateObjectDoesNotBlockWhileReadingContent(t *testing.T) {
	t.Parallel()

	storage, err := NewStorageFS(nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.CreateBucket("bucket", BucketAttrs{}); err != nil {
		t.Fatal(err)
	}

	// An upload whose content doesn't arrive until the end of the test.
	pr, pw := io.Pipe()
	reading := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		obj, err := storage.CreateObject(StreamingObject{
			ObjectAttrs: ObjectAttrs{BucketName: "bucket", Name: "slow"},
			Content:     &unseekable{ReadCloser: pr, reading: reading},
		}, NoConditions{})
		if err == nil {
			obj.Close()
		}
		done <- err
	}()

	<-reading
	listed := make(chan error, 1)
	go func() {
		_, err := storage.ListObjects("bucket", "", false)
		listed <- err
	}()
	select {
	case err := <-listed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListObjects blocked by an upload in progress")
	}

	pw.Write([]byte("content"))
	pw.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// unseekable closes reading when its content is first read.
type unseekable struct {
	io.ReadCloser
	reading chan struct{}
	once    sync.Once
}

func (u *unseekable) Read(p []byte) (int, error) {
	u.once.Do(func() { close(u.reading) })
	return u.ReadCloser.Read(p)
}

func (*unseekable) Seek(int64, int) (int64, error) { return 0, errors.New("not seekable") }
