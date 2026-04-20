// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
	_ "github.com/joho/godotenv/autoload"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/livekit/storage"
)

func TestAliOSS(t *testing.T) {
	key := os.Getenv("ALI_ACCESS_KEY")
	secret := os.Getenv("ALI_SECRET")
	endpoint := os.Getenv("ALI_ENDPOINT")
	bucket := os.Getenv("ALI_BUCKET")

	if key == "" || secret == "" || endpoint == "" || bucket == "" {
		t.Skip("Missing env vars")
	}

	s, err := storage.NewAliOSS(&storage.AliOSSConfig{
		AccessKey: key,
		Secret:    secret,
		Endpoint:  endpoint,
		Bucket:    bucket,
	})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestAzure(t *testing.T) {
	name := os.Getenv("AZURE_ACCOUNT_NAME")
	key := os.Getenv("AZURE_ACCOUNT_KEY")
	container := os.Getenv("AZURE_CONTAINER_NAME")

	if name == "" || key == "" || container == "" {
		t.Skip("Missing env vars")
	}

	s, err := storage.NewAzure(&storage.AzureConfig{
		AccountName:   name,
		AccountKey:    key,
		ContainerName: container,
	})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestGCP(t *testing.T) {
	creds := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	bucket := os.Getenv("GCP_BUCKET")

	if creds == "" || bucket == "" {
		t.Skip("Missing env vars")
	}

	s, err := storage.NewGCP(&storage.GCPConfig{
		CredentialsJSON: creds,
		Bucket:          bucket,
	})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestLocal(t *testing.T) {
	s, err := storage.NewLocal(&storage.LocalConfig{})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestOCI(t *testing.T) {
	key := os.Getenv("OCI_ACCESS_KEY")
	secret := os.Getenv("OCI_SECRET")
	region := os.Getenv("OCI_REGION")
	endpoint := os.Getenv("OCI_ENDPOINT")
	bucket := os.Getenv("OCI_BUCKET")

	if key == "" || secret == "" || region == "" || endpoint == "" || bucket == "" {
		t.Skip("Missing env vars")
	}

	s, err := storage.NewS3(&storage.S3Config{
		AccessKey:      key,
		Secret:         secret,
		Region:         region,
		Endpoint:       endpoint,
		Bucket:         bucket,
		ForcePathStyle: true,
	})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestSupabase(t *testing.T) {
	key := os.Getenv("SUPABASE_ACCESS_KEY")
	secret := os.Getenv("SUPABASE_SECRET")
	region := os.Getenv("SUPABASE_REGION")
	endpoint := os.Getenv("SUPABASE_ENDPOINT")
	bucket := os.Getenv("SUPABASE_BUCKET")

	if key == "" || secret == "" || region == "" || endpoint == "" || bucket == "" {
		t.Skip("Missing env vars")
	}

	s, err := storage.NewS3(&storage.S3Config{
		AccessKey:      key,
		Secret:         secret,
		Region:         region,
		Endpoint:       endpoint,
		Bucket:         bucket,
		ForcePathStyle: true,
	})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestS3(t *testing.T) {
	key := os.Getenv("AWS_ACCESS_KEY")
	secret := os.Getenv("AWS_SECRET")
	bucket := os.Getenv("S3_BUCKET")

	if key == "" || secret == "" || bucket == "" {
		t.Skip("Missing env vars")
	}

	s, err := storage.NewS3(&storage.S3Config{
		AccessKey:    key,
		Secret:       secret,
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		Region:       os.Getenv("AWS_REGION"),
		Bucket:       bucket,
	})
	require.NoError(t, err)

	testStorage(t, s)
}

func TestCustomHeadersYAML(t *testing.T) {
	want := storage.CustomHeaders{
		"x-amz-server-side-encryption": "AES256",
		"x-custom":                     "value",
	}

	cases := []struct {
		name string
		in   string
		want storage.CustomHeaders
	}{
		{
			name: "native map",
			in: `
custom_headers:
  x-amz-server-side-encryption: AES256
  x-custom: value
`,
			want: want,
		},
		{
			name: "quoted JSON string (env-var style)",
			in:   `custom_headers: '{"x-amz-server-side-encryption":"AES256","x-custom":"value"}'`,
			want: want,
		},
		{
			name: "inline JSON (YAML flow syntax, no quotes)",
			in:   `custom_headers: {"x-amz-server-side-encryption":"AES256","x-custom":"value"}`,
			want: want,
		},
		{
			name: "empty string",
			in:   `custom_headers: ""`,
			want: nil,
		},
		{
			name: "omitted",
			in:   `bucket: foo`,
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg storage.S3Config
			require.NoError(t, yaml.Unmarshal([]byte(tc.in), &cfg))
			require.Equal(t, tc.want, cfg.CustomHeaders)
		})
	}
}

func TestCustomHeadersJSON(t *testing.T) {
	want := storage.CustomHeaders{
		"x-amz-server-side-encryption": "AES256",
		"x-custom":                     "value",
	}

	cases := []struct {
		name string
		in   string
		want storage.CustomHeaders
	}{
		{
			name: "native object",
			in:   `{"custom_headers":{"x-amz-server-side-encryption":"AES256","x-custom":"value"}}`,
			want: want,
		},
		{
			name: "JSON-encoded string (env-var style)",
			in:   `{"custom_headers":"{\"x-amz-server-side-encryption\":\"AES256\",\"x-custom\":\"value\"}"}`,
			want: want,
		},
		{
			name: "empty string",
			in:   `{"custom_headers":""}`,
			want: nil,
		},
		{
			name: "omitted",
			in:   `{"bucket":"foo"}`,
			want: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var cfg storage.S3Config
			require.NoError(t, json.Unmarshal([]byte(tc.in), &cfg))
			require.Equal(t, tc.want, cfg.CustomHeaders)
		})
	}
}

func TestCustomHeadersInvalid(t *testing.T) {
	t.Run("yaml: malformed JSON string", func(t *testing.T) {
		var cfg storage.S3Config
		err := yaml.Unmarshal([]byte(`custom_headers: '{not json}'`), &cfg)
		require.Error(t, err)
	})

	t.Run("json: malformed JSON string", func(t *testing.T) {
		var cfg storage.S3Config
		err := json.Unmarshal([]byte(`{"custom_headers":"{not json}"}`), &cfg)
		require.Error(t, err)
	})
}

func TestCustomHeadersReserved(t *testing.T) {
	reserved := []string{
		"Authorization",
		"authorization",
		"X-Amz-Date",
		"x-amz-content-sha256",
		"X-Amz-Security-Token",
		"Host",
		"Content-Length",
		"Content-MD5",
	}
	for _, name := range reserved {
		t.Run("yaml:"+name, func(t *testing.T) {
			var cfg storage.S3Config
			in := fmt.Sprintf("custom_headers:\n  %s: value\n", name)
			err := yaml.Unmarshal([]byte(in), &cfg)
			require.ErrorContains(t, err, "reserved")
		})
		t.Run("json:"+name, func(t *testing.T) {
			var cfg storage.S3Config
			in := fmt.Sprintf(`{"custom_headers":{%q:"value"}}`, name)
			err := json.Unmarshal([]byte(in), &cfg)
			require.ErrorContains(t, err, "reserved")
		})
		t.Run("NewS3:"+name, func(t *testing.T) {
			_, err := storage.NewS3(&storage.S3Config{
				AccessKey:     "test",
				Secret:        "test",
				Region:        "us-east-1",
				Bucket:        "b",
				CustomHeaders: storage.CustomHeaders{name: "value"},
			})
			require.ErrorContains(t, err, "reserved")
		})
	}
}

func TestCustomHeadersMalformed(t *testing.T) {
	cases := []struct {
		name    string
		headers storage.CustomHeaders
		msg     string
	}{
		{"empty name", storage.CustomHeaders{"": "v"}, "invalid header name"},
		{"space in name", storage.CustomHeaders{"bad name": "v"}, "invalid header name"},
		{"newline in name", storage.CustomHeaders{"bad\nname": "v"}, "invalid header name"},
		{"CR in value", storage.CustomHeaders{"X-Ok": "a\rb"}, "invalid value"},
		{"LF in value", storage.CustomHeaders{"X-Ok": "a\nb"}, "invalid value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := storage.NewS3(&storage.S3Config{
				AccessKey:     "test",
				Secret:        "test",
				Region:        "us-east-1",
				Bucket:        "b",
				CustomHeaders: tc.headers,
			})
			require.ErrorContains(t, err, tc.msg)
		})
	}
}

func TestS3CustomHeaders(t *testing.T) {
	const bucket = "test-bucket"

	backend := s3mem.New()
	require.NoError(t, backend.CreateBucket(bucket))
	faker := gofakes3.New(backend)

	var (
		mu       sync.Mutex
		captured []http.Header
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		captured = append(captured, r.Header.Clone())
		mu.Unlock()
		faker.Server().ServeHTTP(w, r)
	}))
	defer ts.Close()

	customHeaders := storage.CustomHeaders{
		"x-amz-server-side-encryption": "AES256",
		"x-custom-test":                "hello",
	}

	s, err := storage.NewS3(&storage.S3Config{
		AccessKey:      "test",
		Secret:         "test",
		Region:         "us-east-1",
		Endpoint:       ts.URL,
		Bucket:         bucket,
		ForcePathStyle: true,
		CustomHeaders:  customHeaders,
	})
	require.NoError(t, err)

	testStorage(t, s)

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, captured, "no requests captured")
	for i, h := range captured {
		for k, v := range customHeaders {
			require.Equal(t, v, h.Get(k), "request %d missing %q", i, k)
		}
	}
}

func testStorage(t *testing.T, s storage.Storage) {
	storagePath := fmt.Sprintf("test-%s.txt", time.Now().Format("01-02-15-04"))
	data := []byte("hello world")

	// upload
	url, size, err := s.UploadData(data, storagePath, "text/plain")
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), size)
	require.NotEmpty(t, url)

	// list
	items, err := s.ListObjects("test")
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.True(t, strings.HasSuffix(items[0], storagePath))

	// download
	downloaded, err := s.DownloadData(storagePath)
	require.NoError(t, err)
	require.Equal(t, data, downloaded)

	// delete
	err = s.DeleteObject(storagePath)
	require.NoError(t, err)
}
