// Copyright 2024 LiveKit, Inc.
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

package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"golang.org/x/net/http/httpguts"
	"gopkg.in/yaml.v3"
)

type AliOSSConfig struct {
	AccessKey string `yaml:"access_key,omitempty"`
	Secret    string `yaml:"secret,omitempty"`
	Endpoint  string `yaml:"endpoint,omitempty"`
	Bucket    string `yaml:"bucket,omitempty"`
}

type AzureConfig struct {
	AccountName     string                 `yaml:"account_name,omitempty"` // (env AZURE_STORAGE_ACCOUNT)
	AccountKey      string                 `yaml:"account_key,omitempty"`  // (env AZURE_STORAGE_KEY)
	ContainerName   string                 `yaml:"container_name,omitempty"`
	TokenCredential azblob.TokenCredential `yaml:"-"` // required for presigned url generation
}

type GCPConfig struct {
	CredentialsJSON string       `yaml:"credentials_json,omitempty"` // (env GOOGLE_APPLICATION_CREDENTIALS)
	Bucket          string       `yaml:"bucket,omitempty"`
	ProxyConfig     *ProxyConfig `yaml:"proxy_config,omitempty"`
}

type LocalConfig struct {
	StorageDir string `yaml:"storage_dir,omitempty"`
}

type S3Config struct {
	AccessKey            string       `yaml:"access_key,omitempty"`
	Secret               string       `yaml:"secret,omitempty"`
	SessionToken         string       `yaml:"session_token,omitempty"`
	AssumeRoleArn        string       `yaml:"assume_role_arn,omitempty"`         // ARN of the role to assume for file upload. Egress will make an AssumeRole API call using the provided access_key and secret to assume that role
	AssumeRoleExternalId string       `yaml:"assume_role_external_id,omitempty"` // ExternalID to use when assuming role for upload
	Region               string       `yaml:"region,omitempty"`
	Endpoint             string       `yaml:"endpoint,omitempty"`
	Bucket               string       `yaml:"bucket,omitempty"`
	ForcePathStyle       bool         `yaml:"force_path_style,omitempty"`
	ProxyConfig          *ProxyConfig `yaml:"proxy_config,omitempty"`

	MaxRetries    int           `yaml:"max_retries,omitempty"`
	MaxRetryDelay time.Duration `yaml:"max_retry_delay,omitempty"`
	MinRetryDelay time.Duration `yaml:"min_retry_delay,omitempty"`

	Metadata           map[string]string `yaml:"metadata,omitempty"`
	Tagging            string            `yaml:"tagging,omitempty"`
	ContentDisposition string            `yaml:"content_disposition,omitempty"`

	// CustomHeaders are added to every S3 HTTP request. Accepts either a
	// native map or a JSON-encoded string (useful for configuration via a
	// single environment variable), e.g. `{"X-Amz-Tagging":"a=b"}`.
	//
	// Headers managed by the SDK or the SigV4 signer are rejected (see
	// reservedCustomHeaders). Values placed here are visible to the SDK's
	// request logger when logging is enabled — do not use for secrets.
	CustomHeaders CustomHeaders `yaml:"custom_headers,omitempty" json:"custom_headers,omitempty"`
}

type CustomHeaders map[string]string

// reservedCustomHeaders are headers that must not be overridden via
// CustomHeaders: they are either set by the SigV4 signer (and would be
// silently clobbered), computed by the HTTP client, or load-bearing for
// request routing and payload integrity.
var reservedCustomHeaders = map[string]struct{}{
	"Authorization":        {},
	"X-Amz-Date":           {},
	"X-Amz-Security-Token": {},
	"X-Amz-Content-Sha256": {},
	"Host":                 {},
	"Content-Length":       {},
	"Content-Md5":          {},
}

func validateCustomHeaders(h CustomHeaders) error {
	for k, v := range h {
		if k == "" || !httpguts.ValidHeaderFieldName(k) {
			return fmt.Errorf("custom_headers: invalid header name %q", k)
		}
		if !httpguts.ValidHeaderFieldValue(v) {
			return fmt.Errorf("custom_headers: invalid value for header %q", k)
		}
		if _, reserved := reservedCustomHeaders[http.CanonicalHeaderKey(k)]; reserved {
			return fmt.Errorf("custom_headers: %q is reserved and cannot be overridden", k)
		}
	}
	return nil
}

func (h *CustomHeaders) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.MappingNode {
		m := map[string]string{}
		if err := node.Decode(&m); err != nil {
			return err
		}
		if err := validateCustomHeaders(m); err != nil {
			return err
		}
		*h = m
		return nil
	}

	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("custom_headers must be a map or a JSON string: %w", err)
	}
	return h.parseJSONString(s)
}

func (h *CustomHeaders) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		m := map[string]string{}
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		if err := validateCustomHeaders(m); err != nil {
			return err
		}
		*h = m
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("custom_headers must be a JSON object or a JSON-encoded string: %w", err)
	}
	return h.parseJSONString(s)
}

func (h *CustomHeaders) parseJSONString(s string) error {
	if s == "" {
		*h = nil
		return nil
	}
	m := map[string]string{}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return fmt.Errorf("failed to parse custom_headers JSON string: %w", err)
	}
	if err := validateCustomHeaders(m); err != nil {
		return err
	}
	*h = m
	return nil
}

type ProxyConfig struct {
	Url      string `yaml:"url,omitempty"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}
