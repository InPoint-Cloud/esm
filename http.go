/*
Copyright 2016 Medcl (m AT medcl.net)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	log "github.com/InPoint-Cloud/esm/internal/log"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
)

// Get sends a GET request and returns the response with its body, the body is returned for any status code
func Get(url string, auth *Auth, proxy string) (*http.Response, string, []error) {
	client, err := getClient(proxy)
	if err != nil {
		return nil, "", []error{err}
	}
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, "", []error{err}
	}
	if auth != nil {
		if len(auth.ApiKey) > 0 {
			req.Header.Set("Authorization", "ApiKey "+auth.ApiKey)
		} else {
			req.SetBasicAuth(auth.User, auth.Pass)
		}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, "", []error{err}
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp, "", []error{err}
	}
	return resp, string(body), nil
}

func newDeleteRequest(client *http.Client, method, urlStr string) (*http.Request, error) {
	if method == "" {
		// We document that "" means "GET" for Request.Method, and people have
		// relied on that from NewRequest, so keep that working.
		// We still enforce validMethod for non-empty methods.
		method = "GET"
	}
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	req := &http.Request{
		Method:     method,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}
	return req, nil
}

// insecureTLS disables the verification of TLS certificates, set by --insecure
var insecureTLS bool

type clientKey struct {
	proxy    string
	insecure bool
}

// one client per proxy, source and target may use different proxies and are requested concurrently
var clients sync.Map

func getClient(proxy string) (*http.Client, error) {
	key := clientKey{proxy: proxy, insecure: insecureTLS}
	if c, ok := clients.Load(key); ok {
		return c.(*http.Client), nil
	}
	transport := &http.Transport{
		DisableKeepAlives:  true,
		DisableCompression: false,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: key.insecure,
		},
	}
	if len(proxy) > 0 {
		proxyUrl, err := url.Parse(proxy)
		if err != nil {
			return nil, err
		}
		transport.Proxy = http.ProxyURL(proxyUrl)
	}
	c, _ := clients.LoadOrStore(key, &http.Client{Transport: transport})
	return c.(*http.Client), nil
}

// HTTPStatusError is returned by Request when the server answers with a non-200 status
type HTTPStatusError struct {
	Code   int
	Length int64
	Body   string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("server error: code=%d, length=%d, info=", e.Code, e.Length) + e.Body
}

func Request(method string, loadUrl string, auth *Auth, body *bytes.Buffer, proxy string) (string, error) {

	client, err := getClient(proxy)
	if err != nil {
		log.Error(err)
		return "", err
	}

	var reqest *http.Request
	if body != nil {
		reqest, err = http.NewRequest(method, loadUrl, body)
	} else {
		reqest, err = newDeleteRequest(client, method, loadUrl)
	}

	if err != nil {
		log.Error(err)
		return "", err
	}

	if auth != nil {
		if len(auth.ApiKey) > 0 {
			reqest.Header.Set("Authorization", "ApiKey "+auth.ApiKey)
		} else {
			reqest.SetBasicAuth(auth.User, auth.Pass)
		}
	}

	reqest.Header.Set("Content-Type", "application/json")

	//enable gzip
	//reqest.Header.Set("Content-Encoding", "gzip")
	//GzipHandler(reqest)
	//

	resp, errs := client.Do(reqest)
	if errs != nil {
		log.Error(SubString(errs.Error(), 0, 500))
		return "", errs
	}

	if resp != nil && resp.Body != nil {
		//io.Copy(io.Discard, resp.Body)
		defer resp.Body.Close()
	}

	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return "", &HTTPStatusError{Code: resp.StatusCode, Length: resp.ContentLength, Body: string(b)}
	}

	respBody, err := io.ReadAll(resp.Body)

	//log.Error(SubString(string(respBody), 0, 500))

	if err != nil {
		log.Error(SubString(string(err.Error()), 0, 500))
		return string(respBody), err
	}

	if err != nil {
		return string(respBody), err
	}
	io.Copy(io.Discard, resp.Body)
	defer resp.Body.Close()
	return string(respBody), nil
}

func DecodeJson(jsonStream string, o interface{}) error {

	decoder := json.NewDecoder(strings.NewReader(jsonStream))
	// UseNumber causes the Decoder to unmarshal a number into an interface{} as a Number instead of as a float64.
	decoder.UseNumber()
	//decoder.

	if err := decoder.Decode(o); err != nil {
		fmt.Println("error:", err)
		return err
	}
	return nil
}
