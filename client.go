package godruid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	DefaultEndPoint             = "/druid/v2"
	DEFAULT_MaxIdleConns        = 1000
	DEFAULT_MaxIdleConnsPerHost = 300
	DEFAULT_TIMEOUT             = 60
)

// reuse http client to reuse tcp connections.
var httpClient *http.Client

func init() {
	// Customize the Transport to have larger connection pool
	defaultRoundTripper := http.DefaultTransport
	defaultTransportPointer, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic("defaultRoundTripper not an *http.Transport")
	}
	defaultTransport := *defaultTransportPointer // dereference it to get a copy of the struct that the pointer points to
	defaultTransport.MaxIdleConns = DEFAULT_MaxIdleConns
	defaultTransport.MaxIdleConnsPerHost = DEFAULT_MaxIdleConnsPerHost
	httpClient = &http.Client{Transport: &defaultTransport, Timeout: DEFAULT_TIMEOUT * time.Second}
}

type Client struct {
	Url      string
	EndPoint string
	Timeout  time.Duration

	Debug                bool
	UseDefaultHTTPClient bool
	LastRequest          string
	LastResponse         string
}

func (c *Client) Query(query Query) (err error) {
	query.setup()
	var reqJson []byte
	if c.Debug {
		reqJson, err = json.MarshalIndent(query, "", "  ")
	} else {
		reqJson, err = json.Marshal(query)
	}
	if err != nil {
		return
	}
	result, err := c.QueryRaw(reqJson)
	if err != nil {
		return
	}

	return query.onResponse(result)
}

func (c *Client) QueryRaw(req []byte) (result []byte, err error) {
	if c.EndPoint == "" {
		c.EndPoint = DefaultEndPoint
	}
	endPoint := c.EndPoint
	if c.Debug {
		endPoint += "?pretty"
		c.LastRequest = string(req)
	}
	if err != nil {
		return
	}

	if !c.UseDefaultHTTPClient {
		// By default, use 60 second timeout unless specified otherwise
		// by the caller
		clientTimeout := DEFAULT_TIMEOUT * time.Second
		if c.Timeout != 0 {
			clientTimeout = c.Timeout
		}

		httpClient = &http.Client{
			Timeout: clientTimeout,
		}
	}

	resp, err := httpClient.Post(c.Url+endPoint, "application/json", bytes.NewBuffer(req))
	if err != nil {
		return
	}
	defer func() {
		resp.Body.Close()
	}()

	result, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	if c.Debug {
		c.LastResponse = string(result)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(result))
	}

	return
}
