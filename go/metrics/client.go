/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/openark/golib/log"
)

// Client wraps a DogStatsD client, automatically applying a namespace prefix and
// default tags to every metric. All methods are no-ops when addr is empty.
type Client struct {
	sd          *statsd.Client
	defaultTags []string
}

// NewClient creates a metrics client. If addr is empty the client is a no-op and
// no connection is attempted.
func NewClient(addr, prefix string, defaultTags []string) (*Client, error) {
	if addr == "" {
		return &Client{}, nil
	}
	sd, err := statsd.New(addr,
		statsd.WithNamespace(prefix),
		statsd.WithoutTelemetry(),
		statsd.WithoutOriginDetection(),
		statsd.WithClientSideAggregation(),
		statsd.WithExtendedClientSideAggregation(),
		statsd.WithMaxSamplesPerContext(1_000),
		statsd.WithMaxBytesPerPayload(8_172),
		statsd.WithAggregationInterval(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	log.Infof("metrics: DogStatsD client connected to %s (prefix: %s)", addr, prefix)
	return &Client{sd: sd, defaultTags: defaultTags}, nil
}

func (c *Client) Gauge(name string, value float64, tags ...string) {
	if c.sd == nil {
		return
	}
	_ = c.sd.Gauge(name, value, c.mergeTags(tags), 1.0)
}

func (c *Client) Count(name string, value int64, tags ...string) {
	if c.sd == nil {
		return
	}
	_ = c.sd.Count(name, value, c.mergeTags(tags), 1.0)
}

func (c *Client) Distribution(name string, value float64, tags ...string) {
	if c.sd == nil {
		return
	}
	_ = c.sd.Distribution(name, value, c.mergeTags(tags), 1.0)
}

// Timing emits duration as a Distribution in milliseconds.
func (c *Client) Timing(name string, d time.Duration, tags ...string) {
	c.Distribution(name, float64(d.Milliseconds()), tags...)
}

func (c *Client) Close() error {
	if c.sd == nil {
		return nil
	}
	return c.sd.Close()
}

func (c *Client) mergeTags(tags []string) []string {
	if len(c.defaultTags) == 0 {
		return tags
	}
	if len(tags) == 0 {
		return c.defaultTags
	}
	merged := make([]string, 0, len(c.defaultTags)+len(tags))
	return append(append(merged, c.defaultTags...), tags...)
}
