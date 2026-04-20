/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"slices"
	"testing"
	"time"
)

func TestNewClient_NoAddr_IsNoOp(t *testing.T) {
	c, err := NewClient("", "pre.", []string{"a:b"})
	if err != nil {
		t.Fatal(err)
	}
	if c == nil || c.sd != nil {
		t.Fatalf("expected noop client without statsd connection")
	}
	c.Gauge("x", 1)
	c.Count("y", 2)
	c.Distribution("z", 3)
	c.Timing("t", time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestClient_mergeTags(t *testing.T) {
	tests := []struct {
		name        string
		defaultTags []string
		tags        []string
		want        []string
	}{
		{"nil_default", nil, []string{"k:v"}, []string{"k:v"}},
		{"empty_extra", []string{"env:prod"}, []string(nil), []string{"env:prod"}},
		{"combined", []string{"env:prod"}, []string{"quantile:0.5"}, []string{"env:prod", "quantile:0.5"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{defaultTags: tt.defaultTags}
			got := c.mergeTags(tt.tags)
			if !slices.Equal(got, tt.want) {
				t.Fatalf("got %#v want %#v", got, tt.want)
			}
		})
	}
}
