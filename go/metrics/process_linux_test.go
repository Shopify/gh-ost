//go:build linux

/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import "testing"

func TestParseKB(t *testing.T) {
	tests := []struct {
		line string
		want uint64
	}{
		{"VmRSS:  1234 kB", 1234 * 1024},
		{"VmSize:\t 8 kB", 8 * 1024},
		{"bad", 0},
		{"", 0},
	}
	for _, tt := range tests {
		if got := parseKB(tt.line); got != tt.want {
			t.Errorf("parseKB(%q) = %d want %d", tt.line, got, tt.want)
		}
	}
}

func TestReadMemory_Integration(t *testing.T) {
	rss, virt, err := readMemory()
	if err != nil {
		t.Fatalf("readMemory: %v", err)
	}
	if rss == 0 && virt == 0 {
		t.Fatal("expected non-zero RSS or VmSize on linux")
	}
	if _, err := countOpenFDs(); err != nil {
		t.Fatal(err)
	}
	secs, err := readCPUSeconds()
	if err != nil {
		t.Fatal(err)
	}
	// jiffies can still be zero in a fast test before the scheduler charges CPU time
	if secs < 0 {
		t.Fatalf("readCPUSeconds returned negative %v", secs)
	}
}
