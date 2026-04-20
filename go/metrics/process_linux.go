//go:build linux

/*
   Copyright 2022 GitHub Inc.
	 See https://github.com/github/gh-ost/blob/master/LICENSE
*/

package metrics

import (
	"os"
	"strconv"
	"strings"
)

// newProcessEmitter returns a function that emits process metrics each time it is called:
// open file descriptors, resident/virtual memory, and cumulative CPU seconds (use rate in dashboards).
func newProcessEmitter(client *Client) func() {
	return func() {
		if fds, err := countOpenFDs(); err == nil {
			client.Gauge("go.process.open_fds", float64(fds))
		}
		if rss, virt, err := readMemory(); err == nil {
			client.Gauge("go.process.resident_memory_bytes", float64(rss))
			client.Gauge("go.process.virtual_memory_bytes", float64(virt))
		}
		if cpu, err := readCPUSeconds(); err == nil {
			client.Gauge("go.process.cpu_seconds_total", cpu)
		}
	}
}

func countOpenFDs() (int, error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}
	return len(entries), nil
}

// readMemory returns RSS and virtual memory in bytes from /proc/self/status.
func readMemory() (rss, virt uint64, err error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			rss = parseKB(line)
		} else if strings.HasPrefix(line, "VmSize:") {
			virt = parseKB(line)
		}
	}
	return rss, virt, nil
}

// parseKB parses a /proc/self/status line of the form "VmRSS:   1234 kB" into bytes.
func parseKB(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, _ := strconv.ParseUint(fields[1], 10, 64)
	return v * 1024
}

// readCPUSeconds returns total CPU time (user+system) in seconds from /proc/self/stat.
func readCPUSeconds() (float64, error) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, err
	}
	// Skip past the comm field "(name)" which may contain spaces.
	s := string(data)
	rparen := strings.LastIndex(s, ")")
	if rparen < 0 {
		return 0, nil
	}
	fields := strings.Fields(s[rparen+1:])
	// utime is field index 11 (0-based after comm+state), stime is 12.
	if len(fields) < 13 {
		return 0, nil
	}
	utime, _ := strconv.ParseFloat(fields[11], 64)
	stime, _ := strconv.ParseFloat(fields[12], 64)
	const clkTck = 100.0 // USER_HZ, standard on Linux
	return (utime + stime) / clkTck, nil
}
