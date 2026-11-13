package main

import (
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	var lat []time.Duration
	for i := 1; i <= 100; i++ {
		lat = append(lat, time.Duration(i)*time.Millisecond)
	}
	for p, want := range map[float64]float64{0.50: 50, 0.90: 90, 0.99: 99, 1.0: 100} {
		if got := percentileMs(lat, p); got != want {
			t.Errorf("p%.0f = %v, want %v", p*100, got, want)
		}
	}
	if percentileMs(nil, 0.5) != 0 {
		t.Error("empty input should give 0")
	}
}

func TestSummarizeCountsStatuses(t *testing.T) {
	o := options{targets: []string{"x"}, conc: 1, duration: 2 * time.Second, series: true}
	samples := []sample{
		{at: 100 * time.Millisecond, latency: time.Millisecond, status: 200},
		{at: 1500 * time.Millisecond, latency: time.Millisecond, status: 200},
		{at: 1600 * time.Millisecond, latency: time.Millisecond, status: 429},
		{at: 1700 * time.Millisecond, latency: time.Millisecond, status: 0},
	}
	r := summarize(o, samples, 2*time.Second)
	if r.Status["200"] != 2 || r.Status["429"] != 1 || r.Errors != 1 {
		t.Errorf("status counts wrong: %+v errors=%d", r.Status, r.Errors)
	}
	if r.AllowedRPS != 1 || r.ThroughputRPS != 2 {
		t.Errorf("rates wrong: allowed=%v throughput=%v", r.AllowedRPS, r.ThroughputRPS)
	}
	if len(r.AllowedPerSec) != 2 || r.AllowedPerSec[0] != 1 || r.AllowedPerSec[1] != 1 {
		t.Errorf("series wrong: %v", r.AllowedPerSec)
	}
}
