package main

import (
	"encoding/json"
	"fmt"
	"io"
	"text/tabwriter"
	"time"
)

// report is the tool's output. GB and Gbit are decimal (1e9).
type report struct {
	Mode       string  `json:"mode"`
	Conns      int     `json:"conns"`
	Size       int     `json:"size"`
	Ring       int     `json:"ring"`
	DurationS  float64 `json:"duration_s"`
	SpinNS     int64   `json:"spin_ns"`
	GOMAXPROCS int     `json:"gomaxprocs"`

	Aggregate *rate    `json:"aggregate,omitempty"`
	PerConn   []rate   `json:"per_conn,omitempty"`
	Latency   *latency `json:"latency,omitempty"`
}

// rate is what the consumer read over a measured window.
type rate struct {
	Conn          int     `json:"conn"`
	Records       uint64  `json:"records"`
	Bytes         uint64  `json:"bytes"`
	Seconds       float64 `json:"seconds"`
	GBps          float64 `json:"gb_per_s"`
	Gbps          float64 `json:"gbit_per_s"`
	RecordsPerSec float64 `json:"records_per_s"`
}

// latency is a round-trip distribution in microseconds.
type latency struct {
	Samples int     `json:"samples"`
	P50     float64 `json:"p50_us"`
	P99     float64 `json:"p99_us"`
	P999    float64 `json:"p999_us"`
	Min     float64 `json:"min_us"`
	Mean    float64 `json:"mean_us"`
	Max     float64 `json:"max_us"`
}

func newReport(o options, gomaxprocs int) *report {
	return &report{
		Mode:       o.mode,
		Conns:      o.conns,
		Size:       o.size,
		Ring:       o.ring,
		DurationS:  o.duration.Seconds(),
		SpinNS:     int64(o.spin),
		GOMAXPROCS: gomaxprocs,
	}
}

func newRate(conn int, records, bytes uint64, d time.Duration) rate {
	r := rate{Conn: conn, Records: records, Bytes: bytes, Seconds: d.Seconds()}
	if d > 0 {
		r.GBps = float64(bytes) / d.Seconds() / 1e9
		r.Gbps = r.GBps * 8
		r.RecordsPerSec = float64(records) / d.Seconds()
	}
	return r
}

// addThroughput fills per-connection rates and the aggregate over the span
// from the earliest connection start to the latest connection end.
func (r *report) addThroughput(stats []connStats) {
	var records, bytes uint64
	var first, last time.Time
	for i, s := range stats {
		r.PerConn = append(r.PerConn, newRate(i, s.records, s.bytes, s.end.Sub(s.start)))
		if s.records == 0 {
			continue
		}
		records += s.records
		bytes += s.bytes
		if first.IsZero() || s.start.Before(first) {
			first = s.start
		}
		if s.end.After(last) {
			last = s.end
		}
	}
	agg := newRate(-1, records, bytes, last.Sub(first))
	r.Aggregate = &agg
}

// empty reports whether nothing was measured.
func (r *report) empty() bool {
	if r.Latency != nil {
		return r.Latency.Samples == 0
	}
	return r.Aggregate == nil || r.Aggregate.Records == 0 || r.Aggregate.GBps <= 0
}

func (r *report) writeJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

func (r *report) writeText(w io.Writer) error {
	fmt.Fprintf(w, "mode=%s conns=%d size=%d ring=%d duration=%gs spin=%v gomaxprocs=%d\n",
		r.Mode, r.Conns, r.Size, r.Ring, r.DurationS, time.Duration(r.SpinNS), r.GOMAXPROCS)
	if l := r.Latency; l != nil {
		_, err := fmt.Fprintf(w, "round trip: samples=%d p50=%.2fus p99=%.2fus p999=%.2fus min=%.2fus mean=%.2fus max=%.2fus\n",
			l.Samples, l.P50, l.P99, l.P999, l.Min, l.Mean, l.Max)
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "conn\trecords\tseconds\tGB/s\tGbps\trecords/s\t")
	row := func(name string, x rate) {
		fmt.Fprintf(tw, "%s\t%d\t%.3f\t%.2f\t%.1f\t%.0f\t\n", name, x.Records, x.Seconds, x.GBps, x.Gbps, x.RecordsPerSec)
	}
	for _, x := range r.PerConn {
		row(fmt.Sprint(x.Conn), x)
	}
	if r.Aggregate != nil {
		row("all", *r.Aggregate)
	}
	return tw.Flush()
}
