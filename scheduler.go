/* Copyright 2017 Victor Penso, Matteo Dessalvi

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>. */

package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

/*
 * Execute the Slurm sdiag command to read the current statistics
 * from the Slurm scheduler. It will be repreatedly called by the
 * collector.
 */

// Basic metrics for the scheduler
type SchedulerMetrics struct {
	threads                           float64
	queue_size                        float64
	dbd_queue_size                    float64
	last_cycle                        float64
	mean_cycle                        float64
	cycle_per_minute                  float64
	backfill_last_cycle               float64
	backfill_mean_cycle               float64
	backfill_depth_mean               float64
	total_backfilled_jobs_since_start float64
	total_backfilled_jobs_since_cycle float64
	total_backfilled_heterogeneous    float64
	jobs_submitted                    float64
	jobs_started                      float64
	jobs_completed                    float64
	jobs_canceled                     float64
	jobs_failed                       float64
}

// Execute the sdiag command and return its output
func SchedulerData() []byte {
	cmd := exec.Command("sdiag")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	out, _ := ioutil.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		log.Fatal(err)
	}
	return out
}

// Extract the relevant metrics from the sdiag output
func ParseSchedulerMetrics(input []byte) *SchedulerMetrics {
	var sm SchedulerMetrics
	lines := strings.Split(string(input), "\n")
	// Guard variables to check for string repetitions in the output of sdiag
	// (two occurencies of the following strings: 'Last cycle', 'Mean cycle')
	lc_count := 0
	mc_count := 0
	for _, line := range lines {
		key, val, f := strings.Cut(line, ":")
		if f {
			floatval, _ := strconv.ParseFloat(strings.TrimSpace(val), 64)
			switch strings.TrimSpace(key) {
			case "Server thread count":
				sm.threads = floatval
			case "Agent queue size":
				sm.queue_size = floatval
			case "DBD Agent queue size":
				sm.dbd_queue_size = floatval
			case "Last cycle":
				if lc_count == 0 {
					sm.last_cycle = floatval
					lc_count = 1
				}
				if lc_count == 1 {
					sm.backfill_last_cycle = floatval
				}
			case "Mean cycle":
				if mc_count == 0 {
					sm.mean_cycle = floatval
					mc_count = 1
				}
				if mc_count == 1 {
					sm.backfill_mean_cycle = floatval
				}
			case "Cycles per minute":
				sm.cycle_per_minute = floatval
			case "Depth Mean":
				sm.backfill_depth_mean = floatval
			case "Total backfilled jobs (since last slurm start)":
				sm.total_backfilled_jobs_since_start = floatval
			case "Total backfilled jobs (since last stats cycle start)":
				sm.total_backfilled_jobs_since_cycle = floatval
			case "Total backfilled heterogeneous job components":
				sm.total_backfilled_heterogeneous = floatval
			case "Jobs submitted":
				sm.jobs_submitted = floatval
			case "Jobs started":
				sm.jobs_started = floatval
			case "Jobs completed":
				sm.jobs_completed = floatval
			case "Jobs canceled":
				sm.jobs_canceled = floatval
			case "Jobs failed":
				sm.jobs_failed = floatval
			}
		}
	}
	return &sm
}

// Returns the scheduler metrics
func SchedulerGetMetrics() *SchedulerMetrics {
	return ParseSchedulerMetrics(SchedulerData())
}

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

// Collector strcture
type SchedulerCollector struct {
	threads                           *prometheus.Desc
	queue_size                        *prometheus.Desc
	dbd_queue_size                    *prometheus.Desc
	last_cycle                        *prometheus.Desc
	mean_cycle                        *prometheus.Desc
	cycle_per_minute                  *prometheus.Desc
	backfill_last_cycle               *prometheus.Desc
	backfill_mean_cycle               *prometheus.Desc
	backfill_depth_mean               *prometheus.Desc
	total_backfilled_jobs_since_start *prometheus.Desc
	total_backfilled_jobs_since_cycle *prometheus.Desc
	total_backfilled_heterogeneous    *prometheus.Desc
	jobs_submitted                    *prometheus.Desc
	jobs_started                      *prometheus.Desc
	jobs_completed                    *prometheus.Desc
	jobs_canceled                     *prometheus.Desc
	jobs_failed                       *prometheus.Desc
}

// Send all metric descriptions
func (c *SchedulerCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.threads
	ch <- c.queue_size
	ch <- c.dbd_queue_size
	ch <- c.last_cycle
	ch <- c.mean_cycle
	ch <- c.cycle_per_minute
	ch <- c.backfill_last_cycle
	ch <- c.backfill_mean_cycle
	ch <- c.backfill_depth_mean
	ch <- c.total_backfilled_jobs_since_start
	ch <- c.total_backfilled_jobs_since_cycle
	ch <- c.total_backfilled_heterogeneous
	ch <- c.jobs_submitted
	ch <- c.jobs_started
	ch <- c.jobs_completed
	ch <- c.jobs_canceled
	ch <- c.jobs_failed
}

// Send the values of all metrics
func (sc *SchedulerCollector) Collect(ch chan<- prometheus.Metric) {
	sm := SchedulerGetMetrics()
	ch <- prometheus.MustNewConstMetric(sc.threads, prometheus.GaugeValue, sm.threads)
	ch <- prometheus.MustNewConstMetric(sc.queue_size, prometheus.GaugeValue, sm.queue_size)
	ch <- prometheus.MustNewConstMetric(sc.dbd_queue_size, prometheus.GaugeValue, sm.dbd_queue_size)
	ch <- prometheus.MustNewConstMetric(sc.last_cycle, prometheus.GaugeValue, sm.last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.mean_cycle, prometheus.GaugeValue, sm.mean_cycle)
	ch <- prometheus.MustNewConstMetric(sc.cycle_per_minute, prometheus.GaugeValue, sm.cycle_per_minute)
	ch <- prometheus.MustNewConstMetric(sc.backfill_last_cycle, prometheus.GaugeValue, sm.backfill_last_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_mean_cycle, prometheus.GaugeValue, sm.backfill_mean_cycle)
	ch <- prometheus.MustNewConstMetric(sc.backfill_depth_mean, prometheus.GaugeValue, sm.backfill_depth_mean)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_jobs_since_start, prometheus.GaugeValue, sm.total_backfilled_jobs_since_start)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_jobs_since_cycle, prometheus.GaugeValue, sm.total_backfilled_jobs_since_cycle)
	ch <- prometheus.MustNewConstMetric(sc.total_backfilled_heterogeneous, prometheus.GaugeValue, sm.total_backfilled_heterogeneous)
	ch <- prometheus.MustNewConstMetric(sc.jobs_submitted, prometheus.CounterValue, sm.jobs_submitted)
	ch <- prometheus.MustNewConstMetric(sc.jobs_started, prometheus.CounterValue, sm.jobs_started)
	ch <- prometheus.MustNewConstMetric(sc.jobs_completed, prometheus.CounterValue, sm.jobs_completed)
	ch <- prometheus.MustNewConstMetric(sc.jobs_canceled, prometheus.CounterValue, sm.jobs_canceled)
	ch <- prometheus.MustNewConstMetric(sc.jobs_failed, prometheus.CounterValue, sm.jobs_failed)
}

// Returns the Slurm scheduler collector, used to register with the prometheus client
func NewSchedulerCollector() *SchedulerCollector {
	return &SchedulerCollector{
		threads: prometheus.NewDesc(
			"slurm_scheduler_threads",
			"Information provided by the Slurm sdiag command, number of scheduler threads ",
			nil,
			nil),
		queue_size: prometheus.NewDesc(
			"slurm_scheduler_queue_size",
			"Information provided by the Slurm sdiag command, length of the scheduler queue",
			nil,
			nil),
		dbd_queue_size: prometheus.NewDesc(
			"slurm_scheduler_dbd_queue_size",
			"Information provided by the Slurm sdiag command, length of the DBD agent queue",
			nil,
			nil),
		last_cycle: prometheus.NewDesc(
			"slurm_scheduler_last_cycle",
			"Information provided by the Slurm sdiag command, scheduler last cycle time in (microseconds)",
			nil,
			nil),
		mean_cycle: prometheus.NewDesc(
			"slurm_scheduler_mean_cycle",
			"Information provided by the Slurm sdiag command, scheduler mean cycle time in (microseconds)",
			nil,
			nil),
		cycle_per_minute: prometheus.NewDesc(
			"slurm_scheduler_cycle_per_minute",
			"Information provided by the Slurm sdiag command, number scheduler cycles per minute",
			nil,
			nil),
		backfill_last_cycle: prometheus.NewDesc(
			"slurm_scheduler_backfill_last_cycle",
			"Information provided by the Slurm sdiag command, scheduler backfill last cycle time in (microseconds)",
			nil,
			nil),
		backfill_mean_cycle: prometheus.NewDesc(
			"slurm_scheduler_backfill_mean_cycle",
			"Information provided by the Slurm sdiag command, scheduler backfill mean cycle time in (microseconds)",
			nil,
			nil),
		backfill_depth_mean: prometheus.NewDesc(
			"slurm_scheduler_backfill_depth_mean",
			"Information provided by the Slurm sdiag command, scheduler backfill mean depth",
			nil,
			nil),
		total_backfilled_jobs_since_start: prometheus.NewDesc(
			"slurm_scheduler_backfilled_jobs_since_start_total",
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last slurm start",
			nil,
			nil),
		total_backfilled_jobs_since_cycle: prometheus.NewDesc(
			"slurm_scheduler_backfilled_jobs_since_cycle_total",
			"Information provided by the Slurm sdiag command, number of jobs started thanks to backfilling since last time stats where reset",
			nil,
			nil),
		total_backfilled_heterogeneous: prometheus.NewDesc(
			"slurm_scheduler_backfilled_heterogeneous_total",
			"Information provided by the Slurm sdiag command, number of heterogeneous job components started thanks to backfilling since last Slurm start",
			nil,
			nil),
		jobs_submitted: prometheus.NewDesc(
			"slurm_scheduler_jobs_submitted",
			"sdiag: Number of jobs submitted since last reset",
			nil,
			nil),
		jobs_started: prometheus.NewDesc(
			"slurm_scheduler_jobs_started",
			"sdiag: Number of jobs started sind last reset. This includes backfillled jobs.",
			nil,
			nil),
		jobs_completed: prometheus.NewDesc(
			"slurm_scheduler_jobs_completed",
			"sdiag: Number of jobs completed since last reset",
			nil,
			nil),
		jobs_canceled: prometheus.NewDesc(
			"slurm_scheduler_jobs_canceled",
			"sdiag: Number of jobs canceled since last reset",
			nil,
			nil),
		jobs_failed: prometheus.NewDesc(
			"slurm_scheduler_jobs_failed",
			"sdiag: Numer of jobs failed since last reset",
			nil,
			nil),
	}
}
