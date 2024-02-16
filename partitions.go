/* Copyright 2020 Victor Penso

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
        "io/ioutil"
        "os/exec"
        "log"
        "strings"
        "strconv"
        "github.com/prometheus/client_golang/prometheus"
)

func PartitionsData() []byte {
        cmd := exec.Command("sinfo", "-a", "-h", "-o%R,%C,%F")
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

func PartitionsPendingJobsData() []byte {
        cmd := exec.Command("squeue","-a","-r","-h","-o%P","--states=PENDING")
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

type PartitionMetrics struct {
        allocated float64
        idle float64
        other float64
        pending float64
        total float64
        nodes_allocated float64
        nodes_idle float64
        nodes_other float64
        nodes_total float64
}

func ParsePartitionsMetrics() map[string]*PartitionMetrics {
        partitions := make(map[string]*PartitionMetrics)
        lines := strings.Split(string(PartitionsData()), "\n")
        for _, line := range lines {
                if strings.Contains(line,",") {
                        // name of a partition
                        partition := strings.Split(line,",")[0]
                        _,key := partitions[partition]
                        if !key {
                                partitions[partition] = &PartitionMetrics{0,0,0,0,0,0,0,0,0}
                        }
                        states := strings.Split(line,",")[1]
                        allocated,_ := strconv.ParseFloat(strings.Split(states,"/")[0],64)
                        idle,_ := strconv.ParseFloat(strings.Split(states,"/")[1],64)
                        other,_ := strconv.ParseFloat(strings.Split(states,"/")[2],64)
                        total,_ := strconv.ParseFloat(strings.Split(states,"/")[3],64)
                        partitions[partition].allocated = allocated
                        partitions[partition].idle = idle
                        partitions[partition].other = other
                        partitions[partition].total = total

                        nodes_states := strings.Split(line,",")[2]
                        nodes_allocated,_ := strconv.ParseFloat(strings.Split(nodes_states,"/")[0],64)
                        nodes_idle,_ := strconv.ParseFloat(strings.Split(nodes_states,"/")[1],64)
                        nodes_other,_ := strconv.ParseFloat(strings.Split(nodes_states,"/")[2],64)
                        nodes_total,_ := strconv.ParseFloat(strings.Split(nodes_states,"/")[3],64)
                        partitions[partition].nodes_allocated = nodes_allocated
                        partitions[partition].nodes_idle = nodes_idle
                        partitions[partition].nodes_other = nodes_other
                        partitions[partition].nodes_total = nodes_total

		}
        }
        // get list of pending jobs by partition name
        list := strings.Split(string(PartitionsPendingJobsData()),"\n")
        for _,partition := range list {
		// accumulate the number of pending jobs
		_,key := partitions[partition]
		if key {
			partitions[partition].pending += 1
                }
        }


        return partitions
}

type PartitionsCollector struct {
        allocated *prometheus.Desc
        idle *prometheus.Desc
        other *prometheus.Desc
        pending *prometheus.Desc
        total *prometheus.Desc
        nodes_allocated *prometheus.Desc
        nodes_idle *prometheus.Desc
        nodes_other *prometheus.Desc
        nodes_total *prometheus.Desc
}

func NewPartitionsCollector() *PartitionsCollector {
        labels := []string{"partition"}
        return &PartitionsCollector{
                allocated: prometheus.NewDesc("slurm_partition_cpus_allocated", "Allocated CPUs for partition", labels,nil),
		idle: prometheus.NewDesc("slurm_partition_cpus_idle", "Idle CPUs for partition", labels,nil),
		other: prometheus.NewDesc("slurm_partition_cpus_other", "Other CPUs for partition", labels,nil),
		pending: prometheus.NewDesc("slurm_partition_jobs_pending", "Pending jobs for partition", labels,nil),
		total: prometheus.NewDesc("slurm_partition_cpus_total", "Total CPUs for partition", labels,nil),
                nodes_allocated: prometheus.NewDesc("slurm_partition_nodes_allocated", "Allocated nodes for partition", labels,nil),
		nodes_idle: prometheus.NewDesc("slurm_partition_nodes_idle", "Idle nodes for partition", labels,nil),
		nodes_other: prometheus.NewDesc("slurm_partition_nodes_other", "Nodes in other states for partition", labels,nil),
		nodes_total: prometheus.NewDesc("slurm_partition_nodes_total", "Total number of nodes for partition", labels,nil),
        }
}

func (pc *PartitionsCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- pc.allocated
        ch <- pc.idle
        ch <- pc.other
        ch <- pc.pending
        ch <- pc.total
        ch <- pc.nodes_allocated
        ch <- pc.nodes_idle
        ch <- pc.nodes_other
        ch <- pc.nodes_total
}

func (pc *PartitionsCollector) Collect(ch chan<- prometheus.Metric) {
        pm := ParsePartitionsMetrics()
        for p := range pm {
		ch <- prometheus.MustNewConstMetric(pc.allocated, prometheus.GaugeValue, pm[p].allocated, p)
		ch <- prometheus.MustNewConstMetric(pc.idle, prometheus.GaugeValue, pm[p].idle, p)
		ch <- prometheus.MustNewConstMetric(pc.other, prometheus.GaugeValue, pm[p].other, p)
		ch <- prometheus.MustNewConstMetric(pc.pending, prometheus.GaugeValue, pm[p].pending, p)
		ch <- prometheus.MustNewConstMetric(pc.total, prometheus.GaugeValue, pm[p].total, p)
		ch <- prometheus.MustNewConstMetric(pc.nodes_allocated, prometheus.GaugeValue, pm[p].nodes_allocated, p)
		ch <- prometheus.MustNewConstMetric(pc.nodes_idle, prometheus.GaugeValue, pm[p].nodes_idle, p)
		ch <- prometheus.MustNewConstMetric(pc.nodes_other, prometheus.GaugeValue, pm[p].nodes_other, p)
		ch <- prometheus.MustNewConstMetric(pc.nodes_total, prometheus.GaugeValue, pm[p].nodes_total, p)
        }
}
