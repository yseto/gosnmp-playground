package main

import (
	"container/list"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	mackerel "github.com/mackerelio/mackerel-client-go"
)

var buffers = list.New()
var mutex = &sync.Mutex{}

var graphDefs = []*mackerel.GraphDefsParam{
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifInDiscards",
		Unit:        "integer",
		DisplayName: "In Discards",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifInDiscards.*",
				DisplayName: "%1",
			},
		},
	},
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifOutDiscards",
		Unit:        "integer",
		DisplayName: "Out Discards",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifOutDiscards.*",
				DisplayName: "%1",
			},
		},
	},
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifInErrors",
		Unit:        "integer",
		DisplayName: "In Errors",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifInErrors.*",
				DisplayName: "%1",
			},
		},
	},
	&mackerel.GraphDefsParam{
		Name:        "custom.interface.ifOutErrors",
		Unit:        "integer",
		DisplayName: "Out Errors",
		Metrics: []*mackerel.GraphDefsMetric{
			&mackerel.GraphDefsMetric{
				Name:        "custom.interface.ifOutErrors.*",
				DisplayName: "%1",
			},
		},
	},
}

var receiveDirection = map[string]bool{
	"ifInOctets":   true,
	"ifHCInOctets": true,
	"ifInDiscards": true,
	"ifInErrors":   true,
}

func runMackerel(ctx context.Context, collectParams *CollectParams) {
	client := mackerel.NewClient(apikey)
	hostId, err := initialForMackerel(collectParams.target, client)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go ticker(ctx, &wg, hostId, collectParams)

	wg.Add(1)
	go sendTicker(ctx, &wg)
	wg.Wait()
}

func initialForMackerel(target string, client *mackerel.Client) (*string, error) {

	log.Info("init for mackerel")

	idPath, err := hostIdPath(target)
	if err != nil {
		return nil, err
	}
	mkInterfaces := []mackerel.Interface{
		mackerel.Interface{
			Name:          "main",
			IPv4Addresses: []string{target},
		},
	}
	var hostId string
	if _, err := os.Stat(idPath); err == nil {
		bytes, err := os.ReadFile(idPath)
		if err != nil {
			return nil, err
		}
		hostId = string(bytes)
		_, err = client.UpdateHost(hostId, &mackerel.UpdateHostParam{
			Name:       target,
			Interfaces: mkInterfaces,
		})
		if err != nil {
			return nil, err
		}
	} else {
		hostId, err = client.CreateHost(&mackerel.CreateHostParam{
			Name:       target,
			Interfaces: mkInterfaces,
		})
		if err != nil {
			return nil, err
		}
		err = os.WriteFile(idPath, []byte(hostId), 0666)
		if err != nil {
			return nil, err
		}
	}
	err = client.CreateGraphDefs(graphDefs)
	if err != nil {
		return nil, err
	}
	return &hostId, nil
}

func transform(hostId *string, metrics []MetricsDutum) []*mackerel.HostMetricValue {
	now := time.Now().Unix()

	mkMetrics := make([]*mackerel.HostMetricValue, 0, len(metrics))
	for _, metric := range metrics {
		if metric.delta {
			direction := "txBytes"
			if receiveDirection[metric.name] {
				direction = "rxBytes"
			}
			mkName := fmt.Sprintf("interface.%s.%s.delta", escapeInterfaceName(metric.ifName), direction)
			mkMetrics = append(mkMetrics, &mackerel.HostMetricValue{
				HostID: *hostId,
				MetricValue: &mackerel.MetricValue{
					Name:  mkName,
					Time:  now,
					Value: (metric.value / 60),
				},
			})
		} else {
			mkName := fmt.Sprintf("custom.interface.%s.%s", metric.name, escapeInterfaceName(metric.ifName))
			mkMetrics = append(mkMetrics, &mackerel.HostMetricValue{
				HostID: *hostId,
				MetricValue: &mackerel.MetricValue{
					Name:  mkName,
					Time:  now,
					Value: metric.value,
				},
			})
		}
	}
	return mkMetrics
}

func hostIdPath(target string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.id.txt", target)), nil
}

func snapshotPath(target string) (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.txt", target)), nil
}

func saveSnapshot(ssPath string, snapshot []SnapshotDutum) error {
	file, err := os.Create(ssPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	records := make([][]string, 0)

	for _, v := range snapshot {
		records = append(records, []string{strconv.FormatUint(v.ifIndex, 10), v.key, strconv.FormatUint(v.value, 10)})
	}

	err = writer.WriteAll(records)
	if err != nil {
		return err
	}

	if err := writer.Error(); err != nil {
		return err
	}
	return nil
}

func loadSnapshot(ssPath string) ([]SnapshotDutum, error) {
	file, err := os.Open(ssPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data := make([]SnapshotDutum, 0)
	reader := csv.NewReader(file)
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ifIndex, err := strconv.ParseUint(line[0], 10, 64)
		if err != nil {
			return nil, err
		}
		value, err := strconv.ParseUint(line[2], 10, 64)
		if err != nil {
			return nil, err
		}
		data = append(data, SnapshotDutum{ifIndex: ifIndex, key: line[1], value: value})
	}
	return data, nil
}

func ticker(ctx context.Context, wg *sync.WaitGroup, hostId *string, collectParams *CollectParams) {
	t := time.NewTicker(1 * time.Minute)
	defer func() {
		log.Info("stopping...")
		t.Stop()
		wg.Done()
	}()

	for {
		select {
		case <-t.C:
			metrics, err := collect(ctx, collectParams)
			if err != nil {
				log.Warn(err)
			}

			mkMetrics := transform(hostId, metrics)
			mutex.Lock()
			buffers.PushBack(mkMetrics)
			mutex.Unlock()

		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func sendTicker(ctx context.Context, wg *sync.WaitGroup) {
	t := time.NewTicker(500 * time.Millisecond)
	client := mackerel.NewClient(apikey)

	defer func() {
		log.Info("stopping...")
		t.Stop()
		wg.Done()
	}()

	for {
		select {
		case <-t.C:
			send(ctx, client)

		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func send(ctx context.Context, client *mackerel.Client) {
	if buffers.Len() == 0 {
		return
	}

	e := buffers.Front()
	log.Infof("send current value: %#v\n", e.Value)
	log.Infof("buffers len: %d\n", buffers.Len())

	err := client.PostHostMetricValues(e.Value.([]*mackerel.HostMetricValue))
	if err != nil {
		log.Warn(err)
		return
	} else {
		log.Info("send mackerel")
	}
	mutex.Lock()
	buffers.Remove(e)
	mutex.Unlock()
}
