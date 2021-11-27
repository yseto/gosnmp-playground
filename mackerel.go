package main

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	mackerel "github.com/mackerelio/mackerel-client-go"
	"github.com/sirupsen/logrus"
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
	hostId, err := initialForMackerel(collectParams, client)
	if err != nil {
		log.Fatal(err)
	}

	wg := sync.WaitGroup{}

	wg.Add(1)
	go ticker(ctx, &wg, hostId, collectParams)

	wg.Add(1)
	go sendTicker(ctx, &wg, client, hostId)
	wg.Wait()
}

func initialForMackerel(c *CollectParams, client *mackerel.Client) (*string, error) {
	log.Info("init for mackerel")

	idPath, err := c.hostIdPath()
	if err != nil {
		return nil, err
	}
	interfaces := []mackerel.Interface{
		mackerel.Interface{
			Name:          "main",
			IPv4Addresses: []string{c.target},
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
			Name:       c.target,
			Interfaces: interfaces,
		})
		if err != nil {
			return nil, err
		}
	} else {
		hostId, err = client.CreateHost(&mackerel.CreateHostParam{
			Name:       c.target,
			Interfaces: interfaces,
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

func transform(metrics []MetricsDutum) []*mackerel.MetricValue {
	now := time.Now().Unix()

	mkMetrics := make([]*mackerel.MetricValue, 0, len(metrics))
	// delta
	for _, metric := range metrics {
		var name string
		ifName := escapeInterfaceName(metric.IfName)
		if deltaValues[metric.Mib] {
			direction := "txBytes"
			if receiveDirection[metric.Mib] {
				direction = "rxBytes"
			}
			name := fmt.Sprintf("interface.%s.%s.delta", ifName, direction)
			metric.Value /= 60
		} else {
			name := fmt.Sprintf("custom.interface.%s.%s", metric.Mib, ifName)
		}
		mkMetrics = append(mkMetrics, &mackerel.MetricValue{
			Name:  name,
			Time:  now,
			Value: metric.Value,
		})

	}
	return mkMetrics
}

func (c *CollectParams) hostIdPath() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.id.txt", c.target)), nil
}

func (c *CollectParams) snapshotPath() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.json", c.target)), nil
}

func saveSnapshot(ssPath string, snapshot []MetricsDutum) error {
	file, _ := json.Marshal(snapshot)
	return os.WriteFile(ssPath, file, 0644)
}

func loadSnapshot(ssPath string) ([]MetricsDutum, error) {
	var snapshot []MetricsDutum
	if _, err := os.Stat(ssPath); err == nil {
		file, err := os.ReadFile(ssPath)
		if err != nil {
			return nil, err
		}
		json.Unmarshal(file, &snapshot)
	}
	return snapshot, nil
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
			err := innerTicker(ctx, hostId, collectParams)
			if err != nil {
				log.Warn(err)
			}
		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func innerTicker(ctx context.Context, hostId *string, collectParams *CollectParams) error {
	ssPath, err := collectParams.snapshotPath()
	if err != nil {
		return err
	}

	prevSnapshot, err := loadSnapshot(ssPath)
	if err != nil {
		return err
	}

	rawMetrics, err := collect(ctx, collectParams)
	if err != nil {
		return err
	}

	err = saveSnapshot(ssPath, rawMetrics)
	if err != nil {
		return err
	}

	metrics := make([]MetricsDutum, 0)
	for _, metric := range rawMetrics {
		prevValue := metric.Value
		for _, v := range prevSnapshot {
			if v.IfIndex == metric.IfIndex && v.Mib == metric.Mib {
				prevValue = v.Value
				break
			}
		}

		value := calcurateDiff(prevValue, metric.Value, overflowValue[metric.Mib])
		metrics = append(metrics, MetricsDutum{
			IfIndex: metric.IfIndex,
			Mib:     metric.Mib,
			Value:   value,
			IfName:  metric.IfName,
		})

		log.WithFields(logrus.Fields{
			"rawIfName": metric.IfName,
			"ifName":    escapeInterfaceName(metric.IfName),
			"mib":       metric.Mib,
			"value":     value,
		}).Debug()
	}

	mutex.Lock()
	buffers.PushBack(transform(metrics))
	mutex.Unlock()

	return nil
}

func sendTicker(ctx context.Context, wg *sync.WaitGroup, client *mackerel.Client, hostId *string) {
	t := time.NewTicker(500 * time.Millisecond)

	defer func() {
		log.Info("stopping...")
		t.Stop()
		wg.Done()
	}()

	for {
		select {
		case <-t.C:
			sendToMackerel(ctx, client, hostId)

		case <-ctx.Done():
			log.Warn("cancellation from context:", ctx.Err())
			return
		}
	}
}

func sendToMackerel(ctx context.Context, client *mackerel.Client, hostId *string) {
	if buffers.Len() == 0 {
		return
	}

	e := buffers.Front()
	// log.Infof("send current value: %#v", e.Value)
	// log.Infof("buffers len: %d", buffers.Len())

	err := client.PostHostMetricValuesByHostID(*hostId, e.Value.([](*mackerel.MetricValue)))
	if err != nil {
		log.Warn(err)
		return
	} else {
		log.Info("success")
	}
	mutex.Lock()
	buffers.Remove(e)
	mutex.Unlock()
}
