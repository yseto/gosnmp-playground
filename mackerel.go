package main

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	mackerel "github.com/mackerelio/mackerel-client-go"
)

var buffers = list.New()
var mutex = &sync.Mutex{}
var snapshot []MetricsDutum

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

var overflowValue = map[string]uint64{
	"ifInOctets":    math.MaxUint32,
	"ifOutOctets":   math.MaxUint32,
	"ifHCInOctets":  math.MaxUint64,
	"ifHCOutOctets": math.MaxUint64,
	"ifInDiscards":  math.MaxUint64,
	"ifOutDiscards": math.MaxUint64,
	"ifInErrors":    math.MaxUint64,
	"ifOutErrors":   math.MaxUint64,
}

var receiveDirection = map[string]bool{
	"ifInOctets":   true,
	"ifHCInOctets": true,
	"ifInDiscards": true,
	"ifInErrors":   true,
}

var deltaValues = map[string]bool{
	"ifInOctets":    true,
	"ifOutOctets":   true,
	"ifHCInOctets":  true,
	"ifHCOutOctets": true,
}

func runMackerel(ctx context.Context, collectParams *CollectParams) {
	client := mackerel.NewClient(apikey)

	hostId, err := initialForMackerel(collectParams, client)
	if err != nil {
		log.Fatal(err)
	}

	snapshot, err = collect(ctx, collectParams)
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
			Name:       c.name,
			Interfaces: interfaces,
		})
		if err != nil {
			return nil, err
		}
	} else {
		hostId, err = client.CreateHost(&mackerel.CreateHostParam{
			Name:       c.name,
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

func (c *CollectParams) hostIdPath() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Join(wd, fmt.Sprintf("%s.id.txt", c.target)), nil
}

func ticker(ctx context.Context, wg *sync.WaitGroup, hostId *string, collectParams *CollectParams) {
	t := time.NewTicker(1 * time.Minute)
	defer func() {
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

func escapeInterfaceName(ifName string) string {
	return strings.Replace(strings.Replace(strings.Replace(ifName, "/", "-", -1), ".", "_", -1), " ", "", -1)
}

func calcurateDiff(a, b, overflow uint64) uint64 {
	if b < a {
		return overflow - a + b
	} else {
		return b - a
	}
}

func innerTicker(ctx context.Context, hostId *string, collectParams *CollectParams) error {
	rawMetrics, err := collect(ctx, collectParams)
	if err != nil {
		return err
	}

	prevSnapshot := snapshot
	snapshot = rawMetrics

	now := time.Now().Unix()

	metrics := make([]*mackerel.MetricValue, 0)
	for _, metric := range rawMetrics {
		prevValue := metric.Value
		for _, v := range prevSnapshot {
			if v.IfIndex == metric.IfIndex && v.Mib == metric.Mib {
				prevValue = v.Value
				break
			}
		}

		value := calcurateDiff(prevValue, metric.Value, overflowValue[metric.Mib])

		var name string
		ifName := escapeInterfaceName(metric.IfName)
		if deltaValues[metric.Mib] {
			direction := "txBytes"
			if receiveDirection[metric.Mib] {
				direction = "rxBytes"
			}
			name = fmt.Sprintf("interface.%s.%s.delta", ifName, direction)
			value /= 60
		} else {
			name = fmt.Sprintf("custom.interface.%s.%s", metric.Mib, ifName)
		}
		metrics = append(metrics, &mackerel.MetricValue{
			Name:  name,
			Time:  now,
			Value: value,
		})

	}

	mutex.Lock()
	buffers.PushBack(metrics)
	mutex.Unlock()

	return nil
}

func sendTicker(ctx context.Context, wg *sync.WaitGroup, client *mackerel.Client, hostId *string) {
	t := time.NewTicker(500 * time.Millisecond)

	defer func() {
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
