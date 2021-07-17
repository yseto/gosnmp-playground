package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
)

const (
	MIBifNumber = "1.3.6.1.2.1.2.1.0"
	MIBifIndex  = "1.3.6.1.2.1.2.2.1.1"
	MIBifDescr  = "1.3.6.1.2.1.2.2.1.2"
)

var mibOidmapping = map[string]string{
	"ifInOctets":    "1.3.6.1.2.1.2.2.1.10",
	"ifOutOctets":   "1.3.6.1.2.1.2.2.1.16",
	"ifHCInOctets":  "1.3.6.1.2.1.31.1.1.1.6",
	"ifHCOutOctets": "1.3.6.1.2.1.31.1.1.1.10",
	"ifInDiscards":  "1.3.6.1.2.1.2.2.1.13",
	"ifOutDiscards": "1.3.6.1.2.1.2.2.1.19",
	"ifInErrors":    "1.3.6.1.2.1.2.2.1.14",
	"ifOutErrors":   "1.3.6.1.2.1.2.2.1.20",
}

var overflowValue = map[string]uint64{
	"ifInOctets":    math.MaxInt32,
	"ifOutOctets":   math.MaxInt32,
	"ifHCInOctets":  math.MaxInt64,
	"ifHCOutOctets": math.MaxInt64,
	"ifInDiscards":  math.MaxInt64,
	"ifOutDiscards": math.MaxInt64,
	"ifInErrors":    math.MaxInt64,
	"ifOutErrors":   math.MaxInt64,
}

var deltaValues = map[string]bool{
	"ifInOctets":    true,
	"ifOutOctets":   true,
	"ifHCInOctets":  true,
	"ifHCOutOctets": true,
}

type ResultWithName struct {
	name  string
	value map[uint64]uint64
}

type SnapshotDutum struct {
	ifIndex uint64
	key     string
	value   uint64
}

func main() {
	var community, target string
	flag.StringVar(&community, "community", "public", "the community string for device")
	flag.StringVar(&target, "target", "127.0.0.1", "ip address")
	includeInterface := flag.String("include-interface", "", "include interface name")
	excludeInterface := flag.String("exclude-interface", "", "exclude interface name")
	rawMibs := flag.String("mibs", "all", "mib name joind with ',' or 'all'")
	flag.Parse()

	// TODO rule.
	if *includeInterface != "" && *excludeInterface != "" {
		log.Fatal("excludeInterface, includeInterface is exclusive control.")
	}
	includeReg, err := regexp.Compile(*includeInterface)
	if err != nil {
		log.Fatal(err)
	}
	excludeReg, err := regexp.Compile(*excludeInterface)
	if err != nil {
		log.Fatal(err)
	}

	mibs, err := mibsValidate(rawMibs)
	if err != nil {
		log.Fatal(err)
	}

	ssPath, err := snapshotPath(target)
	if err != nil {
		log.Fatal(err)
	}

	gosnmp.Default.Target = target
	gosnmp.Default.Community = community
	gosnmp.Default.Timeout = time.Duration(10 * time.Second)
	err = gosnmp.Default.Connect()
	if err != nil {
		log.Fatal("Connect err: %v", err)
	}
	defer gosnmp.Default.Conn.Close()

	ifNumber, err := getInterfaceNumber()
	if err != nil {
		log.Fatal(err)
	}
	ifDescr, err := bulkWalkGetInterfaceName(ifNumber)
	if err != nil {
		log.Fatal(err)
	}
	ifIndex, err := bulkWalkGetInterfaceNumber(ifNumber)
	if err != nil {
		log.Fatal(err)
	}

	results := make([]ResultWithName, 0, len(mibs))
	for _, mib := range mibs {
		result, err := bulkWalk(mibOidmapping[mib], ifNumber)
		if err != nil {
			log.Fatal(err)
		}
		results = append(results, ResultWithName{name: mib, value: result})
	}

	var snapshotData []SnapshotDutum
	if _, err := os.Stat(ssPath); err == nil {
		snapshotData, err = loadSnapshot(ssPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	savedSnapshot := make([]SnapshotDutum, 0)

	for _, ifNum := range ifIndex {
		ifName := ifDescr[ifNum]
		if *includeInterface != "" && !includeReg.MatchString(ifName) {
			continue
		}

		if *excludeInterface != "" && excludeReg.MatchString(ifName) {
			continue
		}

		for _, rwn := range results {
			key := rwn.name
			value := rwn.value[ifNum]

			savedSnapshot = append(savedSnapshot, SnapshotDutum{ifIndex: ifNum, key: key, value: value})

			prevValue := value
			for _, k := range snapshotData {
				if k.ifIndex == ifNum && k.key == key {
					prevValue = k.value
					break
				}
			}
			value = calcurateDiff(prevValue, value, overflowValue[key])
			log.Printf("%s\t%s\t%d\n", escapeInterfaceName(ifName), key, value)
		}
	}
	err = saveSnapshot(ssPath, savedSnapshot)
	if err != nil {
		log.Fatal(err)
	}
}

func escapeInterfaceName(ifName string) string {
	return strings.Replace(ifName, ".", "_", -1)
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

func mibsValidate(rawMibs *string) ([]string, error) {
	var parseMibs []string
	switch *rawMibs {
	case "all":
		for key := range mibOidmapping {
			// skipped 32 bit octets.
			if key == "ifInOctets" || key == "ifOutOctets" {
				continue
			}
			parseMibs = append(parseMibs, key)
		}
	case "":
	default:
		for _, name := range strings.Split(*rawMibs, ",") {
			if _, exists := mibOidmapping[name]; !exists {
				return nil, fmt.Errorf("mib %s is not supported.", name)
			}
			parseMibs = append(parseMibs, name)
		}
	}
	return parseMibs, nil
}

func calcurateDiff(a, b, overflow uint64) uint64 {
	if b < a {
		return overflow - a + b
	} else {
		return b - a
	}
}

func captureIfIndex(oid, name string) (uint64, error) {
	indexStr := strings.Replace(name, "."+oid+".", "", 1)
	return strconv.ParseUint(indexStr, 10, 64)
}

func getInterfaceNumber() (uint64, error) {
	result, err := gosnmp.Default.Get([]string{MIBifNumber})
	if err != nil {
		return 0, err
	}
	variable := result.Variables[0]
	switch variable.Type {
	case gosnmp.OctetString:
		return 0, errors.New("cant get interface number")
	default:
		return gosnmp.ToBigInt(variable.Value).Uint64(), nil
	}
}

func bulkWalkGetInterfaceNumber(length uint64) ([]uint64, error) {
	kv := make([]uint64, 0, length)
	err := gosnmp.Default.BulkWalk(MIBifIndex, func(pdu gosnmp.SnmpPDU) error {
		switch pdu.Type {
		case gosnmp.OctetString:
			return errors.New("cant parse interface number.")
		default:
			kv = append(kv, gosnmp.ToBigInt(pdu.Value).Uint64())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(kv, func(i, j int) bool {
		return kv[i] < kv[j]
	})
	return kv, nil
}

func bulkWalkGetInterfaceName(length uint64) (map[uint64]string, error) {
	kv := make(map[uint64]string, length)
	err := gosnmp.Default.BulkWalk(MIBifDescr, func(pdu gosnmp.SnmpPDU) error {
		index, err := captureIfIndex(MIBifDescr, pdu.Name)
		if err != nil {
			return err
		}
		switch pdu.Type {
		case gosnmp.OctetString:
			kv[index] = string(pdu.Value.([]byte))
		default:
			return errors.New("cant parse interface name.")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func bulkWalk(oid string, length uint64) (map[uint64]uint64, error) {
	kv := make(map[uint64]uint64, length)
	err := gosnmp.Default.BulkWalk(oid, func(pdu gosnmp.SnmpPDU) error {
		index, err := captureIfIndex(oid, pdu.Name)
		if err != nil {
			return err
		}
		switch pdu.Type {
		case gosnmp.OctetString:
			return errors.New("cant parse value.")
		default:
			kv[index] = gosnmp.ToBigInt(pdu.Value).Uint64()
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return kv, nil
}
