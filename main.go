package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gosnmp/gosnmp"
	"github.com/sirupsen/logrus"
)

const (
	MIBifNumber     = "1.3.6.1.2.1.2.1.0"
	MIBifDescr      = "1.3.6.1.2.1.2.2.1.2"
	MIBifOperStatus = "1.3.6.1.2.1.2.2.1.8"
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

type MetricsDutum struct {
	IfIndex uint64 `json:"ifIndex"`
	Mib     string `json:"mib"`
	IfName  string `json:"ifName"`
	Value   uint64 `json:"value"`
}

type CollectParams struct {
	community, target, name            string
	mibs                               []string
	includeRegexp, excludeRegexp       *regexp.Regexp
	includeInterface, excludeInterface *string
	skipDownLinkState                  *bool
}

var log = logrus.New()
var apikey = os.Getenv("MACKEREL_API_KEY")

func parseFlags() (*CollectParams, error) {
	var community, target, name string
	flag.StringVar(&community, "community", "public", "the community string for device")
	flag.StringVar(&target, "target", "127.0.0.1", "ip address")
	includeInterface := flag.String("include-interface", "", "include interface name")
	excludeInterface := flag.String("exclude-interface", "", "exclude interface name")
	rawMibs := flag.String("mibs", "all", "mib name joind with ',' or 'all'")
	level := flag.Bool("verbose", false, "verbose")
	skipDownLinkState := flag.Bool("skip-down-link-state", false, "skip down link state")
	flag.StringVar(&name, "name", "", "name")
	flag.Parse()

	logLevel := logrus.WarnLevel
	if *level {
		logLevel = logrus.DebugLevel
	}
	log.SetLevel(logLevel)

	if name == "" {
		name = target
	}

	if *includeInterface != "" && *excludeInterface != "" {
		return nil, errors.New("excludeInterface, includeInterface is exclusive control.")
	}
	includeReg, err := regexp.Compile(*includeInterface)
	if err != nil {
		return nil, err
	}
	excludeReg, err := regexp.Compile(*excludeInterface)
	if err != nil {
		return nil, err
	}

	mibs, err := mibsValidate(rawMibs)
	if err != nil {
		return nil, err
	}

	return &CollectParams{
		target:            target,
		name:              name,
		community:         community,
		mibs:              mibs,
		includeRegexp:     includeReg,
		excludeRegexp:     excludeReg,
		includeInterface:  includeInterface,
		excludeInterface:  excludeInterface,
		skipDownLinkState: skipDownLinkState,
	}, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})

	collectParams, err := parseFlags()
	if err != nil {
		log.Fatal(err)
	}

	log.Info("start")

	gosnmp.Default.Target = collectParams.target
	gosnmp.Default.Community = collectParams.community
	gosnmp.Default.Timeout = time.Duration(10 * time.Second)
	gosnmp.Default.Context = ctx

	if apikey == "" {
		log.SetLevel(logrus.DebugLevel)

		_, err := collect(ctx, collectParams)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		runMackerel(ctx, collectParams)
	}
}

func collect(ctx context.Context, c *CollectParams) ([]MetricsDutum, error) {
	err := gosnmp.Default.Connect()
	if err != nil {
		return nil, err
	}
	defer gosnmp.Default.Conn.Close()

	ifNumber, err := getInterfaceNumber()
	if err != nil {
		return nil, err
	}
	ifDescr, err := bulkWalkGetInterfaceName(ifNumber)
	if err != nil {
		return nil, err
	}

	var ifOperStatus map[uint64]bool
	if *c.skipDownLinkState {
		ifOperStatus, err = bulkWalkGetInterfaceState(ifNumber)
		if err != nil {
			return nil, err
		}
	}

	metrics := make([]MetricsDutum, 0)

	for _, mib := range c.mibs {
		values, err := bulkWalk(mibOidmapping[mib], ifNumber)
		if err != nil {
			return nil, err
		}

		for ifIndex, value := range values {
			ifName := ifDescr[ifIndex]
			if *c.includeInterface != "" && !c.includeRegexp.MatchString(ifName) {
				continue
			}

			if *c.excludeInterface != "" && c.excludeRegexp.MatchString(ifName) {
				continue
			}

			// skip when down(2)
			if *c.skipDownLinkState && !ifOperStatus[ifIndex] {
				continue
			}

			log.WithFields(logrus.Fields{
				"IfIndex": ifIndex,
				"Mib":     mib,
				"IfName":  ifName,
				"Value":   value,
			}).Debug()

			metrics = append(metrics, MetricsDutum{IfIndex: ifIndex, Mib: mib, IfName: ifName, Value: value})
		}
	}
	return metrics, nil
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

func bulkWalkGetInterfaceState(length uint64) (map[uint64]bool, error) {
	kv := make(map[uint64]bool, length)
	err := gosnmp.Default.BulkWalk(MIBifOperStatus, func(pdu gosnmp.SnmpPDU) error {
		index, err := captureIfIndex(MIBifOperStatus, pdu.Name)
		if err != nil {
			return err
		}
		switch pdu.Type {
		case gosnmp.OctetString:
			return errors.New("cant parse value.")
		default:
			tmp := gosnmp.ToBigInt(pdu.Value).Uint64()
			if tmp != 2 {
				kv[index] = true
			} else {
				kv[index] = false
			}
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
