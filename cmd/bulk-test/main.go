package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/bougou/go-ipmi"

	hist "github.com/VividCortex/gohistogram"
	"github.com/metricq/metricq-source-capella/pkg/command"
)

func startIPMISession(host string) (*ipmi.Client, error) {
	port := 623
	username := "admin"
	password := ""

	log.Printf("opening connection to %s", host)

	client, err := ipmi.NewClient(host, port, username, password)
	if err != nil {
		return nil, fmt.Errorf("failed to create IPMI Client: %w", err)
	}

	// when nothing else helps...
	// client.WithDebug(true)

	client.WithInterface(ipmi.InterfaceLanplus)
	client.WithCipherSuiteID(ipmi.CipherSuiteID17)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// we want to sample with 1Hz, but encounter missing packets from time to
	// time. So let's set a ridiculous low timeout.
	client.WithTimeout(900 * time.Millisecond)

	return client, nil
}

type Client struct {
	ipmi     *ipmi.Client
	nodeName string
}

func (client *Client) Close(ctx context.Context) {
	client.ipmi.Close(ctx)
}

func (client *Client) ReadExactSampleTime(ctx context.Context) (time.Duration, error) {
	response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 3, 0}, "")
	if err != nil {
		return 0, fmt.Errorf("failed to read exact sample time: %w", err)
	}

	if len(response.Response) != 4 {
		return 0, fmt.Errorf("failed to read exact sample time: unexpected buffer length: %v != 4", len(response.Response))
	}

	offset_ns := int64(binary.LittleEndian.Uint32(response.Response[0:4]))

	return time.Duration(offset_ns), nil
}

func (client *Client) RecalibrateSampleTime(ctx context.Context) error {
	response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 3, 2}, "")
	if err != nil {
		return fmt.Errorf("failed to recalibrate sample time: %w", err)
	}

	if len(response.Response) != 0 {
		return fmt.Errorf("failed to recalibrate sample time: unexpected buffer length: %v != 0", len(response.Response))
	}

	return nil
}

func (client *Client) ResetEnergyAccumulator(ctx context.Context) error {
	response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 3, 1}, "")
	if err != nil {
		return fmt.Errorf("failed to reset energy accumulator: %w", err)
	}

	if len(response.Response) != 1 && response.Response[0] != 0x0 {
		return fmt.Errorf("failed to reset energy accumulator: unexpected buffer length: %v != 0", len(response.Response))
	}

	return nil
}

func (client *Client) SinglePowerCommand(ctx context.Context) (command.PowerReading, error) {
	r := command.PowerReading{}

	slog.Debug("sending IPMI Raw Command", "node", client.nodeName, "command", "single power")

	response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 8, 0, 0, 0}, "")
	if err != nil {
		return r, fmt.Errorf("failed to execute single power command: %w", err)
	}

	err = r.Unpack(response.Response)
	if err != nil {
		return r, fmt.Errorf("failed to unpack single power command response: %w", err)
	}

	return r, nil
}

func (client *Client) SingleEnergyCommand(ctx context.Context) (command.EnergyReading, error) {
	r := command.EnergyReading{}

	slog.Debug("sending IPMI Raw Command", "node", client.nodeName, "command", "single energy")

	response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 2, 0, 0, 0}, "")
	if err != nil {
		return r, fmt.Errorf("failed to execute single energy command: %w", err)
	}

	err = r.Unpack(response.Response)
	if err != nil {
		return r, fmt.Errorf("failed to unpack single energy command response: %w", err)
	}

	return r, nil
}

func (client *Client) BulkEnergyCommand(ctx context.Context) (command.BulkEnergyReading, error) {
	r := command.BulkEnergyReading{}

	slog.Debug("sending IPMI Raw Command", "node", client.nodeName, "command", "bulk energy")

	response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 1, 0, 0, 0}, "")
	if err != nil {
		return r, fmt.Errorf("failed to execute bulk energy command: %w", err)
	}

	err = r.Unpack(response.Response)
	if err != nil {
		return r, fmt.Errorf("failed to unpack bulk energy command response: %w", err)
	}

	return r, nil
}

func (client *Client) Reconnect(ctx context.Context) error {
	// intentionally ignore the error for already closed
	client.ipmi.Close(ctx)

	session, err := startIPMISession(client.nodeName)
	if err != nil {
		return fmt.Errorf("failed to reconnect IPMI: %w", err)
	}

	client.ipmi = session

	return nil
}

const hostPattern string = "c%d.bmc.capella.hpc.tu-dresden.de"

func main() {
	hg := hist.NewHistogram(60)

	var verbosityFlag string
	flag.StringVar(&verbosityFlag, "verbosity", "WARN", "Sets the verbosity of logging. Allowed values: [DEBUG, INFO, WARN, ERROR]")
	flag.StringVar(&verbosityFlag, "V", "WARN", "Sets the verbosity of logging. (shorthand)")
	flag.Parse()

	level := new(slog.LevelVar)

	switch verbosityFlag {
	case "DEBUG":
		level.Set(slog.LevelDebug)
	case "INFO":
		level.Set(slog.LevelInfo)
	case "ERROR":
		level.Set(slog.LevelError)
	default:
		log.Printf("unknown input for verbosity: %v", verbosityFlag)
		fallthrough
	case "WARN":
		level.Set(slog.LevelWarn)
	}

	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	log.Printf("opening connection")

	connectionStart := time.Now()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clients := make(map[string]Client)
	connectionErrors := make(map[string]int)
	timeErrors := make(map[string]int)
	valueErrors := make(map[string]int)
	initMutex := sync.Mutex{}

	wg := sync.WaitGroup{}
	for i := 1; i <= 1; i++ {
		wg.Add(1)
		host := fmt.Sprintf(hostPattern, i)
		connectionErrors[host] = 0
		timeErrors[host] = 0
		valueErrors[host] = 0

		go func(i int, hostname string, ctx context.Context) {
			defer wg.Done()

			client, err := startIPMISession(hostname)

			if err != nil {
				log.Printf("Failed to connect to %s: %v", hostname, err)
				return
			}

			c := Client{
				client, fmt.Sprintf("c%d", i),
			}

			slog.Info("resetting energy counter", "node", c.nodeName)
			if err := c.ResetEnergyAccumulator(ctx); err != nil {
				slog.Warn(err.Error(), "node", c.nodeName)
			} else {
				slog.Info("successfully reset energy counter", "node", c.nodeName)
			}

			initMutex.Lock()
			defer initMutex.Unlock()

			clients[hostname] = c
		}(i, host, ctx)
	}

	wg.Wait()

	log.Printf("Establishing the connection took: %v\n\n", time.Now().Sub(connectionStart))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	wg = sync.WaitGroup{}

	const LOOP_INTERVAL = 1 * time.Millisecond

	for host, client := range clients {

		wg.Add(1)

		deadline := time.Now().Add(LOOP_INTERVAL)

		go func(host string, client Client, deadline time.Time) {
			defer wg.Done()
			defer client.Close(ctx)

			var lastBulkReading *command.BulkEnergyReading

			if reading, err := client.BulkEnergyCommand(ctx); err != nil {
                log.Fatal("failed first reading. Node not available?")
			} else {
				lastBulkReading = &reading
			}

		WorkerLoop:
			for {
                // start := time.Now()
				currentBulkReading, err := client.BulkEnergyCommand(ctx)
                // end := time.Now()

                //slog.Info(fmt.Sprintf("request took %v ms", end.Sub(start).Milliseconds()))

				if err != nil {
					slog.Error(fmt.Sprintf("failed to execute raw command: %v", err), "node", host, "command", "single energy")
					if err := client.Reconnect(ctx); err != nil {
						slog.Error(fmt.Sprintf("failed to reconnect IPMI: %v", err), "node", host)
					}
				} else if lastBulkReading != nil {
					if lastBulkReading.Timestamp() != currentBulkReading.Timestamp() {
						slog.Info(fmt.Sprintf("Reading update: %v Diff: %v ms", currentBulkReading.Timestamp(), currentBulkReading.Timestamp().Sub(lastBulkReading.Timestamp()).Milliseconds()), "node", host)
						hg.Add(float64(currentBulkReading.Timestamp().Sub(lastBulkReading.Timestamp()).Milliseconds()))

						lastBulkReading = &currentBulkReading
						fmt.Print(hg.String())
                        fmt.Printf("Mean: %v", hg.Mean())
					}
				}

				for ; deadline.Before(time.Now()); deadline = deadline.Add(LOOP_INTERVAL) {
				}

				select {
				case <-ctx.Done():
					break WorkerLoop
				case <-time.After(deadline.Sub(time.Now())):
					deadline = deadline.Add(LOOP_INTERVAL)
					continue
				}
			}
		}(host, client, deadline)

	}

	wg.Wait()
}
