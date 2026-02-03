package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/bougou/go-ipmi"
	"github.com/getsentry/sentry-go"
	metricq "github.com/metricq/metricq-go"

	"github.com/metricq/metricq-source-capella/pkg/command"
)

type MetricQConfig struct {
	User     string `json:"user"`
	Password string `json:"password"`
}

func startIPMISession(ctx context.Context, host, username, password string) (*ipmi.Client, error) {
	port := 623

	client, err := ipmi.NewClient(host, port, username, password)
	if err != nil {
		return nil, fmt.Errorf("failed to create IPMI Client: %w", err)
	}

	// when nothing else helps...
	// client.WithDebug(true)

	client.WithInterface(ipmi.InterfaceLanplus)
	client.WithCipherSuiteID(ipmi.CipherSuiteID17)

	ctxTimeout, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := client.Connect(ctxTimeout); err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	// we want to sample with 1Hz, but encounter missing packets from time to
	// time. So let's set a ridiculous low timeout.
	client.WithTimeout(900 * time.Millisecond)

	return client, nil
}

type Client struct {
	ipmi       *ipmi.Client
	nodeName   string
	hostname   string
	user       string
	password   string
	cpu        *metricq.SourceMetric
	gpu        *metricq.SourceMetric
	node       *metricq.SourceMetric
	energy     *metricq.SourceMetric
	fromEnergy *metricq.SourceMetric
}

const cpuMetricPattern string = "hpc.capella.%s.cpu.power.acc"
const gpuMetricPattern string = "hpc.capella.%s.gpu.power.acc"
const nodeMetricPattern string = "hpc.capella.%s.power"
const energyPattern string = "hpc.capella.%s.energy"
const fromEnergyMetricPattern string = "hpc.capella.%s.power.fe"
const hostPattern string = "%s.bmc.capella.hpc.tu-dresden.de"

func NewClient(nodeName string, source *metricq.Source, username string, password string) Client {
	hostname := fmt.Sprintf(hostPattern, nodeName)

	cpu_metric := fmt.Sprintf(cpuMetricPattern, nodeName)
	gpu_metric := fmt.Sprintf(gpuMetricPattern, nodeName)
	node_metric := fmt.Sprintf(nodeMetricPattern, nodeName)
	energy_metric := fmt.Sprintf(energyPattern, nodeName)
	from_energy_metric := fmt.Sprintf(fromEnergyMetricPattern, nodeName)

	return Client{
		nil,
		nodeName,
		hostname,
		username,
		password,
		source.Metric(cpu_metric),
		source.Metric(gpu_metric),
		source.Metric(node_metric),
		source.Metric(energy_metric),
		source.Metric(from_energy_metric),
	}
}

func (client *Client) Connect(ctx context.Context) error {
	slog.Info("opening IPMI connection", "node", client.nodeName)

	session, err := startIPMISession(ctx, client.hostname, client.user, client.password)
	if err != nil {
		return fmt.Errorf("failed to open IPMI connection: %w", err)
	}

	client.ipmi = session

	return nil
}

func (client *Client) Close(ctx context.Context) {
	client.ipmi.Close(ctx)
}

func (client *Client) Reconnect(ctx context.Context) error {
	// future me, please don't try this again. It will just
	// crash the source constantly, as an internal channel
	// is already closed and this would try to close a closed
	// channel.
	// client.ipmi.Close(ctx)

	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("failed to reconnect IPMI connection: %w", err)
	}

	return nil
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

func main() {
	var verbosityFlag string
	var metricqServer string
	var metricqToken string
	var sentryDsn string

	flag.StringVar(&metricqServer, "server", "amqp://localhost", "metricq server URL")
	flag.StringVar(&metricqToken, "token", "source-lenovo-power-meter", "metricq client token")
	flag.StringVar(&sentryDsn, "sentry", "", "the sentry.io DSN to use")
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

	if sentryDsn != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn: sentryDsn,
			// Enable printing of SDK debug messages.
			// Useful when getting started or trying to figure something out.
			Debug: false,
			// Adds request headers and IP for users,
			// visit: https://docs.sentry.io/platforms/go/data-management/data-collected/ for more info
			SendDefaultPII: true,
			EnableLogs:     true,
		})

		if err != nil {
			log.Fatalf("failed to sentry.Init: %s", err)
		}
		defer sentry.Flush(2 * time.Second)
	}

	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	slog.Info("opening connection to metricq", "token", metricqToken)

	connectionStart := time.Now()

	source, err := metricq.NewSource(metricqToken, metricqServer)
	if err != nil {
		log.Panicf("failed to create source: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err = source.Connect(ctx); err != nil {
		log.Panicf("failed to open connection to metricq: %v", err)
	}
	defer source.Close()

	go source.HandleDiscover(ctx, "v0.0.2")

	jsonConfig, err := source.Register(ctx)
	if err != nil {
		log.Panicf("failed to register as metricq source: %v", err)
	}

	var config MetricQConfig
	if err = json.Unmarshal(jsonConfig, &config); err != nil {
		log.Panicf("failed to parse config from metricq: %v", err)
	}

	clients := make(map[string]Client)
	metadata := make(map[string]any)
	initMutex := sync.Mutex{}

	wg := sync.WaitGroup{}
	for i := 1; i <= 156; i++ {
		wg.Add(1)

		nodeName := fmt.Sprintf("c%d", i)

		go func(i int, nodeName string, ctx context.Context) {
			defer wg.Done()

			c := NewClient(nodeName, source, config.User, config.Password)

			if err := c.Connect(ctx); err != nil {
				slog.Warn("failed to connect", "node", nodeName, "err", err)
				return
			}

			// slog.Info("starting to recalibrate sample time", "node", c.nodeName)
			// if err := c.RecalibrateSampleTime(ctx); err != nil {
			// 	slog.Warn(err.Error(), "node", c.nodeName)
			// } else {
			// 	for offset, err := c.ReadExactSampleTime(ctx); offset == 0; {
			// 		if err != nil {
			// 			slog.Warn(err.Error(), "node", c.nodeName)
			// 			break
			// 		}
			// 		time.Sleep(time.Second)
			// 	}
			// }
			// slog.Info("finished to recalibrate sample time", "node", c.nodeName)

			slog.Info("resetting energy counter", "node", c.nodeName)
			if err := c.ResetEnergyAccumulator(ctx); err != nil {
				slog.Warn(err.Error(), "node", c.nodeName)
			} else {
				slog.Info("successfully reset energy counter", "node", c.nodeName)
			}

			initMutex.Lock()
			defer initMutex.Unlock()

			metadata[c.node.Name()] = metricq.MetricMetadata{
				Description: "accurate and fast node power",
				Unit:        "W",
				Rate:        1,
			}
			metadata[c.gpu.Name()] = metricq.MetricMetadata{
				Description: "accurate and fast CPU power",
				Unit:        "W",
				Rate:        1,
			}
			metadata[c.cpu.Name()] = metricq.MetricMetadata{
				Description: "accurate and fast GPU power",
				Unit:        "W",
				Rate:        1,
			}
			metadata[c.energy.Name()] = metricq.MetricMetadata{
				Description: "accurate and fast node energy",
				Unit:        "J",
				Rate:        1,
			}
			metadata[c.fromEnergy.Name()] = metricq.MetricMetadata{
				Description: "accurate and fast node power from energy",
				Unit:        "W",
				Rate:        1,
			}

			clients[nodeName] = c
		}(i, nodeName, ctx)
	}

	wg.Wait()

	err = source.DeclareMetrics(ctx, metadata)
	if err != nil {
		log.Fatalf("Failed to declare metrics: %v", err)
	}

	log.Printf("Establishing the connection took: %v\n\n", time.Now().Sub(connectionStart))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	wg = sync.WaitGroup{}

	for nodeName, client := range clients {

		wg.Add(1)

		deadline := time.Now().Add(time.Second)

		go func(nodeName string, client Client, deadline time.Time) {
			defer wg.Done()
			defer client.Close(ctx)

			var lastReading *command.EnergyReading
			var lastTimestamp time.Time

		WorkerLoop:
			for {
				start := time.Now()

				r, err := client.SinglePowerCommand(ctx)
				if err != nil {
					slog.Warn("failed to execute raw command", "node", nodeName, "command", "single power", "err", err)
					if err := client.Reconnect(ctx); err != nil {
						slog.Error("failed to reconnect", "node", nodeName, "err", err)
					}
				} else {
					if r.CpuPower() > 1000 {
						slog.Info("read implausible cpu power value", "node", nodeName, "value", r.CpuPower())
					}

					if r.GpuPower() > 4000 {
						slog.Info("read implausible gpu power value", "node", nodeName, "value", r.GpuPower())
					}

					slog.Debug("read power values", "cpu", r.CpuPower(), "gpu", r.GpuPower(), "node", nodeName)

					err = client.cpu.Send(ctx, start, r.CpuPower())
					if err != nil {
						slog.Error("failed to send metric", "node", nodeName, "err", err, "metric", client.cpu.Name())
						panic(err)
					}

					err = client.gpu.Send(ctx, start, r.GpuPower())
					if err != nil {
						slog.Error("failed to send metric", "node", nodeName, "err", err, "metric", client.gpu.Name())
						panic(err)
					}

					err = client.node.Send(ctx, start, r.NodePower())
					if err != nil {
						slog.Error("failed to send metric", "node", nodeName, "err", err, "metric", client.node.Name())
						panic(err)
					}
				}

				start = time.Now()
				currentReading, err := client.SingleEnergyCommand(ctx)
				if err != nil {
					slog.Warn("failed to execute raw command", "node", nodeName, "command", "single energy", "err", err)
					if err := client.Reconnect(ctx); err != nil {
						slog.Error("failed to reconnect", "node", nodeName, "err", err)
					}
				} else {
					hostTime := time.Now()
					if lastReading != nil {
						timestamp := currentReading.Time()
						offset := hostTime.Sub(timestamp)
						if offset.Abs().Seconds() > 10 {
							slog.Info("read implausible energy timestamp", "node", nodeName, "offset", int(offset.Seconds()), "timestamp", timestamp, "hostTime", hostTime)
						}

						duration_s := timestamp.Sub(lastReading.Time()).Seconds()
						if duration_s < 0 {
							slog.Info("measurement interval duration is negative", "node", nodeName, "lastTimestamp", lastReading.Time(), "timestamp", timestamp)
						}

						// using host time instead to calculate the energy
						duration_s = hostTime.Sub(lastTimestamp).Seconds()

						energy_j := currentReading.Energy() - lastReading.Energy()
						if energy_j < 0 {
							slog.Info("energy is negative. Possible overflow?", "node", nodeName, "previous energy", lastReading.Energy(), "current energy", currentReading.Energy())
						}

						err = client.energy.Send(ctx, start, currentReading.Energy())
						if err != nil {
							slog.Error("failed to send metric", "node", nodeName, "err", err, "metric", client.energy.Name())
							panic(err)
						}

						err = client.fromEnergy.Send(ctx, start, energy_j/duration_s)
						if err != nil {
							slog.Error("failed to send metric", "node", nodeName, "err", err, "metric", client.fromEnergy.Name())
							panic(err)
						}
					}

					lastReading = &currentReading
					lastTimestamp = hostTime
				}

				if deadline.Before(time.Now()) {
					slog.Info("missed deadline", "node", nodeName, "deadline", deadline, "now", time.Now())
				}

				for ; deadline.Before(time.Now()); deadline = deadline.Add(time.Second) {
				}

				select {
				case <-ctx.Done():
					break WorkerLoop
				case <-time.After(deadline.Sub(time.Now())):
					deadline = deadline.Add(time.Second)
					continue
				}
			}
		}(nodeName, client, deadline)
	}

	wg.Wait()
}
