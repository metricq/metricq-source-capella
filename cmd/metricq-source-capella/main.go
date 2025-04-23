package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bougou/go-ipmi"
	metricq "github.com/metricq/metricq-go"

	"github.com/metricq/metricq-source-capella/pkg/command"
)

func startIPMISession(host string) (*ipmi.Client, error) {
	port := 623
	username := ""
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
	ipmi        *ipmi.Client
	nodeName    string
	cpu         *metricq.SourceMetric
	gpu         *metricq.SourceMetric
	node        *metricq.SourceMetric
	energy      *metricq.SourceMetric
	from_energy *metricq.SourceMetric
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

	if len(response.Response) != 0 {
		return fmt.Errorf("failed to reset energy accumulator: unexpected buffer length: %v != 0", len(response.Response))
	}

	return nil
}

func (client *Client) SinglePowerCommand(ctx context.Context) (command.PowerReading, error) {
	r := command.PowerReading{}

	slog.Info("sending IPMI Raw Command", "node", client.nodeName, "command", "single power")

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

	slog.Info("sending IPMI Raw Command", "node", client.nodeName, "command", "single energy")

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

func (client *Client) Reconnect(ctx context.Context) error {
	// TODO seems like on timeout the channel is already closed.
	// Hopefully, that's always the case, otherwise we leak channels
	// if err := client.ipmi.Close(ctx); err != nil {
	// 	return fmt.Errorf("failed to reconnect IPMI: %w", err)
	// }

	session, err := startIPMISession(client.nodeName)
	if err != nil {
		return fmt.Errorf("failed to reconnect IPMI: %w", err)
	}

	client.ipmi = session

	return nil
}

const cpuMetricPattern string = "hpc.capella.c%d.cpu.power.acc"
const gpuMetricPattern string = "hpc.capella.c%d.gpu.power.acc"
const nodeMetricPattern string = "hpc.capella.c%d.power"
const energyPattern string = "hpc.capella.c%d.energy"
const fromEnergyMetricPattern string = "hpc.capella.c%d.power.fe"
const hostPattern string = "c%d.bmc.capella.hpc.tu-dresden.de"

func main() {
	

	log.Printf("opening connection to metricq")

	connectionStart := time.Now()

	source, err := metricq.NewSource("source-capella-ipmi-raw-debug", "")
	if err != nil {
		log.Panicf("failed to create source: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err = source.Connect(ctx); err != nil {
		log.Panicf("failed to open connection to metricq: %v", err)
	}
	defer source.Close()

	go source.HandleDiscover(ctx, "v0.0.1")

	// ignore config for now, we hardcode the shit out of this
	_, err = source.Register(ctx)
	if err != nil {
		log.Panicf("failed to register as metricq source: %v", err)
	}

	clients := make(map[string]Client)
	metadata := make(map[string]any)
	connectionErrors := make(map[string]int)
	timeErrors := make(map[string]int)
	valueErrors := make(map[string]int)
	initMutex := sync.Mutex{}
	errorMutex := sync.Mutex{}

	wg := sync.WaitGroup{}
	for i := 1; i <= 156; i++ {
		wg.Add(1)
		host := fmt.Sprintf(hostPattern, i)
		connectionErrors[host] = 0
		timeErrors[host] = 0
		valueErrors[host] = 0

		go func(i int, hostname string, ctx context.Context) {
			defer wg.Done()

			cpu_metric := fmt.Sprintf(cpuMetricPattern, i)
			gpu_metric := fmt.Sprintf(gpuMetricPattern, i)
			node_metric := fmt.Sprintf(nodeMetricPattern, i)
			energy_metric := fmt.Sprintf(energyPattern, i)
			from_energy_metric := fmt.Sprintf(fromEnergyMetricPattern, i)
			client, err := startIPMISession(hostname)

			if err != nil {
				log.Printf("Failed to connect to %s: %v", hostname, err)
				return
			}

			c := Client{
				client,
				fmt.Sprintf("c%d", i),
				source.Metric(cpu_metric),
				source.Metric(gpu_metric),
				source.Metric(node_metric),
				source.Metric(energy_metric),
				source.Metric(from_energy_metric),
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

			initMutex.Lock()
			defer initMutex.Unlock()

			metadata[node_metric] = metricq.MetricMetadata{
				Description: "accurate and fast node power",
				Unit:        "W",
				Rate:        1,
			}
			metadata[cpu_metric] = metricq.MetricMetadata{
				Description: "accurate and fast CPU power",
				Unit:        "W",
				Rate:        1,
			}
			metadata[gpu_metric] = metricq.MetricMetadata{
				Description: "accurate and fast GPU power",
				Unit:        "W",
				Rate:        1,
			}
			metadata[energy_metric] = metricq.MetricMetadata{
				Description: "accurate and fast node energy",
				Unit:        "J",
				Rate:        1,
			}
			metadata[from_energy_metric] = metricq.MetricMetadata{
				Description: "accurate and fast node power from energy",
				Unit:        "W",
				Rate:        1,
			}

			clients[hostname] = c
		}(i, host, ctx)
	}

	wg.Wait()

	hosts := make([]string, 0, len(clients))
	for k := range clients {
		hosts = append(hosts, k)
	}

	slices.Sort(hosts)

	err = source.DeclareMetrics(ctx, metadata)
	if err != nil {
		log.Fatalf("Failed to declare metrics: %v", err)
	}

	log.Printf("Establishing the connection took: %v\n\n", time.Now().Sub(connectionStart))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	wg = sync.WaitGroup{}

	for host, client := range clients {

		wg.Add(1)

		deadline := time.Now().Add(time.Second)

		go func(host string, client Client, deadline time.Time) {
			defer wg.Done()
			defer client.Close(ctx)

			var lastReading *command.EnergyReading
			var lastTimestamp time.Time

		WorkerLoop:
			for {
				start := time.Now()

				r, err := client.SinglePowerCommand(ctx)
				if err != nil {
					slog.Error(fmt.Sprintf("failed to execute raw command for power: %v", err), "node", host)
					errorMutex.Lock()
					connectionErrors[host] += 1
					errorMutex.Unlock()
					if err := client.Reconnect(ctx); err != nil {
						slog.Error(fmt.Sprintf("failed to reconnect IPMI: %v", err), "node", host)
					}
				} else {

					if r.CpuPower() > 1000 {
						slog.Warn("read implausible cpu power value", "node", host, "value", r.CpuPower())
						errorMutex.Lock()
						valueErrors[host] += 1
						errorMutex.Unlock()
					}

					if r.GpuPower() > 4000 {
						slog.Warn("read implausible gpu power value", "node", host, "value", r.GpuPower())
						errorMutex.Lock()
						valueErrors[host] += 1
						errorMutex.Unlock()
					}

					slog.Debug(fmt.Sprintf("Read Power Values for %s: %d W CPU; %d W GPU", host, int(r.CpuPower()), int(r.GpuPower())), "node", host)

					err = client.cpu.Send(ctx, start, r.CpuPower())
					if err != nil {
						slog.Error("failed to send metric", "node", host, "err", err, "metric", client.cpu.Name())
						panic(err)
					}

					err = client.gpu.Send(ctx, start, r.GpuPower())
					if err != nil {
						slog.Error("failed to send metric", "node", host, "err", err, "metric", client.gpu.Name())
						panic(err)
					}

					err = client.node.Send(ctx, start, r.NodePower())
					if err != nil {
						slog.Error("failed to send metric", "node", host, "err", err, "metric", client.node.Name())
						panic(err)
					}
				}

				start = time.Now()
				currentReading, err := client.SingleEnergyCommand(ctx)
				if err != nil {
					slog.Error(fmt.Sprintf("failed to execute raw command: %v", err), "node", host, "command", "single energy")
					errorMutex.Lock()
					connectionErrors[host] += 1
					errorMutex.Unlock()
				} else {
					hostTime := time.Now()
					if lastReading != nil {
						timestamp := currentReading.Time()
						offset := hostTime.Sub(timestamp)
						if offset.Abs().Seconds() > 10 {
							slog.Warn("read implausible energy timestamp", "node", host, "offset", int(offset.Seconds()), "timestamp", timestamp, "hostTime", hostTime)
							errorMutex.Lock()
							timeErrors[host] += 1
							errorMutex.Unlock()
						}

						duration_s := timestamp.Sub(lastReading.Time()).Seconds()
						if duration_s < 0 {
							slog.Warn("duration is negative", "node", host, "lastTimestamp", lastReading.Time(), "timestamp", timestamp)
							errorMutex.Lock()
							timeErrors[host] += 1
							errorMutex.Unlock()
						}

						// using host time instead to calculate the energy
						duration_s = hostTime.Sub(lastTimestamp).Seconds()

						energy_j := currentReading.Energy() - lastReading.Energy()
						if energy_j < 0 {
							slog.Warn("energy is negative. Possible overflow?", "node", host, "previous energy", lastReading.Energy(), "current energy", currentReading.Energy())
							errorMutex.Lock()
							valueErrors[host] += 1
							errorMutex.Unlock()
						}

						err = client.energy.Send(ctx, start, currentReading.Energy())
						if err != nil {
							slog.Error("failed to send metric", "node", host, "err", err, "metric", client.energy.Name())
							panic(err)
						}

						err = client.from_energy.Send(ctx, start, energy_j/duration_s)
						if err != nil {
							slog.Error("failed to send metric", "node", host, "err", err, "metric", client.from_energy.Name())
							panic(err)
						}
					}

					lastReading = &currentReading
					lastTimestamp = hostTime
				}

				if deadline.Before(time.Now()) {
					slog.Warn("missed deadline", "node", host)
				}

				for ; deadline.Before(time.Now()); deadline = deadline.Add(time.Second) {
					// we count every missed deadline as one connection error
					errorMutex.Lock()
					connectionErrors[host] += 1
					errorMutex.Unlock()
				}

				select {
				case <-ctx.Done():
					break WorkerLoop
				case <-time.After(deadline.Sub(time.Now())):
					deadline = deadline.Add(time.Second)
					continue
				}
			}
		}(host, client, deadline)

	}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {

			log.Print("------------------------------------------------------")
			log.Print("Errored hosts in last 60 seconds:")

			errorMutex.Lock()

			numErroredHosts := 0
			numTotalErrors := 0

			for _, host := range hosts {
				numConnectionErrors := connectionErrors[host]
				numTimeErrors := timeErrors[host]
				numValueErrors := valueErrors[host]

				if numConnectionErrors > 0 || numTimeErrors > 0 || numValueErrors > 0 {
					log.Printf("[%34s] (%3d / %3d / %3d) %s%s%s", host, numConnectionErrors, numTimeErrors, numValueErrors, strings.Repeat("C", numConnectionErrors), strings.Repeat("t", numTimeErrors), strings.Repeat("V", numValueErrors))
					connectionErrors[host] = 0
					timeErrors[host] = 0
					valueErrors[host] = 0
					numErroredHosts += 1
					numTotalErrors += numConnectionErrors
				}
			}
			errorMutex.Unlock()

			if numErroredHosts > 0 {
				log.Printf("[Total %d of %d hosts] (%d) %s", numErroredHosts, len(clients), numTotalErrors, strings.Repeat("C", numTotalErrors))
			}

			log.Print("------------------------------------------------------")

			select {
			case <-ctx.Done():
				log.Print("finished error watcher.")
				return
			case <-time.After(60 * time.Second):
				continue
			}
		}
	}()

	wg.Wait()
}
