package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bougou/go-ipmi"
	metricq "github.com/metricq/metricq-go"
)

func createConnection(host string) (*ipmi.Client, error) {
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

	// TODO Add proper code to trigger recalibration on startup
	// According to documentation, this process take 2-5 minutes! Yes, minutes
	// While the calibration is running, this should return 0
	// response, err := client.RawCommand(0x3A, 0x32, []byte{4, 3, 0}, "")
	//     if err == nil {
	//         log.Printf("failed to read sample time: %v", err)
	//     } else {
	//         sampleTime := binary.LittleEndian.Uint32(response.Response[0:4])
	//         log.Printf("[%34s] sample time: %v", host, sampleTime)
	//     }

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

type EnergyReading struct {
	timestamp    time.Time
	timestamp_s  uint32
	timestamp_ms uint16
	energy_j     uint32
	energy_mj    uint16
}

func NewReading() EnergyReading {
	return EnergyReading{timestamp: time.Now()}
}

func (r *EnergyReading) Unpack(buf []byte) error {
	if len(buf) != 14 {
		return fmt.Errorf("unexpected buffer length: %v != 14", len(buf))
	}

	r.timestamp_s = binary.LittleEndian.Uint32(buf[8:12])
	r.timestamp_ms = binary.LittleEndian.Uint16(buf[12:14])
	r.energy_j = binary.LittleEndian.Uint32(buf[2:6])
	r.energy_mj = binary.LittleEndian.Uint16(buf[6:8])

	return nil
}

func (r *EnergyReading) Time() time.Time {
	return time.Unix(int64(r.timestamp_s), int64(r.timestamp_ms)*1000*1000)
}

func (r *EnergyReading) HostTime() time.Time {
	return r.timestamp
}

func (r *EnergyReading) Energy() float64 {
	return float64(r.energy_j) + float64(r.energy_mj)/1000
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
	for i := 1; i <= 148; i++ {
		wg.Add(1)
		host := fmt.Sprintf(hostPattern, i)
		connectionErrors[host] = 0
		timeErrors[host] = 0
		valueErrors[host] = 0

		go func(i int, hostname string) {
			defer wg.Done()

			cpu_metric := fmt.Sprintf(cpuMetricPattern, i)
			gpu_metric := fmt.Sprintf(gpuMetricPattern, i)
			node_metric := fmt.Sprintf(nodeMetricPattern, i)
			energy_metric := fmt.Sprintf(energyPattern, i)
			from_energy_metric := fmt.Sprintf(fromEnergyMetricPattern, i)
			client, err := createConnection(hostname)

			if err != nil {
				log.Printf("Failed to connect to %s: %v", hostname, err)
				return
			}

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

			clients[hostname] = Client{
				client,
				fmt.Sprintf("c%d", i),
				source.Metric(cpu_metric),
				source.Metric(gpu_metric),
				source.Metric(node_metric),
				source.Metric(energy_metric),
				source.Metric(from_energy_metric),
			}
		}(i, host)
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

			var lastReading *EnergyReading

		WorkerLoop:
			for {
				start := time.Now()

				// log.Printf("sending IPMI Raw Command to %s", host)
				response, err := client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 8, 0, 0, 0}, "")
				if err != nil {
					errorMutex.Lock()
					log.Printf("[%34s] Failed to execute raw command for power: %s", host, err)

					connectionErrors[host] += 1
					errorMutex.Unlock()
				} else {
					buf := bytes.NewBuffer(response.Response[6:14])
					var cpu_power uint16
					var gpu_power uint16
					binary.Read(buf, binary.LittleEndian, &cpu_power)
					binary.Read(buf, binary.LittleEndian, &gpu_power)

					if cpu_power > 1000 {
						log.Printf("[%34s] Read implausible cpu power value: %v W", host, cpu_power)
						errorMutex.Lock()
						valueErrors[host] += 1
						errorMutex.Unlock()
					}

					if gpu_power > 4000 {
						log.Printf("[%34s] Read implausible gpu power value: %v W", host, gpu_power)
						errorMutex.Lock()
						valueErrors[host] += 1
						errorMutex.Unlock()
					}

					// log.Printf("Read Power Values for %s: %d W CPU; %d W GPU", host, cpu_power, gpu_power)

					err = client.cpu.Send(ctx, start, float64(cpu_power))
					if err != nil {
						log.Fatalf("Failed to send metric %s: %v", client.cpu.Name(), err)
					}

					err = client.gpu.Send(ctx, start, float64(gpu_power))
					if err != nil {
						log.Fatalf("Failed to send metric %s: %v", client.gpu.Name(), err)
					}

					err = client.node.Send(ctx, start, float64(cpu_power+gpu_power))
					if err != nil {
						log.Fatalf("Failed to send metric %s: %v", client.gpu.Name(), err)
					}
				}

				response, err = client.ipmi.RawCommand(ctx, 0x3A, 0x32, []byte{4, 2, 0, 0, 0}, "")
				if err != nil {
					errorMutex.Lock()
					log.Printf("[%34s] Failed to execute raw command for energy: %s", host, err)

					connectionErrors[host] += 1
					errorMutex.Unlock()
				} else if len(response.Response) >= 14 {
					currentReading := EnergyReading{timestamp: time.Now()}
                    currentReading.Unpack(response.Response)

					if lastReading != nil {
						timestamp := currentReading.Time()
						offset := currentReading.HostTime().Sub(timestamp)
						if offset.Abs().Seconds() > 10 {
							// log.Printf("[%34s] Read implausible energy timestamp: Offset %v sec (%v </> %v)", host, int(offset.Seconds()), timestamp, currentReading.HostTime())
							errorMutex.Lock()
							timeErrors[host] += 1
							errorMutex.Unlock()
						}

						duration_s := timestamp.Sub(lastReading.Time()).Seconds()
						if duration_s < 0 {
							log.Printf("[%34s] duration is negative: %v </> %v", host, lastReading.Time(), timestamp)
							errorMutex.Lock()
							timeErrors[host] += 1
							errorMutex.Unlock()
						}

						// using host time instead to calculate the energy
						duration_s = currentReading.HostTime().Sub(lastReading.HostTime()).Seconds()

						energy_j := currentReading.Energy() - lastReading.Energy()
						if energy_j < 0 {
							log.Printf("[%34s] energy is negative. Possible overflow? %v </> %v", host, lastReading.Energy(), currentReading.Energy())
							errorMutex.Lock()
							valueErrors[host] += 1
							errorMutex.Unlock()
						}

						err = client.energy.Send(ctx, currentReading.timestamp, energy_j)
						if err != nil {
							log.Fatalf("[%34s] failed to send energy metric %s: %v", host, client.energy.Name(), err)
						}

						err = client.from_energy.Send(ctx, currentReading.timestamp, energy_j/duration_s)
						if err != nil {
							log.Fatalf("[%34s] failed to send metric %s: %v", host, client.cpu.Name(), err)
						}
					}

					lastReading = &currentReading
				} else {
					log.Printf("[%s] ipmi raw command response for energy has wrong format", host)
				}

				duration := time.Now().Sub(start)

				if duration > time.Second {
					log.Printf("[%s] request took: %v", host, duration)
				}

				if deadline.Before(time.Now()) {
					log.Printf("[%s] missed deadline: %v", host, deadline)
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
