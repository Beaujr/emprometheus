# EmPrometheus
Running emhass against prometheus

## Goal
Be able to run [emhass by davidusb-geek](https://github.com/davidusb-geek/emhass) without home-assistant instead using prometheus data

## docker-compose
```bash
docker compose -f docker-compose.yaml up -d
```
or 
```makefile
make compose
```

## flags
```go
var (
    dir               = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
    tariff            = flag.Bool("tariff", false, "generate tariff files")
    process           = flag.Bool("process", false, "process emhasses")
    server            = flag.Bool("server", false, "run data server")
    promApi           = flag.String("prometheus", "http://192.168.1.112:9090", "http://promapi:port")
    promUser          = flag.String("prometheus.user", "", "http://promapi:port")
    promPass          = flag.String("prometheus.pass", "", "http://promapi:port")
    octopusProduct    = flag.String("octopus.product", "COSY-FIX-12M-25-09-24", "Octopus Product")
    octopusTariff     = flag.String("octopus.tariff", "E-1R-COSY-FIX-12M-25-09-24-N", "Octopus Tariff")
    useTemporal       = flag.Bool("temporal.enable", false, "use temporal")
    temporalNamespace = flag.String("temporal.namespace", "beau", "temporal namespace")
    temporalAddress   = flag.String("temporal.address", "temporal-frontend-headless.temporal.svc.cluster.local:7233", "temporal address")
    temporalSchedule  = flag.String("temporal.schedule", "2 11,23 * * *", "temporal schedule")
    soc = flag.String("soc", "battery_soc", "the prometheus battery SOC metric")
)
```

## Trigger a forecast
This will fascade a call to emhass and save the output
```bash
curl http://localhost:8123/action/dayahead-optim
curl -X POST \
  http://localhost:8123/process \
  -H 'forecast-method: dayahead-optim' 
```

```bash
curl http://localhost:8123/action/naive-mpc-optim
curl -X POST \
  http://localhost:8123/process \
  -H 'forecast-method: naive-mpc-optim' 
```

## Temporal
workflow is work in progress but the existing logic is as below
```mermaid
flowchart TD
    subgraph Temporal
        A2["Forecast Workflow - Once every 24 hours"]
    end

    subgraph Services
        B2["Emprometheus"]
        C2["Emhass"]
    end

    subgraph Storage
        D2[("Optimization Store")]
    end

    subgraph FileStorage
        F2[("opt_res_latest.csvV")]
    end


    A2 -->|1. GET /action/dayahead-optim| B2
    B2 -->|2. GET /action/dayahead-optim| C2
    C2 -->|Optimization Result| F2
    B2 -->|3. Read CSV| F2
    B2 -->|4. Persist Optimization| D2
    D2 -->|5. Read Optimization| B2
    B2 -->|6. Generate and save Schedules| D2
    B2 -->|7. Create hourly schedules in Temporal| A2
```
### Workflows
```mermaid
flowchart LR

    subgraph Temporal
        FW["Forecast Workflow"]
        MPC["MPC Workflow"]
        IW["Inverter Workflow"]
        TS[("Temporal Schedules")]
    end

    subgraph Device
        INV["Inverter"]
    end

    FW -->|Creates 24 hourly schedules| TS
    MPC -->|Creates / Updates schedules for next X horizon hours| TS
    TS -->|Triggers scheduled executions| IW
    IW -->|Applies optimizations| INV
```

### Notes
Currently the tariff generated in the CSV is for Octopus using this is optional.