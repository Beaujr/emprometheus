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
    dir            = flag.String("dir", "./emhass_config/data", "the directory to serve files from")
    tariff         = flag.Bool("tariff", false, "generate tariff files")
    process        = flag.Bool("process", false, "process emhasses")
    server         = flag.Bool("server", false, "run data server")
    promApi        = flag.String("prometheus", "http://192.168.1.112:9090", "http://promapi:port")
    promUser       = flag.String("prometheus.user", "", "http://promapi:port")
    promPass       = flag.String("prometheus.pass", "", "http://promapi:port")
    octopusProduct = flag.String("octopus.product", "COSY-FIX-12M-25-09-24", "Octopus Product")
    octopusTariff  = flag.String("octopus.tariff", "E-1R-COSY-FIX-12M-25-09-24-N", "Octopus Tariff")
)
```
### Notes
Currently the tariff generated in the CSV is for Octopus using this is optional.