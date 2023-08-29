package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/rs/zerolog"
	yaml "gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/SnapITHD/Pinpoint-DirectIP/mux"
	"github.com/SnapITHD/Pinpoint-DirectIP/sbd"
)

const (
	defaultListen = "127.0.0.1:2022"
	logJSON       = "json"
	logFMT        = "fmt"
	logTERM       = "term"
)

var (
	revision     string
	builddate    string
	distribution mux.Distributer
	log          zerolog.Logger
)

func main() {
	config := flag.String("config", "", "specify the configuration for your forwarding rules")
	health := flag.String("health", "127.0.0.1:2023", "the healtcheck URL (http)")
	//stage := flag.String("stage", "test", "the name of the stage where this service is running")
	loglevel := flag.String("loglevel", "info", "the loglevel, debug|info|warn|error|crit")
	logformat := flag.String("logformat", "json", "the logformat, fmt|json|term")
	workers := flag.Int("workers", 5, "the number of workers")
	useproxyprotocol := flag.Bool("proxyprotocol", false, "use the proxyprotocol on the listening socket")

	flag.Parse()

	log = setLogOutput(*logformat, *loglevel)

	//log = log15.New("stage", *stage)
	//log := zerolog.New(os.Stdout).With().Timestamp().Logger()

	log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	listen := defaultListen
	if len(flag.Args()) > 0 {
		listen = flag.Arg(0)
	}
	log.Info().Msgf("start service: revision %s, builddate %s, listen %s ...", revision, builddate, listen)
	//log.Info("start service", "revision", revision, "builddate", builddate, "listen", listen)
	distribution = mux.New(*workers, log)
	if *config != "" {
		cfg, err := os.Open(*config)
		if err != nil {
			log.Panic().AnErr("error", err).Msg("cannot open config file")
			os.Exit(1)
		}

		var targets mux.Targets
		err = yaml.NewDecoder(cfg).Decode(&targets)
		if err != nil {
			log.Panic().AnErr("error", err).Msg("cannot unmarshal data")
			os.Exit(1)
		}
		err = distribution.WithTargets(targets)
		if err != nil {
			log.Panic().AnErr("error", err).Msg("cannot use config")
			os.Exit(1)
		}
		log.Info().Any("targets", targets).Msg("change configuration")
	}

	ctx := context.Background()
	client, err := rest.InClusterConfig()
	if err != nil {
		log.Info().Msg("no incluster config, assume standalone mode")
	} else {
		log.Info().Msg("incluster config found, assume kubernetes mode")
		go watchServices(ctx, log, client, distribution)
	}

	go runHealth(*health)
	sbd.NewService(log, listen, sbd.Logger(log, distribution), *useproxyprotocol)
}

func runHealth(health string) {
	http.ListenAndServe(health, http.HandlerFunc(func(rw http.ResponseWriter, rq *http.Request) {
		fmt.Fprintf(rw, "OK")
	}))
}

func watchServices(ctx context.Context, log zerolog.Logger, client *rest.Config, s mux.Distributer) {
	clientset, err := kubernetes.NewForConfig(client)
	if err != nil {
		log.Error().AnErr("error", err).Msg("cannot create clientset for services")
		os.Exit(1)
	}

	watcher, err := clientset.CoreV1().Services(v1.NamespaceAll).
		Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		log.Error().AnErr("error", err).Msg("cannot create watcher for services")
		os.Exit(1)
	}

	for event := range watcher.ResultChan() {
		svc := event.Object.(*v1.Service)
		targets := s.Targets()
		if event.Type == watch.Added {
			t := targetFromService(svc)
			if t != nil {
				targets = append(targets, *t)
				err = s.WithTargets(targets)
				if err != nil {
					log.Error().AnErr("error", err).Msg("cannot change targets")
				} else {
					log.Info().Any("targets", targets).Msg("added new target")
				}
			}
		} else if event.Type == watch.Deleted {
			var tgs []mux.Target
			for _, t := range targets {
				if t.ID == string(svc.ObjectMeta.UID) {
					continue
				}
				tgs = append(tgs, t)
			}
			err = s.WithTargets(tgs)
			if err != nil {
				log.Error().AnErr("error", err).Msg("cannot change targets")
			} else {
				log.Info().Any("targets", targets).Msg("deleted target")
			}

		} else if event.Type == watch.Modified {
			t := targetFromService(svc)
			if t != nil {
				var tgs []mux.Target
				for _, tt := range targets {
					if tt.ID == string(svc.ObjectMeta.UID) {
						tgs = append(tgs, *t)
					} else {
						tgs = append(tgs, tt)
					}
				}
				err = s.WithTargets(tgs)
				if err != nil {
					log.Error().AnErr("error", err).Msg("cannot change targets")
				} else {
					log.Info().Any("targets", targets).Msg("modified target")
				}

			}
		}
	}
}

func targetFromService(s *v1.Service) *mux.Target {
	mt := s.ObjectMeta
	a := mt.Annotations
	if t, ok := a["protegear.io/directip-imei"]; ok {
		path := a["protegear.io/directip-path"]
		if path == "" {
			path = "/"
		}
		port := a["protegear.io/directip-port"]
		if port == "" {
			port = "8080"
		}
		ip := s.Spec.ClusterIP
		if ip != "" {
			log.Info().Str("imei", t).Str("path", path).Str("port", port).Str("ip", ip).Msg("found target")
			bk := mux.Target{
				ID:          string(s.ObjectMeta.UID),
				Backend:     fmt.Sprintf("http://%s:%s%s", ip, port, path),
				IMEIPattern: t,
			}
			return &bk
		}
	}
	return nil
}

func setLogOutput(format, loglevel string) zerolog.Logger {
	var output io.Writer = os.Stdout

	switch format {
	case logFMT:
		// No need to change anything, as the default JSON format is used by zerolog.
	case logTERM:
		output = zerolog.ConsoleWriter{Out: os.Stdout}
	}

	lvl, err := zerolog.ParseLevel(loglevel)
	if err != nil {
		lvl = zerolog.InfoLevel
		fmt.Printf("cannot parse level from parameter: %v\n", err)
	}

	logger := zerolog.New(output).Level(lvl).With().Caller().Timestamp().Logger()
	return logger
}
