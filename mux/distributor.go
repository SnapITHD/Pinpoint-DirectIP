// Package mux provides a service to split incoming directip messages to backend HTTP
// services. The mux stores a list of targets and each target has a pattern for an IMEI.
// If the IMEI of the incoming message matches with the given regurlar expression, the mux
// will send an HTTP request with a JSON message to the configured backend.
//
// Every target service will receive a sbd.InformationElements as a JSON representation in its
// POST body. Please take into account that this service and package does not parse the payload
// which is of type []byte. Many devices use the payload to transfer specific types of data. Your
// backend service has to know how to handle these types.
package mux

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/rs/zerolog"

	"github.com/SnapITHD/Pinpoint-DirectIP/sbd"
)

// A Target stores the configuration of a backend service where the SBD data should be pushed.
type Target struct {
	ID          string            `yaml:"id,omitempty"`
	IMEIPattern string            `yaml:"imeipattern"`
	Backend     string            `yaml:"backend"`
	SkipTLS     bool              `yaml:"skiptls,omitempty"`
	Header      map[string]string `yaml:"header"`
	imeipattern *regexp.Regexp
	client      *http.Client
}

// Targets is a list of Target's
type Targets []Target

// A Distributer can handle the SBD data and dispatches them to the targets. When
// the targets are reconfigured, the can be set vith WithTargets.
type Distributer interface {
	WithTargets(targets Targets) error
	Targets() Targets
	Handle(data *sbd.InformationBucket) error
	Close()
}

type distributer struct {
	zerolog.Logger
	targets       []Target
	sbdChannel    chan *sbdMessage
	configChannel chan Targets
}

type sbdMessage struct {
	data          sbd.InformationBucket
	returnedError chan error
}

// New creates a new Distributor with the given number of workers
func New(numworkers int, log zerolog.Logger) Distributer {
	sc := make(chan *sbdMessage)
	cc := make(chan Targets)
	s := &distributer{
		sbdChannel:    sc,
		configChannel: cc,
		Logger:        log,
	}
	for i := 0; i < numworkers; i++ {
		go s.run(i)
	}
	return s
}

func (f *distributer) Targets() Targets {
	return f.targets
}

func (f *distributer) WithTargets(targets Targets) error {
	var ar Targets
	for _, t := range targets {
		p, err := regexp.Compile(t.IMEIPattern)
		if err != nil {
			return fmt.Errorf("cannot compile patter: %q: %v", t.IMEIPattern, err)
		}
		t.imeipattern = p
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: t.SkipTLS,
			},
		}
		t.client = &http.Client{Transport: tr}

		ar = append(ar, t)
	}
	f.configChannel <- ar
	return nil
}

func (f *distributer) Handle(data *sbd.InformationBucket) error {
	return f.distribute(data)
}

func (f *distributer) distribute(data *sbd.InformationBucket) error {
	msg := &sbdMessage{data: *data, returnedError: make(chan error)}
	f.sbdChannel <- msg
	rerr := <-msg.returnedError
	close(msg.returnedError)
	return rerr
}

func (f *distributer) Close() {
	f.Info().Msg("close distributor")
	close(f.configChannel)
	close(f.sbdChannel)
}

func (f *distributer) run(worker int) {
	f.Info().Int("worker", worker).Msg("start distributor service")
	for {
		select {
		case cfg, more := <-f.configChannel:
			if !more {
				return
			}
			f.Info().Any("config", cfg).Int("worker", worker).Msgf("set config")
			f.targets = cfg
		case msg := <-f.sbdChannel:
			go f.handle(msg)
		}
	}
}

func (f *distributer) handle(m *sbdMessage) {
	js, err := json.Marshal(m.data)
	if err != nil {
		m.returnedError <- err
		return
	}
	imei := m.data.Header.GetIMEI()
	for _, t := range f.targets {
		if t.imeipattern.MatchString(imei) {
			rq, err := http.NewRequest(http.MethodPost, t.Backend, bytes.NewBuffer(js))
			if err != nil {
				f.Error().Str("error", err.Error()).Str("target", t.Backend).Msg("cannot create request")
				m.returnedError <- err
				return
			}
			rq.Header.Add("Content-Type", "application/json")
			for k, v := range t.Header {
				rq.Header.Add(k, v)
			}
			rsp, err := t.client.Do(rq)
			if err != nil {
				f.Error().Str("target", t.Backend).Str("error", err.Error()).Msg("cannot call webhook")
				m.returnedError <- err
				return
			}
			defer rsp.Body.Close()
			content, _ := io.ReadAll(rsp.Body)
			if rsp.StatusCode/100 == 2 {
				f.Info().Str("target", t.Backend).Str("status", rsp.Status).Str("content", string(content)).Msg("data transmitted")
			} else {
				f.Error().Str("target", t.Backend).Str("status", rsp.Status).Str("content", string(content)).Msg("data not transmitted")
				m.returnedError <- err
				return
			}
		}
	}
	m.returnedError <- nil
}
