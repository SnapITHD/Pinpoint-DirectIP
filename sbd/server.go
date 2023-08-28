package sbd

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"time"

	proxyproto "github.com/pires/go-proxyproto"
	"github.com/rs/zerolog"
)

const (
	deadline = 30 * time.Second
)

// A Handler is called by the service when a new *Short Burst Data* packet
// comes in. The handler will get an *InformationBucket* where all the packet data
// is bundled. If this handler returns nil, the server will send a positiv
// acknowledge back otherwise the packet will not be acknowledged.
type Handler interface {
	Handle(data *InformationBucket) error
}

// A HandlerFunc makes a handler from a function.
type HandlerFunc func(data *InformationBucket) error

// Handle implements the required interface for *Handler*.
func (f HandlerFunc) Handle(data *InformationBucket) error {
	return f(data)
}

// Logger is a middleware function which wraps a handler with logging
// capabilities.
func Logger(log zerolog.Logger, next Handler) Handler {
	return HandlerFunc(func(data *InformationBucket) error {
		js, err := json.Marshal(data)
		if err != nil {
			return err
		}
		log.Info().Str("elements", string(js)).Msg("new data")
		return next.Handle(data)
	})
}

type result struct {
	MessageHeader
	Header
	MOConfirmationMessage
}

func createResult(status byte) *result {
	return &result{MessageHeader: MessageHeader{ProtocolRevision: protocolRevision, MessageLength: 4}, Header: Header{ID: moConfirmationID, ElementLength: 1}, MOConfirmationMessage: MOConfirmationMessage{Status: status}}
}

// NewService starts a listener on the given *address* and dispatches every
// short burst data packet to the given handler. If the handler returns a
// non-nil error, the service will send a negative response, otherwise the
// response status will be ok.
func NewService(log zerolog.Logger, address string, h Handler, proxyprotocol bool) error {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("cannot open listening address %q: %v", address, err)
	}
	if proxyprotocol {
		l = &proxyproto.Listener{Listener: l, ReadHeaderTimeout: 10 * time.Second}
	}
	defer l.Close()
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal().AnErr("error", err).Msg("cannot accept")
			// let it crash! it's up to the caller of the program to restart it
			panic(err)
		}

		go func(c net.Conn) {
			// directip connects, sends message and closes connection, so no while loop is needed
			// to read more than one message from the connection
			defer c.Close()

			// set a deadline so we do not run out of connections
			c.SetDeadline(time.Now().Add(deadline))

			log.Info().Msg("new connection")
			el, err := GetElements(c)
			res := createResult(0)
			if err != nil {
				log.Error().AnErr("error", err).Msg("cannot get elements from connection")
				binary.Write(c, binary.BigEndian, res)
				return
			}
			log.Info().Any("elements", el).Msg("received data")
			err = h.Handle(el)
			if err != nil {
				log.Error().AnErr("error", err).Msg("error handling message")
			} else {
				res.Status = 1
			}
			log.Info().Any("result", res).Msg("write response")
			binary.Write(c, binary.BigEndian, res)
		}(conn)
	}
}
