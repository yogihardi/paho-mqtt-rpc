package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/google/uuid"
)

var (
	ErrRequestTimeout = errors.New("request timeout")
)

// Handler is the struct providing a request/response functionality for the paho
// MQTT v5 client
type Handler struct {
	sync.Mutex
	c              *paho.Client
	correlData     map[string]chan *paho.Publish
	requestTimeout time.Duration
}

type HandlerInput struct {
	Server         string
	Username       *string
	Password       *string
	RequestTimeout time.Duration
}

func NewHandler(ctx context.Context, in HandlerInput) (*Handler, error) {
	mqttClient, err := mqttConnect(ctx, in)
	if err != nil {
		return nil, err
	}

	h := &Handler{
		c:              mqttClient,
		correlData:     make(map[string]chan *paho.Publish),
		requestTimeout: in.RequestTimeout,
	}

	h.c.Router.RegisterHandler(fmt.Sprintf("%s/responses", h.c.ClientID), h.responseHandler)

	_, err = h.c.Subscribe(ctx, &paho.Subscribe{
		Subscriptions: map[string]paho.SubscribeOptions{
			fmt.Sprintf("%s/responses", h.c.ClientID): {QoS: 1},
		},
	})
	if err != nil {
		return nil, err
	}

	return h, nil
}

func mqttConnect(ctx context.Context, in HandlerInput) (*paho.Client, error) {
	conn, err := net.Dial("tcp", in.Server)
	if err != nil {
		return nil, err
	}

	clientID := uuid.NewString()
	c := paho.NewClient(paho.ClientConfig{
		ClientID: clientID,
		Conn:     conn,
	})
	cp := &paho.Connect{
		KeepAlive:  30,
		CleanStart: true,
		ClientID:   clientID,
	}
	if in.Username != nil {
		cp.UsernameFlag = true
		cp.Username = *in.Username
	}
	if in.Password != nil {
		cp.PasswordFlag = true
		cp.Password = []byte(*in.Password)
	}
	ca, err := c.Connect(ctx, cp)
	if err != nil {
		return nil, err
	}
	if ca.ReasonCode != 0 {
		return nil, fmt.Errorf("failed to connect to %s : %d - %s", in.Server, ca.ReasonCode, ca.Properties.ReasonString)
	}

	return c, nil
}

func (h *Handler) addCorrelID(cID string, r chan *paho.Publish) {
	h.Lock()
	defer h.Unlock()

	h.correlData[cID] = r
}

func (h *Handler) getCorrelIDChan(cID string) chan *paho.Publish {
	h.Lock()
	defer h.Unlock()

	rChan := h.correlData[cID]
	delete(h.correlData, cID)

	return rChan
}

func (h *Handler) Request(ctx context.Context, pb *paho.Publish) (*paho.Publish, error) {
	cID := uuid.New().String()
	rChan := make(chan *paho.Publish)

	h.addCorrelID(cID, rChan)

	if pb.Properties == nil {
		pb.Properties = &paho.PublishProperties{}
	}

	pb.Properties.CorrelationData = []byte(cID)
	pb.Properties.ResponseTopic = fmt.Sprintf("%s/responses", h.c.ClientID)
	pb.Retain = false

	_, err := h.c.Publish(ctx, pb)
	if err != nil {
		return nil, err
	}

	for {
		select {
		case <-time.After(h.requestTimeout):
			return nil, ErrRequestTimeout
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp := <-rChan:
			return resp, nil
		}
	}
}

func (h *Handler) responseHandler(pb *paho.Publish) {
	if pb.Properties == nil || pb.Properties.CorrelationData == nil {
		return
	}

	rChan := h.getCorrelIDChan(string(pb.Properties.CorrelationData))
	if rChan == nil {
		return
	}

	rChan <- pb
}
