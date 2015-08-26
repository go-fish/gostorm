package gostorm

import (
	zmq "github.com/pebbe/zmq4"
)

type Bolt struct {
	Component
}

func NewBolt(pair string) (bolt *Bolt, err error) {
	bolt = &Bolt{}

	err = bolt.InitSocket(pair)
	return
}

func (this *Bolt) InitSocket(pair string) (err error) {
	this.Component.Context, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.Socket, err = this.Component.Context.NewSocket(zmq.PAIR)
	if err != nil {
		return
	}

	err = this.Component.Socket.Connect("tcp://" + pair)
	return
}
