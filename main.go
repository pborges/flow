package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/pborges/flow/flow"
	"os"
	"time"
	"encoding/json"
)

func NewTimerNode(id string, duration time.Duration) *TimerNode {
	node := TimerNode{
		id:       id,
		duration: duration,
		tick: &flow.Source{
			Name: "tick",
			//Type: "time.Time",
		},
	}
	return &node
}

type TimerNode struct {
	id       string
	duration time.Duration
	tick     *flow.Source
}

func (t *TimerNode) Run(quit <-chan struct{}) {
	ticker := time.NewTicker(t.duration)
	for {
		select {
		case ti := <-ticker.C:
			t.tick.Exec(ti)
		case <-quit:
			fmt.Println("he's dead jim")
			return
		}
	}
}

func (t TimerNode) Id() string {
	return t.id
}
func (t TimerNode) Sinks() []*flow.Sink {
	return []*flow.Sink{}
}
func (t TimerNode) Sources() []*flow.Source {
	return []*flow.Source{t.tick}
}

func NewLedNode(id string) LedNode {
	node := LedNode{
		id: id,
		toggle: &flow.Sink{
			Name: "toggle",
		},
		setState: &flow.Sink{
			Name: "setState",
			//Type: "bool",
		},
		getState: &flow.Source{
			Name: "getState",
			//Type: "bool",
		},
	}
	node.setState.Fn = func(d interface{}) error {
		if b, ok := d.(bool); ok {
			node.curState = b
			fmt.Println("setState", id, node.curState)
			node.getState.Exec(node.curState)
			return nil
		}
		return errors.New("expected bool but did not get it")
	}
	node.toggle.Fn = func(d interface{}) error {
		node.curState = !node.curState
		fmt.Println("toggle", id, node.curState)
		node.getState.Exec(node.curState)
		return nil
	}
	return node
}

type LedNode struct {
	id       string
	curState bool
	toggle   *flow.Sink
	setState *flow.Sink
	getState *flow.Source
}

func (t LedNode) Id() string {
	return t.id
}
func (t LedNode) Sinks() []*flow.Sink {
	return []*flow.Sink{t.toggle, t.setState}
}
func (t LedNode) Sources() []*flow.Source {
	return []*flow.Source{t.getState}
}

func NewDebugNode(id string) DebugNode {
	node := DebugNode{
		id: id,
		Sink: &flow.Sink{
			Name: "Sink",
			Fn: func(data interface{}) error {
				fmt.Println(data)
				return nil
			},
		},
	}
	return node
}

type DebugNode struct {
	id   string
	Sink *flow.Sink
}

func (t DebugNode) Id() string {
	return t.id
}
func (t DebugNode) Sinks() []*flow.Sink {
	return []*flow.Sink{t.Sink}
}
func (t DebugNode) Sources() []*flow.Source {
	return []*flow.Source{}
}

func main() {

	f := flow.NewFlow()

	if err := f.RegisterNode(NewTimerNode("0debf802-c580-415b-ac65-d28a1c844c3c", time.Second)); err != nil {
		panic(err)
	}
	if err := f.RegisterNode(NewLedNode("806af738-b8e4-4c0d-9a38-3a494816933b")); err != nil {
		panic(err)
	}
	if err := f.RegisterNode(NewLedNode("56ab82b7-7102-4bb0-a5f7-2c3c933f9f07")); err != nil {
		panic(err)
	}
	if err := f.RegisterNode(NewDebugNode("5a2bb2ab-7677-4a54-96e6-f1f37f02c502")); err != nil {
		panic(err)
	}
	if err := f.LinkNode("0debf802-c580-415b-ac65-d28a1c844c3c", "tick", "806af738-b8e4-4c0d-9a38-3a494816933b", "toggle"); err != nil {
		panic(err)
	}
	if err := f.LinkNode("806af738-b8e4-4c0d-9a38-3a494816933b", "getState", "56ab82b7-7102-4bb0-a5f7-2c3c933f9f07", "setState"); err != nil {
		panic(err)
	}

	 //if err := f.UnLinkNode("806af738-b8e4-4c0d-9a38-3a494816933b", "getState", "56ab82b7-7102-4bb0-a5f7-2c3c933f9f07", "setState"); err != nil {
	 //	panic(err)
	 //}

	f.ErrorSink = &flow.Sink{
		Name: "ERROR",
		Fn: func(data interface{}) error {
			e := json.NewEncoder(os.Stdout)
			e.SetIndent("", "  ")
			e.Encode(data)
			e.Encode(data.(flow.Error).Err.Error())
			return nil
		},
	}

	f.Start()
	bufio.NewReader(os.Stdin).ReadLine()
	f.Stop()
	bufio.NewReader(os.Stdin).ReadLine()
}

