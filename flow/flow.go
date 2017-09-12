package flow

import (
	"errors"
	"fmt"
	"reflect"
)

type Flow struct {
	sinkMap   map[string]map[string]*Sink
	sourceMap map[string]map[string]*Source
	nodeMap   map[string]Node
	quitChan  chan struct{}
}

func NewFlow() *Flow {
	return &Flow{
		sinkMap:   make(map[string]map[string]*Sink),
		sourceMap: make(map[string]map[string]*Source),
		nodeMap:   make(map[string]Node),
	}
}

type Runner interface {
	Run(quit <-chan struct{})
}

type Node interface {
	Id() string
	Sinks() []*Sink
	Sources() []*Source
}

type nodeLink struct {
	SinkNode   Node
	Sink       *Sink
	SourceNode Node
	Source     *Source
}

func (l nodeLink) String() string {
	return fmt.Sprintf("%s(%s)->%s to %s(%s)->%s",
		reflect.TypeOf(l.SourceNode).Name(),
		l.SourceNode.Id(),
		l.Source.Name,
		reflect.TypeOf(l.SinkNode).Name(),
		l.SinkNode.Id(),
		l.Sink.Name)
}

type Sink struct {
	Name string
	Fn   func(interface{}) error
	//Type string
}

type Source struct {
	Name  string
	links []nodeLink
	//Type   string
}

func (o *Source) String() string {
	return o.Name
}

func linkNode(SinkNode Node, Sink *Sink, SourceNode Node, Source *Source) error {
	if Source.links == nil {
		Source.links = make([]nodeLink, 0)
	}
	ln := nodeLink{
		SinkNode:   SinkNode,
		Sink:       Sink,
		SourceNode: SourceNode,
		Source:     Source,
	}
	Source.links = append(Source.links, ln)
	fmt.Println(ln)
	return nil
}

func unlinkNode(SinkNode Node, Sink *Sink, SourceNode Node, Source *Source) error {
	return errors.New("unimplemented")
}

func (o Source) Exec(d interface{}) {
	for _, link := range o.links {
		go func() {
			if err := link.Sink.Fn(d); err != nil {
				fmt.Println(link, "ERROR:", err)
			}
		}()
	}
}

func (flow *Flow) RegisterNode(n Node) error {
	fmt.Println("register", reflect.TypeOf(n).Name(), n.Id())
	flow.nodeMap[n.Id()] = n

	for _, i := range n.Sinks() {
		if _, ok := flow.sinkMap[n.Id()]; !ok {
			flow.sinkMap[n.Id()] = make(map[string]*Sink)
		}
		fmt.Println("\tSink " + i.Name)
		flow.sinkMap[n.Id()][i.Name] = i
	}
	for _, o := range n.Sources() {
		if _, ok := flow.sourceMap[n.Id()]; !ok {
			//if o.Type == "" {
			//	return errors.New("Source type for " + o.Name + " cannot be empty")
			//}
			flow.sourceMap[n.Id()] = make(map[string]*Source)
		}
		fmt.Println("\tSource " + o.Name)
		flow.sourceMap[n.Id()][o.Name] = o
	}
	return nil
}

func (flow *Flow) LinkNode(sourceGuid, sourceName, sinkGuid, sinkName string) error {
	var sourceNode, sinkNode Node
	var source *Source
	var sink *Sink
	var ok bool

	if sourceNode, ok = flow.nodeMap[sourceGuid]; ok {
		if source, ok = flow.sourceMap[sourceGuid][sourceName]; !ok {
			return errors.New("no source " + sourceName)
		}
	} else {
		return errors.New("no source node " + sourceGuid)
	}
	if sinkNode, ok = flow.nodeMap[sinkGuid]; ok {
		if sink, ok = flow.sinkMap[sinkGuid][sinkName]; !ok {
			return errors.New("no sink " + sinkName)
		}
	} else {
		return errors.New("no sink node " + sinkGuid)
	}
	//if in.Type != "" && out.Type != in.Type {
	//	return errors.New(fmt.Sprintf("type mismatch in: %s out: %s", in.Type, out.Type))
	//}
	return linkNode(sinkNode, sink, sourceNode, source)
}

func (flow *Flow) UnLinkNode(sourceGuid, sourceName, sinkGuid, sinkName string) error {
	var sourceNode, sinkNode Node
	var source *Source
	var sink *Sink
	var ok bool

	if sourceNode, ok = flow.nodeMap[sourceGuid]; ok {
		if source, ok = flow.sourceMap[sourceGuid][sourceName]; !ok {
			return errors.New("no source " + sourceName)
		}
	} else {
		return errors.New("no source node " + sourceGuid)
	}
	if sinkNode, ok = flow.nodeMap[sinkGuid]; ok {
		if sink, ok = flow.sinkMap[sinkGuid][sinkName]; !ok {
			return errors.New("no sink " + sinkName)
		}
	} else {
		return errors.New("no sink node " + sinkGuid)
	}

	return unlinkNode(sinkNode, sink, sourceNode, source)
}

func (flow *Flow) Start() {
	flow.quitChan = make(chan struct{})
	for _, n := range flow.nodeMap {
		if r, ok := n.(Runner); ok {
			go r.Run(flow.quitChan)
		}
	}
}

func (flow *Flow) Stop() {
	close(flow.quitChan)
}
