package flow

import (
	"errors"
	"fmt"
	"reflect"
)

type Error struct {
	SourceNodeGuid string
	SourceNodeType string
	SinkNodeGuid   string
	SinkNodeType   string
	Err            error
}

func (e Error) Error() string {
	return fmt.Sprintf("error %s(%s)-> %s(%s): %+v", e.SourceNodeGuid, e.SourceNodeType, e.SinkNodeGuid, e.SinkNodeType, e.Err)
}

type Flow struct {
	sinkMap   map[string]map[string]*Sink
	sourceMap map[string]map[string]*Source
	nodeMap   map[string]Node
	quitChan  chan struct{}
	ErrorSink *Node
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
		getTypeName(l.SourceNode),
		l.SourceNode.Id(),
		l.Source.Name,
		getTypeName(l.SinkNode),
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
	flow  *Flow
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
	//fmt.Println(ln)
	return nil
}

func unlinkNode(SinkNode Node, Sink *Sink, SourceNode Node, Source *Source) error {
	return errors.New("unimplemented")
}

func (o Source) Exec(d interface{}) {
	for _, link := range o.links {
		go func() {
			if err := link.Sink.Fn(d); err != nil {
				//fmt.Println(link, "ERROR:", err)
				if o.flow.ErrorSink != nil {
					o.flow.ErrorSink.Fn(Error{
						SourceNodeGuid: link.SourceNode.Id(),
						SourceNodeType: getTypeName(link.SourceNode),
						SinkNodeGuid:   link.SinkNode.Id(),
						SinkNodeType:   getTypeName(link.SinkNode),
						Err:            err,
					})
				}
			}
		}()
	}
}

func (f *Flow) RegisterNode(n Node) error {
	fmt.Println("register", getTypeName(n), n.Id())
	f.nodeMap[n.Id()] = n

	for _, i := range n.Sinks() {
		if _, ok := f.sinkMap[n.Id()]; !ok {
			f.sinkMap[n.Id()] = make(map[string]*Sink)
		}
		fmt.Println("\tSink " + i.Name)
		f.sinkMap[n.Id()][i.Name] = i
	}
	for _, o := range n.Sources() {
		if _, ok := f.sourceMap[n.Id()]; !ok {
			//if o.Type == "" {
			//	return errors.New("Source type for " + o.Name + " cannot be empty")
			//}
			f.sourceMap[n.Id()] = make(map[string]*Source)
		}
		fmt.Println("\tSource " + o.Name)
		o.flow = f
		f.sourceMap[n.Id()][o.Name] = o
	}
	return nil
}

func (f *Flow) LinkNode(sourceGuid, sourceName, sinkGuid, sinkName string) error {
	var sourceNode, sinkNode Node
	var source *Source
	var sink *Sink
	var ok bool

	if sourceNode, ok = f.nodeMap[sourceGuid]; ok {
		if source, ok = f.sourceMap[sourceGuid][sourceName]; !ok {
			return errors.New("no source " + sourceName)
		}
	} else {
		return errors.New("no source node " + sourceGuid)
	}
	if sinkNode, ok = f.nodeMap[sinkGuid]; ok {
		if sink, ok = f.sinkMap[sinkGuid][sinkName]; !ok {
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

func (f *Flow) UnLinkNode(sourceGuid, sourceName, sinkGuid, sinkName string) error {
	var sourceNode, sinkNode Node
	var source *Source
	var sink *Sink
	var ok bool

	if sourceNode, ok = f.nodeMap[sourceGuid]; ok {
		if source, ok = f.sourceMap[sourceGuid][sourceName]; !ok {
			return errors.New("no source " + sourceName)
		}
	} else {
		return errors.New("no source node " + sourceGuid)
	}
	if sinkNode, ok = f.nodeMap[sinkGuid]; ok {
		if sink, ok = f.sinkMap[sinkGuid][sinkName]; !ok {
			return errors.New("no sink " + sinkName)
		}
	} else {
		return errors.New("no sink node " + sinkGuid)
	}

	return unlinkNode(sinkNode, sink, sourceNode, source)
}

func (f *Flow) Start() {
	f.quitChan = make(chan struct{})
	for _, n := range f.nodeMap {
		if r, ok := n.(Runner); ok {
			go r.Run(f.quitChan)
		}
	}
}

func (f *Flow) Stop() {
	close(f.quitChan)
}

func getTypeName(i interface{}) string {
	val := reflect.ValueOf(i)
	if val.Kind() == reflect.Ptr {
		return val.Elem().Type().Name()
	}
	return val.Type().Name()
}

