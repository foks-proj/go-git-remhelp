package remhelp

import (
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
)

type BaseCmd struct {
	Output LineOutputter
}

func (b *BaseCmd) DoOutput(lines []string, err error) error {
	return b.Output.Output(lines, err)
}

func (b *BaseCmd) zeroBaseClass() {
	b.Output = nil
}

type Cmd interface {
	Run(MetaContext) error
	zeroBaseClass()
}

type UnknownCmd struct {
	BaseCmd
	Cmd string
}

type CapabilitiesCmd struct {
	BaseCmd
}

type ListCmd struct {
	BaseCmd
	ForPush bool
	oset    OutputSet
}

var _ Cmd = &UnknownCmd{}
var _ Cmd = &CapabilitiesCmd{}
var _ Cmd = &FetchCmd{}

func (c *UnknownCmd) Run(m MetaContext) error {
	return fmt.Errorf("unknown command: %s", c.Cmd)
}

func isHash(r *plumbing.Reference) bool {
	return r.Type() == plumbing.HashReference
}

func refForPush(r *plumbing.Reference) []string {

	var v0 string
	switch r.Type() {
	case plumbing.SymbolicReference:
		v0 = "@" + r.Target().String()
	case plumbing.HashReference:
		v0 = r.Hash().String()
	default:
		v0 = "?"
	}
	return []string{v0, r.Name().String()}
}

func (c *ListCmd) Run(m MetaContext) error {
	err := c.prepareOutput(m)
	err = c.output(m, err)
	return err
}

func (c *ListCmd) prepareOutput(m MetaContext) error {

	m.DbgLogCmd("list" + sel(c.ForPush, " for-push", ""))

	items, err := m.E().Storage().List()
	if err != nil {
		return err
	}

	hasHash := items.HasAnyHashes()

	err = items.ForEach(func(it *plumbing.Reference) error {
		if isHash(it) || (!c.ForPush && hasHash) {
			v := refForPush(it)
			c.oset.Vec(v)
		}
		return nil
	})
	if err != nil {
		return err
	}
	m.DbgLogOutput(fmt.Sprintf("list has %d refs", c.oset.Len()))
	return nil
}

func (c *ListCmd) output(m MetaContext, err error) error {
	var lines []string
	if err == nil {
		lines = c.oset.Empty().Res()
	}
	return c.DoOutput(lines, err)
}

func (c *CapabilitiesCmd) Run(m MetaContext) error {
	m.DbgLogCmd("capabilities")
	return c.DoOutput(NewOutputSet().Lines(
		"fetch",
		"push",
		"list",
	).Empty().Res(), nil)
}
