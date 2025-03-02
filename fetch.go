package remhelp

import (
	"fmt"

	"github.com/go-git/go-git/v5/plumbing"
)

type FetchArg struct {
	Hash plumbing.Hash
	Name plumbing.ReferenceName
}

type FetchCmd struct {
	BaseCmd
	Args []FetchArg
}

var _ Cmd = &ListCmd{}

func (c *FetchCmd) Run(m MetaContext) error {

	err := c.prepareOutput(m)
	err = c.output(m, err)
	return err
}

func (c *FetchCmd) prepareOutput(m MetaContext) error {
	m.DbgLogCmd(fmt.Sprintf("fetch %+v", c.Args))

	ff, err := NewFastFetch(m, c.Args)
	if err != nil {
		return err
	}
	err = ff.Run(m)
	if err != nil {
		return err
	}
	return nil
}

func (c *FetchCmd) output(m MetaContext, err error) error {
	var lines []string
	if err == nil {
		lines = NewOutputSet().Empty().Res()
	}
	return c.DoOutput(lines, err)
}
