package remhelp

import (
	"fmt"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
)

// RefData stores the data for a ref.
type RefData struct {
	IsDelete bool
	Commits  []*object.Commit
}

// RefDataByName represents a map of reference names to data about that ref.
type RefDataByName map[plumbing.ReferenceName]*RefData

type PushCmd struct {
	BaseCmd

	// input args
	Refs []config.RefSpec

	// local state
	set     map[config.RefSpec]bool
	deletes []config.RefSpec
	pushes  []config.RefSpec
	results map[string]error

	fetchFrom *ReverseRemote
}

var _ Cmd = &PushCmd{}

func (c *PushCmd) init(m MetaContext) error {
	for _, ref := range c.Refs {
		m.DbgLogCmd("push " + ref.String())
	}

	for _, ref := range c.Refs {
		switch {
		case ref.IsDelete() && ref.IsWildcard():
			return fmt.Errorf("wildcard delete refspec not supported: %s", ref)
		case ref.IsDelete():
			c.deletes = append(c.deletes, ref)
		default:
			c.pushes = append(c.pushes, ref)
		}
	}

	c.results = make(map[string]error, len(c.Refs))

	c.set = make(map[config.RefSpec]bool, len(c.Refs))
	for _, ref := range c.Refs {
		c.set[ref] = true
	}
	return nil
}

// firstPushedRef gets the first pushed ref that we should treat as the
// primate remote branch. It's generally speaking the firt branch pushed
// so usually will be main. However, if master is pushed instead of main,
// it will be that, or if `foo` is pushed first without a main or master, it
// will become `foo`. This seems to match the behavior of github and also
// standard local git. There should be a knob to change this after the fact
// since we are essentially guessing the user's intentions here. Note that if
// you pass a reference name through to init and the repository is already
// initialized, it will be ignored.
func (c *PushCmd) firstPushedRef() *plumbing.ReferenceName {
	var hasMaster, hasMain bool
	var ret *plumbing.ReferenceName
	for _, ref := range c.pushes {
		if ref.IsWildcard() || ref.IsDelete() {
			continue
		}
		dst := ref.Dst("")
		switch {
		case dst == plumbing.Main:
			hasMain = true
		case dst == plumbing.Master:
			hasMaster = true
		case ret == nil:
			ret = &dst
		}
	}

	// if we're pushing multiple branches and we have either main or
	// master, prefer those, preferring main first.
	if hasMain {
		tmp := plumbing.Main
		return &tmp
	}
	if hasMaster {
		tmp := plumbing.Master
		return &tmp
	}

	// Otherwise, we'll take the first pushed ref, which usually turns
	// out to be the first alphabetically.
	return ret
}

func (c *PushCmd) openFetchFrom(m MetaContext) error {

	// In an interesting hack borrowed from the very clever KBFS git implementation,
	// we set the "fetchFrom" up as the local, the local up as the fetchFrom, and then implement
	// push from the local to the fetchFrom as a fetch from the fetchFrom to the local.
	// So here "fetchFrom" is the local working directory that the git fetchFrom helper is working in.
	var err error

	c.fetchFrom, err = m.ReverseRemote(GitOpTypePush, c.firstPushedRef())
	if err != nil {
		return err
	}
	return nil
}

func (c *PushCmd) doDeletes(m MetaContext) error {
	stor, err := m.E().Storage().ToGitStorage(GitOpTypePush)
	if err != nil {
		return err
	}
	for _, ref := range c.deletes {
		dst := ref.Dst("")
		err := stor.RemoveReference(dst)
		c.results[dst.String()] = err
	}
	return nil
}

func (c *PushCmd) push(m MetaContext) (err error) {
	if len(c.pushes) == 0 {
		return nil
	}

	m.TermLogf(TermLogNoNewline, "️⬆️  Pushing objects to remote ... ")
	defer func() { m.TermLogStatus(err) }()

	m.DbgLogf("pushing %d refs", len(c.pushes))

	// Recall that the "fetchFrom" here is actually local. So we're fetching
	// from local to remote, which is the same as pushing from local to remote.
	err = c.fetchFrom.FetchContext(m.Ctx(), &gogit.FetchOptions{
		RemoteName: c.fetchFrom.Name(),
		RefSpecs:   c.pushes,
	})
	m.DbgLogf("pushed -> %v", err)
	if err == gogit.NoErrAlreadyUpToDate {
		err = nil
	}
	// All refs get the same error
	for _, ref := range c.pushes {
		c.results[ref.Dst("").String()] = err
	}
	return nil
}

func (c *PushCmd) output(m MetaContext, e error) error {
	if e != nil {
		return c.DoOutput(nil, e)
	}
	var oset OutputSet
	for ref, err := range c.results {
		msg := "ok"
		if err != nil {
			msg = err.Error()
		}
		oset.Parts(msg, ref)
	}
	return c.DoOutput(oset.Empty().Res(), nil)
}

func (c *PushCmd) Run(m MetaContext) error {
	err := c.prepareOutput(m)
	err = c.output(m, err)
	if err != nil {
		return err
	}
	return nil
}

func (c *PushCmd) prepareOutput(m MetaContext) error {
	err := c.init(m)
	if err != nil {
		return err
	}
	err = m.InitPackSyncPush()
	if err != nil {
		return err
	}
	err = c.openFetchFrom(m)
	if err != nil {
		return err
	}
	err = c.doDeletes(m)
	if err != nil {
		return err
	}
	err = c.push(m)
	if err != nil {
		return err
	}
	return nil
}
