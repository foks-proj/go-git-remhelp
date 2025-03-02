package remhelp

import (
	"fmt"
	"strings"

	"github.com/go-git/go-billy/v5/osfs"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/go-git/go-git/v5/storage/filesystem"
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

func (c *PushCmd) openFetchFrom(m MetaContext) error {

	// In an interesting hack borrowed from the very clever KBFS git implementation,
	// we set the "fetchFrom" up as the local, the local up as the fetchFrom, and then implement
	// push from the local to the fetchFrom as a fetch from the fetchFrom to the local.
	// So here "fetchFrom" is the local working directory that the git fetchFrom helper is working in.
	var err error
	c.fetchFrom, err = m.ReverseRemote(GitOpTypePush)
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

// fixMaster deals with the fact that the remote might have a default branch of master,
// and the local might have a default branch of main, or vice versa. We remedy this
// situation by rewriting `HEAD` on the remote if there is no corresponding default
// branch on local, but the other default branch is found. This seems pretty safe
// but feels a bit hacky.
func (c *PushCmd) fixMaster(m MetaContext) error {
	localStorer := filesystem.NewStorage(
		osfs.New(m.e.Storage().LocalCheckoutDir().DotGit()),
		cache.NewObjectLRUDefault(),
	)

	remoteStorer, err := m.E().Storage().ToGitStorage(GitOpTypePush)
	if err != nil {
		return err
	}
	remoteRef, err := remoteStorer.Reference(plumbing.HEAD)

	// If we can't find the head reference on the remote, that's ok, no repair needed.
	if err != nil {
		return nil
	}

	// go-git library writes HEAD as `ref: refs/heads/main HEAD`. I'm not sure why,
	// but we need to strip the `HEAD` part to get the branch name.
	strip := func(n plumbing.ReferenceName) plumbing.ReferenceName {
		v := strings.Split(n.String(), " ")
		return plumbing.ReferenceName(v[0])
	}

	remoteTarg := strip(remoteRef.Target())

	var toFind plumbing.ReferenceName
	var switchTo plumbing.ReferenceName
	switch remoteTarg {
	case plumbing.Main:
		toFind = remoteTarg
		switchTo = plumbing.Master
	case plumbing.Master:
		toFind = remoteTarg
		switchTo = plumbing.Main
	default:
		// If remote HEAD is something other than master or main, then no fix.
		return nil
	}

	ref, err := localStorer.ReferenceStorage.Reference(toFind)
	if err == nil && ref != nil {
		// We found a local branch that matches the remote HEAD, ok to no fix
		return nil
	}

	ref, err = localStorer.ReferenceStorage.Reference(switchTo)
	if err != nil || ref == nil {
		// We don't have a local master or main, so no fix needed.
		return nil
	}

	err = remoteStorer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, switchTo))
	if err != nil {
		return err
	}
	return nil
}

func (c *PushCmd) Run(m MetaContext) error {
	err := c.prepareOutput(m)
	err = c.output(m, err)
	if err != nil {
		return err
	}

	err = c.fixMaster(m)
	if err != nil {
		m.DbgLogf("error fixing master: %v", err)
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
