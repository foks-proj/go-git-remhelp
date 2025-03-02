package remhelp

import (
	"context"
	"errors"
	"fmt"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
)

type MetaContext struct {
	ctx context.Context
	e   *ExecContext
}

func (m MetaContext) Ctx() context.Context {
	return m.ctx
}

func (m MetaContext) E() *ExecContext {
	return m.e
}

type ExecContext struct {
	storage Storer
	opts    RemoteHelperOptions
}

func (e *ExecContext) Storage() Storer     { return e.storage }
func (e *ExecContext) DbgLog() DebugLogger { return e.opts.DbgLog }

func (e *ExecContext) TermLog() TermLogger {
	ret := e.opts.TermLog
	if ret == nil {
		ret = &NilTermLogger{}
	}
	return ret
}

func (m MetaContext) DbgLogCmd(s string)    { LogInput(m.E().DbgLog(), s) }
func (m MetaContext) DbgLogOutput(s string) { LogOutput(m.E().DbgLog(), s) }

func (m MetaContext) DbgLogf(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	m.E().DbgLog().Log(s + "\n")
}

func (m MetaContext) TermLogf(opts TermLogBitfield, format string, args ...interface{}) {
	TermLogf(m.Ctx(), m.E().TermLog(), opts, format, args...)
}

func (m MetaContext) TermLogStatus(err error) {
	if err == nil {
		m.TermLogf(TermLogStd, "✅ done")
	} else {
		m.TermLogf(TermLogStd, "❌ failed: %v", err)
	}
}

func NewMetaContextBackground(e *ExecContext) MetaContext {
	return MetaContext{
		ctx: context.Background(),
		e:   e,
	}
}

func NewMetaContext(ctx context.Context, e *ExecContext) MetaContext {
	return MetaContext{
		ctx: ctx,
		e:   e,
	}
}

func NewGoGitRepoWithStorage(stor storage.Storer) (*gogit.Repository, error) {

	// We're initializing with a `nil` FileSystem so therefore we should
	// get a "bare" repository, which is what we want. But there's a bug,
	// we're still making a HEAD ref, which we don't want. Either way,
	// let's use the modern nominal value for the default branch.
	repo, err := gogit.InitWithOptions(stor, nil, gogit.InitOptions{
		DefaultBranch: plumbing.Main,
	})

	if err == nil {
		return repo, nil
	}
	if err != gogit.ErrRepositoryAlreadyExists {
		return nil, err
	}
	repo, err = gogit.Open(stor, nil)
	if err != nil {
		return nil, err
	}
	return repo, nil
}

func (m MetaContext) RemoteRepo(op GitOpType) (*gogit.Repository, error) {
	storage, err := m.E().Storage().ToGitStorage(op)
	if err != nil {
		return nil, err
	}
	return NewGoGitRepoWithStorage(storage)
}

type ReverseRemote struct {
	*gogit.Remote
	name string
}

func (r *ReverseRemote) Name() string { return r.name }

func (m MetaContext) ReverseRemote(
	op GitOpType,
) (
	*ReverseRemote,
	error,
) {
	remote, err := m.RemoteRepo(op)
	if err != nil {
		return nil, err
	}

	s, err := randomHex(16)
	if err != nil {
		return nil, err
	}

	name := "local-" + s

	_, err = remote.Remote(name)
	if err == nil {
		return nil, errors.New("remote already exists; is not expected since name is random")
	}

	tmp, err := remote.CreateRemote(&config.RemoteConfig{
		Name: name,
		URLs: []string{m.e.Storage().LocalCheckoutDir().String()},
	})
	if err != nil {
		return nil, err
	}

	ret := &ReverseRemote{
		Remote: tmp,
		name:   name,
	}
	return ret, nil
}

func (m MetaContext) Repack() (bool, error) {
	e := m.E()
	return GitRepack(
		m,
		GitRepackArg{
			Git:    e.opts.GitCmd,
			Dir:    e.Storage().LocalCheckoutDir().String(),
			Thresh: e.opts.RepackThreshhold,
		},
	)
}

func (m MetaContext) InitPackSyncPush() error {
	ps := m.E().storage.PackSync()
	if ps == nil {
		return nil
	}

	_, err := m.Repack()
	if err != nil {
		return err
	}

	err = ps.InitPush(m)
	if err != nil {
		return err
	}
	return nil
}
