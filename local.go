package remhelp

import (
	"github.com/go-git/go-billy/v5/osfs"
	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/storage/filesystem"
)

type LocalRepo struct {
	storage  *filesystem.Storage
	dir      string
	defBrnch string
}

func NewLocalRepo() *LocalRepo {
	return &LocalRepo{dir: "./.git"}
}

func (r *LocalRepo) Open() error {
	localGit := osfs.New(r.dir)
	buf := cache.NewObjectLRUDefault()
	localStorer := filesystem.NewStorage(localGit, buf)
	r.storage = localStorer
	repo, err := gogit.Open(localStorer, nil)
	if err != nil {
		return err
	}
	cfg, err := repo.Config()
	if err != nil {
		return err
	}
	r.defBrnch = cfg.Init.DefaultBranch
	return nil
}
