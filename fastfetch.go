package remhelp

import (
	"context"
	"errors"
	"io"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/revlist"
	"github.com/go-git/go-git/v5/storage"
)

// FastFetch is a specialized fetcher that combines a PackSync and low-level
// go-git access, to provide an optimized fetch backend.
type FastFetch struct {
	storage.Storer // remote FOKS storage

	wantObjs  []plumbing.Hash
	haveObjs  []plumbing.Hash
	ps        *PackSync
	remote    storage.Storer
	remoteGit *gogit.Repository // XXX might not need this!
	synced    map[plumbing.Hash]bool
	all       []plumbing.Hash
	tl        TermLogger
}

func dedupeAndCleanupHashes(hs []plumbing.Hash) []plumbing.Hash {
	tmp := make(map[plumbing.Hash]struct{})
	for _, h := range hs {
		if !h.IsZero() {
			tmp[h] = struct{}{}
		}
	}
	ret := make([]plumbing.Hash, 0, len(tmp))
	for h := range tmp {
		ret = append(ret, h)
	}
	return ret
}

func lmap[T, U any](xs []T, f func(T) U) []U {
	ret := make([]U, 0, len(xs))
	for _, x := range xs {
		ret = append(ret, f(x))
	}
	return ret
}

func NewFastFetch(
	m MetaContext,
	args []FetchArg,
) (
	*FastFetch,
	error,
) {

	// figure out which hashes we need, starting from the tip
	// commit. We'll then spider the tree to find all the objects
	// in the transitive closure of the commit graph.
	wantObjs := dedupeAndCleanupHashes(lmap(args, func(arg FetchArg) plumbing.Hash { return arg.Hash }))

	storage := m.E().Storage()
	ps := storage.PackSync()
	if ps == nil {
		return nil, InternalError("pack sync not available")
	}

	remote, err := storage.ToGitStorage(GitOpTypeFetch)
	if err != nil {
		return nil, err
	}
	rep, err := NewGoGitRepoWithStorage(remote)
	if err != nil {
		return nil, err
	}

	return &FastFetch{
		Storer:    remote,
		wantObjs:  wantObjs,
		ps:        ps,
		remote:    remote,
		remoteGit: rep,
		synced:    make(map[plumbing.Hash]bool),
		tl:        m.E().TermLog(),
	}, nil

}

func (f *FastFetch) Run(m MetaContext) error {

	err := f.initPackSync(m)
	if err != nil {
		return err
	}
	err = f.readAllLocalRefs(m)
	if err != nil {
		return err
	}
	err = f.walkCommitGraph(m)
	if err != nil {
		return err
	}
	err = f.syncAllObjects(m)
	if err != nil {
		return err
	}
	return nil
}

func (f *FastFetch) walkCommitGraph(m MetaContext) (err error) {
	m.TermLogf(TermLogNoNewline|TermLogCr, MsgFetchObjects)
	defer func() {
		m.TermLogStatus(err)
	}()
	f.all, err = revlist.ObjectsWithStorageForIgnores(
		f,
		f.ps.localGit,
		f.wantObjs,
		f.haveObjs,
	)
	if err != nil {
		return err
	}
	return nil
}

func (f *FastFetch) syncAllObjects(m MetaContext) (err error) {
	update := func(i, n int) {
		if n < 10000 || i%100 == 0 {
			m.TermLogf(TermLogNoNewline|TermLogCr, "⬇️  Downloading remaining objects %.4f%% (%d of %d) ... ",
				float64(i)*float64(100)/float64(n), i, n)
		}
	}
	update(0, len(f.all))
	defer func() {
		m.TermLogStatus(err)
	}()
	for i, h := range f.all {
		update(i, len(f.all))
		if f.synced[h] {
			continue
		}
		_, err = f.getEncodedObject(plumbing.AnyObject, h, false)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *FastFetch) initPackSync(m MetaContext) error {
	m.TermLogf(TermLogNoNewline, "🏗️  Building pack index ... ")
	err := f.ps.InitFastFetch(m.Ctx())
	m.TermLogStatus(err)
	if err != nil {
		return err
	}
	return nil
}

func (f *FastFetch) readAllLocalRefs(
	m MetaContext,
) error {
	refIter, err := f.ps.localGit.ReferenceStorage.IterReferences()
	if err != nil {
		return err
	}
	var tmp []plumbing.Hash
	for {
		ref, err := refIter.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if ref == nil {
			break
		}
		if ref.Type() == plumbing.SymbolicReference {
			continue
		}
		tmp = append(tmp, ref.Hash())
	}
	f.haveObjs = dedupeAndCleanupHashes(tmp)
	return nil
}

func isFatalErr(e error) bool {
	return e != nil && e != plumbing.ErrObjectNotFound
}
func shouldReturn(e error) bool {
	return e == nil || isFatalErr(e)
}

// getEncodedObjectViaLocalOrPacks tries to get an object from the
// local git, or from local packfiles. If it finds the objects in
// a partial pack file (one for which the index is synced but not
// the data), it will pull down the pack the object is in, reindex,
// and fetch from local. If neither strategy works, it returns
// ErrObjectNotFound.
func (f *FastFetch) getEncodedObjectViaLocalOrPacks(
	ctx context.Context,
	typ plumbing.ObjectType,
	hash plumbing.Hash,
) (
	plumbing.EncodedObject,
	error,
) {
	getLocal := func() (plumbing.EncodedObject, error) {
		return f.ps.localGit.EncodedObject(typ, hash)
	}

	// We have some local .idx files in our partial index directory. Check
	// them first. If we do get a hit, sync down the corresponding .pack
	// file, reindex, and then retry the local storage.
	// Note that syncPartialIndex returns ErrObjectNotFound if the object is not in the
	// partial indices. IF we get a nil error, that means we have the object
	// in the local git store (in the pack files), and we should be able to fetch
	// from there
	syncAndGet := func() (plumbing.EncodedObject, error) {
		err := f.ps.syncPartialIndex(ctx, hash)
		if err != nil {
			return nil, err
		}
		return getLocal()
	}

	ret, err := getLocal()
	if shouldReturn(err) {
		return ret, err
	}

	ret, err = syncAndGet()
	if shouldReturn(err) {
		return ret, err
	}

	return nil, plumbing.ErrObjectNotFound
}

func (f *FastFetch) getEncodedObject(
	typ plumbing.ObjectType,
	hash plumbing.Hash,
	doLog bool,
) (
	plumbing.EncodedObject,
	error,
) {
	ctx := context.Background()
	ret, err := f.getEncodedObjectViaLocalOrPacks(ctx, typ, hash)
	if err == nil {
		f.synced[hash] = true
		return ret, nil
	}
	if shouldReturn(err) {
		return ret, err
	}

	// If we sucked down an encoded object during walkCommitGraph,
	// then it should be addeed to the local object storage,
	// as a loose object. Do that here.
	if doLog {
		TermLogMsgFetch(ctx, f.tl, "obj", hash)
	}
	ret, err = f.remote.EncodedObject(typ, hash)
	if err != nil {
		return nil, err
	}
	_, err = f.ps.localGit.ObjectStorage.SetEncodedObject(ret)
	if err != nil {
		return nil, err
	}
	f.synced[hash] = true
	return ret, nil
}

func (f *FastFetch) EncodedObject(
	typ plumbing.ObjectType,
	hash plumbing.Hash,
) (
	plumbing.EncodedObject,
	error,
) {
	return f.getEncodedObject(typ, hash, true)
}

func (f *FastFetch) HasEncodedObject(
	hash plumbing.Hash,
) error {
	_, err := f.getEncodedObject(plumbing.AnyObject, hash, true)
	return err
}

func (f *FastFetch) EncodedObjectSize(
	hash plumbing.Hash,
) (
	int64,
	error,
) {
	obj, err := f.getEncodedObject(plumbing.AnyObject, hash, true)
	if err != nil {
		return 0, err
	}
	return obj.Size(), nil
}

var _ storage.Storer = (*FastFetch)(nil)
