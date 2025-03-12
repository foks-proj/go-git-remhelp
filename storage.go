package remhelp

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/memory"
)

type RefListIter interface {
	HasAnyHashes() bool
	Next() (*plumbing.Reference, error)
	ForEach(func(*plumbing.Reference) error) error
	Close()
}

type RefListSliceIter struct {
	refs []*plumbing.Reference
	pos  int
}

func NewRefListSliceIter(refs []*plumbing.Reference) *RefListSliceIter {
	return &RefListSliceIter{
		refs: refs,
	}
}

func (r *RefListSliceIter) HasAnyHashes() bool {
	for _, ref := range r.refs {
		if ref.Type() == plumbing.HashReference {
			return true
		}
	}
	return false
}

func (r *RefListSliceIter) Next() (*plumbing.Reference, error) {
	if r.pos >= len(r.refs) {
		return nil, io.EOF
	}
	ret := r.refs[r.pos]
	r.pos++
	return ret, nil
}

func (r *RefListSliceIter) ForEach(f func(*plumbing.Reference) error) error {
	for _, ref := range r.refs {
		if err := f(ref); err != nil {
			return err
		}
	}
	return nil
}

func (r *RefListSliceIter) Close() {
	r.pos = len(r.refs)
}

var _ RefListIter = &RefListSliceIter{}

type GitOpType int

const (
	GitOpTypePush GitOpType = iota
	GitOpTypeFetch
)

type Storer interface {
	// For the go-git Storage interface
	ToGitStorage(op GitOpType) (storage.Storer, error)
	PackSync() *PackSync

	List() (RefListIter, error)
	Name() RemoteName
	URL() RemoteURL
	LocalCheckoutDir() LocalCheckoutDir
}

type MemoryStorageFactory struct {
	stores map[RemoteURL]*MemoryStorageBase
}

type RemoteName string
type RemoteURL string
type LocalCheckoutDir string
type LocalPath string

func NewLocalCheckoutDirFromDotGit(p LocalPath) (LocalCheckoutDir, error) {
	parent, base := filepath.Split(string(p))
	if base != ".git" {
		return "", BadGitPathError{Path: p}
	}
	return LocalCheckoutDir(parent), nil
}

func NewLocalCheckDirFromWorkingDirAndGitDir(
	wd LocalPath,
	gd LocalPath,
) (
	LocalCheckoutDir,
	error,
) {
	var tmp LocalPath
	if len(gd) > 0 && filepath.IsAbs(string(gd)) {
		tmp = gd
	} else {
		tmp = LocalPath(filepath.Join(string(wd), string(gd)))
	}
	return NewLocalCheckoutDirFromDotGit(tmp)
}

func (l LocalCheckoutDir) String() string { return string(l) }

func (l LocalCheckoutDir) DotGit() string {
	return filepath.Join(string(l), ".git")
}

type SyncedPack struct {
	Name  IndexName
	Index []byte
	Pack  []byte
	Time  time.Time
}

type MemoryStorageBase struct {
	*memory.Storage
	nm  RemoteName
	url RemoteURL

	packMu sync.RWMutex
	packs  map[IndexName]*SyncedPack
}

func (m *MemoryStorageBase) WithLocal(
	ps *PackSync,
	lcd LocalCheckoutDir,
) *MemoryStorage {
	return &MemoryStorage{
		MemoryStorageBase: m,
		ps:                ps,
		lcd:               lcd,
	}
}

func (m *MemoryStorageBase) HasIndex(
	ctx context.Context,
	name IndexName,
) (
	bool,
	error,
) {
	m.packMu.RLock()
	defer m.packMu.RUnlock()
	_, ok := m.packs[name]
	return ok, nil
}

func (m *MemoryStorageBase) FetchNewIndices(
	ctx context.Context,
	since time.Time,
) (
	[]RawIndex,
	error,
) {
	m.packMu.RLock()
	defer m.packMu.RUnlock()

	var ret []RawIndex
	for _, pack := range m.packs {
		if !pack.Time.Before(since) {
			ret = append(ret, RawIndex{
				Name:  pack.Name,
				Data:  pack.Index,
				CTime: pack.Time,
			})
		}
	}
	return ret, nil
}

func (m *MemoryStorageBase) FetchPackData(
	ctx context.Context,
	name IndexName,
	wc io.Writer,
) error {
	m.packMu.RLock()
	defer m.packMu.RUnlock()

	pack, ok := m.packs[name]
	if !ok {
		return plumbing.ErrObjectNotFound
	}
	_, err := wc.Write(pack.Pack)
	if err != nil {
		return err
	}
	return nil
}

func (m *MemoryStorageBase) getOrMakePack(name IndexName) *SyncedPack {
	obj := m.packs[name]
	if obj != nil {
		return obj
	}
	obj = &SyncedPack{
		Name: name,
		Time: time.Now(),
	}
	m.packs[name] = obj
	return obj
}

func (m *MemoryStorageBase) PushPackData(
	ctx context.Context,
	name IndexName,
	rc io.Reader,
) error {
	m.packMu.Lock()
	defer m.packMu.Unlock()

	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	m.getOrMakePack(name).Pack = data
	return nil
}

func (m *MemoryStorageBase) PushPackIndex(
	ctx context.Context,
	name IndexName,
	rc io.Reader,
) error {
	m.packMu.Lock()
	defer m.packMu.Unlock()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	obj := m.getOrMakePack(name)
	obj.Index = data
	obj.Time = time.Now()
	return nil
}

type MemoryStorage struct {
	*MemoryStorageBase

	// these two fields vary with the remote and the local.
	ps  *PackSync
	lcd LocalCheckoutDir
}

var _ Storer = (*MemoryStorage)(nil)
var _ PackSyncRemoter = (*MemoryStorageBase)(nil)

func NewMemoryStorageBase(nm RemoteName, url RemoteURL) *MemoryStorageBase {
	tmp := memory.NewStorage()
	return &MemoryStorageBase{
		Storage: tmp,
		url:     url,
		nm:      nm,
		packs:   make(map[IndexName]*SyncedPack),
	}
}

func (s *MemoryStorage) ToGitStorage(op GitOpType) (storage.Storer, error) {
	return NewPackSyncStorage(s.Storage, s.ps, op)
}

func (s *MemoryStorage) List() (RefListIter, error) {

	if s.Storage == nil {
		return nil, errors.New("no storage")
	}

	refs, err := s.Storage.IterReferences()
	if err != nil {
		return nil, err
	}
	var v []*plumbing.Reference
	refs.ForEach(func(ref *plumbing.Reference) error {
		v = append(v, ref)
		return nil
	})

	return NewRefListSliceIter(v), nil
}

func (s *MemoryStorage) Name() RemoteName                   { return s.nm }
func (s *MemoryStorage) URL() RemoteURL                     { return s.url }
func (s *MemoryStorage) PackSync() *PackSync                { return s.ps }
func (s *MemoryStorage) LocalCheckoutDir() LocalCheckoutDir { return s.lcd }

func (s *MemoryStorage) HasEncodedObject(h plumbing.Hash) error {
	return s.Storage.HasEncodedObject(h)
}

type StorerFactory interface {
	New(nm RemoteName, url RemoteURL, lcd LocalCheckoutDir) (Storer, error)
}

func NewMemoryStorageFactory() *MemoryStorageFactory {
	return &MemoryStorageFactory{
		stores: make(map[RemoteURL]*MemoryStorageBase),
	}
}

func (f *MemoryStorageFactory) New(nm RemoteName, url RemoteURL, lcd LocalCheckoutDir) (Storer, error) {
	base := f.stores[url]
	if base == nil {
		base = NewMemoryStorageBase(nm, url)
		f.stores[url] = base
	}
	ps, err := NewPackSyncFromPath(nm, base, lcd, &NilTermLogger{})
	if err != nil {
		return nil, err
	}
	ret := base.WithLocal(ps, lcd)
	return ret, nil
}

var _ StorerFactory = &MemoryStorageFactory{}

type StorageWrapper struct {
	storage storage.Storer
	ps      *PackSync
	nm      RemoteName
	url     RemoteURL
	lcd     LocalCheckoutDir
}

func NewStorageWrapper(
	storage storage.Storer,
	nm RemoteName,
	url RemoteURL,
	ps *PackSync,
	lcd LocalCheckoutDir,
) (*StorageWrapper, error) {

	return &StorageWrapper{
		storage: storage,
		nm:      nm,
		url:     url,
		ps:      ps,
		lcd:     lcd,
	}, nil
}

func (w *StorageWrapper) List() (RefListIter, error) {
	refs, err := w.storage.IterReferences()
	if err != nil {
		return nil, err
	}
	var v []*plumbing.Reference
	refs.ForEach(func(ref *plumbing.Reference) error {
		v = append(v, ref)
		return nil
	})
	return NewRefListSliceIter(v), nil
}

func (w *StorageWrapper) ToGitStorage(op GitOpType) (storage.Storer, error) {
	return NewPackSyncStorage(w.storage, w.ps, op)
}

func (w *StorageWrapper) HasEncodedObject(h plumbing.Hash) error {
	return w.storage.HasEncodedObject(h)
}

func (w *StorageWrapper) Name() RemoteName                   { return w.nm }
func (w *StorageWrapper) URL() RemoteURL                     { return w.url }
func (w *StorageWrapper) PackSync() *PackSync                { return w.ps }
func (w *StorageWrapper) LocalCheckoutDir() LocalCheckoutDir { return w.lcd }

var _ Storer = &StorageWrapper{}

type StorageWrapperFactory struct {
	sw *StorageWrapper
}

func (s *StorageWrapperFactory) New(nm RemoteName, url RemoteURL, lcd LocalCheckoutDir) (Storer, error) {
	if s.sw.nm != nm {
		return nil, errors.New("storage wrapper factory: bad name")
	}
	if s.sw.url != url {
		return nil, errors.New("storage wrapper factory: bad url")
	}
	return s.sw, nil
}

var _ StorerFactory = &StorageWrapperFactory{}
