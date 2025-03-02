package remhelp

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/format/idxfile"
	"github.com/go-git/go-git/v5/storage"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/utils/ioutil"
)

type IndexName string

const packSyncTopDir = "foks-pack-sync"
const partialIndexDirName = "ppi" // pack partial index
const packPrefix = "pack-"
const packSuffixData = ".pack"
const packSuffixIndex = ".idx"
const partialFetchTimeFile = "partial-fetch-time"
const packPushTimeFile = "pack-push-time"
const pushManifestFile = "push-manifest" // All packs we have pushed to server
const pullManifestFile = "pull-manifest" // All idx's we have pulled from server

var packDirName = filepath.Join("objects", "pack")

type Index struct {
	Name     IndexName
	Filename string
	Sz       int64
	NumObjs  int
	Raw      []byte
	Idx      *idxfile.MemoryIndex
	Ctime    time.Time
}

var hexRxx = regexp.MustCompile("^[0-9a-fA-F]+$")

func IsPackIndexFile(f string) bool {
	return strings.HasPrefix(f, packPrefix) &&
		strings.HasSuffix(f, packSuffixIndex) &&
		hexRxx.MatchString(f[len(packPrefix):len(f)-len(packSuffixIndex)])
}

func IsPackDataFile(f string) bool {
	return strings.HasPrefix(f, packPrefix) &&
		strings.HasSuffix(f, packSuffixData) &&
		hexRxx.MatchString(f[len(packPrefix):len(f)-len(packSuffixData)])
}

type PackFileType int

const (
	PackFileTypeNone  PackFileType = 0
	PackFileTypeData  PackFileType = (1 << 0)
	PackFileTypeIndex PackFileType = (1 << 1)
	PackFileTypeBoth  PackFileType = (PackFileTypeData | PackFileTypeIndex)
)

type PackFileGeneric struct {
	Name IndexName
	Type PackFileType
}

func (n IndexName) IsValid() bool {
	return hexRxx.MatchString(string(n))
}

func (n IndexName) String() string { return string(n) }

func ParsePackFileGeneric(f string) *PackFileGeneric {
	if !strings.HasPrefix(f, packPrefix) {
		return nil
	}
	var ret PackFileGeneric
	var sffx string
	switch {
	case strings.HasSuffix(f, packSuffixData):
		sffx = packSuffixData
		ret.Type = PackFileTypeData
	case strings.HasSuffix(f, packSuffixIndex):
		sffx = packSuffixIndex
		ret.Type = PackFileTypeIndex
	default:
		return nil
	}
	ret.Name = IndexName(f[len(packPrefix) : len(f)-len(sffx)])
	if !ret.Name.IsValid() {
		return nil
	}
	return &ret
}

func ParsePackIndex(f string) IndexName {
	var zed IndexName
	ret := ParsePackFileGeneric(f)
	if ret == nil || ret.Type != PackFileTypeIndex {

		return zed
	}
	return ret.Name
}

func (i *Index) PackDataFilename() string {
	return i.Name.PackDataFilename()
}

func (i IndexName) PackDataFilename() string {
	return packPrefix + string(i) + packSuffixData
}

func (i IndexName) PackIndexFilename() string {
	return packPrefix + string(i) + packSuffixIndex
}

func (i IndexName) IsZero() bool { return i == "" }

func (i *Index) PackIndexFilename() string {
	return i.Name.PackIndexFilename()
}

func (i *Index) PartialIndexPath(parent string) string {
	return filepath.Join(parent, i.PackIndexFilename())
}

func (i *Index) FullIndexPath() string {
	return filepath.Join(packDirName, i.PackIndexFilename())
}

type ComingledIndex struct {
	sync.RWMutex
	Indices map[IndexName]*Index
	Objects map[plumbing.Hash][]*Index
	Newest  time.Time
}

func (c *ComingledIndex) NumIndicies() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.Indices)
}

func (c *ComingledIndex) Get(h plumbing.Hash) []*Index {
	c.RLock()
	defer c.RUnlock()
	return c.Objects[h]
}

func NewComingledIndex() *ComingledIndex {
	return &ComingledIndex{
		Indices: make(map[IndexName]*Index),
		Objects: make(map[plumbing.Hash][]*Index),
	}
}

type RawIndex struct {
	Name  IndexName
	Data  []byte
	CTime time.Time
}

func (r *RawIndex) Import() (*Index, error) {
	idxf := idxfile.NewMemoryIndex()
	buf := bytes.NewReader(r.Data)
	d := idxfile.NewDecoder(buf)
	err := d.Decode(idxf)
	if err != nil {
		return nil, err
	}
	cnt, err := idxf.Count()
	if err != nil {
		return nil, err
	}
	return &Index{
		Name:    r.Name,
		Idx:     idxf,
		Raw:     r.Data,
		Ctime:   r.CTime,
		Sz:      int64(len(r.Data)),
		NumObjs: int(cnt),
	}, nil
}

type PackSyncRemoter interface {
	HasIndex(ctx context.Context, name IndexName) (bool, error)
	FetchNewIndices(ctx context.Context, since time.Time) ([]RawIndex, error)
	FetchPackData(ctx context.Context, name IndexName, wc io.Writer) error
	PushPackData(ctx context.Context, name IndexName, rc io.Reader) error
	PushPackIndex(ctx context.Context, name IndexName, rc io.Reader) error
}

type PackSync struct {
	rn        RemoteName
	rem       PackSyncRemoter
	dotGitDir billy.Filesystem
	localGit  *filesystem.Storage
	fetchCI   *ComingledIndex
	tl        TermLogger

	initFetchMu  sync.Mutex
	didInitFetch bool

	rpiMu         sync.Mutex
	didRpiRefresh bool

	dirMu           sync.Mutex
	packDir         billy.Filesystem
	topDir          billy.Filesystem
	partialIndexDir billy.Filesystem

	manifestMu sync.Mutex

	initPushMu  sync.RWMutex
	pushCI      *ComingledIndex
	didInitPush bool
}

func NewPackSync(
	rn RemoteName,
	rem PackSyncRemoter,
	localGit *filesystem.Storage,
	dotGitDir billy.Filesystem,
	tl TermLogger,
) *PackSync {
	return &PackSync{
		rn:        rn,
		rem:       rem,
		localGit:  localGit,
		dotGitDir: dotGitDir,
		fetchCI:   NewComingledIndex(),
		tl:        tl,
	}
}

func NewPackSyncFromPath(
	rn RemoteName,
	rem PackSyncRemoter,
	lcd LocalCheckoutDir,
	tl TermLogger,
) (
	*PackSync,
	error,
) {
	fs := osfs.New(lcd.DotGit())
	localGitStore := filesystem.NewStorage(fs, cache.NewObjectLRUDefault())
	ps := NewPackSync(rn, rem, localGitStore, fs, tl)
	return ps, nil
}

func (p *PackSync) packSyncTopDirName() string {
	return filepath.Join(packSyncTopDir, string(p.rn))
}

func (p *PackSync) partialIndexDirName() string {
	return filepath.Join(p.packSyncTopDirName(), partialIndexDirName)
}

// InitFastFetch initializes the packsync object for a fetch operation.
// It first builds an index from the on-disk partial indices, then fetches
// new indices from the server, and finally updates the index with the new
// indices.
func (p *PackSync) InitFastFetch(ctx context.Context) error {
	p.initFetchMu.Lock()
	defer p.initFetchMu.Unlock()

	if p.didInitFetch {
		return nil
	}
	err := p.readIndicesFromDir(ctx, p.fetchCI, p.partialIndexDirName(), nil)
	if err != nil {
		return err
	}
	_, err = p.refreshPartialIndex(ctx, p.fetchCI)
	if err != nil {
		return err
	}
	p.didInitFetch = true
	return nil
}

func fileToIndexName(f string) (IndexName, error) {
	ext := filepath.Ext(f)
	base := f[0 : len(f)-len(ext)]
	if len(base) < 2 {
		return "", BadIndexNameError{}
	}
	prfx := "pack-"
	if !strings.HasPrefix(base, prfx) {
		return "", BadIndexNameError{}
	}
	return IndexName(base[len(prfx):]), nil
}

func (c *ComingledIndex) Absorb(idx *Index) error {
	c.Lock()
	defer c.Unlock()

	// Have already absorbed this index
	if c.Indices[idx.Name] != nil {
		return nil
	}

	c.Indices[idx.Name] = idx
	iter, err := idx.Idx.Entries()
	if err != nil {
		return err
	}

	for {
		entry, err := iter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		tab := c.Objects
		tab[entry.Hash] = append(tab[entry.Hash], idx)
	}

	return nil
}

// The pushManifest keeps track of all the local packs we have pusehd to the server.
func (p *PackSync) writeIndexToPushManifest(
	ctx context.Context,
	nm IndexName,
) error {
	return p.writeIndexToManifest(ctx, nm, pushManifestFile)
}

func (p *PackSync) writeIndexToPullManifest(
	ctx context.Context,
	nm IndexName,
) error {
	return p.writeIndexToManifest(ctx, nm, pullManifestFile)
}

func (p *PackSync) writeIndexToManifest(
	ctx context.Context,
	nm IndexName,
	fn string,
) error {
	p.manifestMu.Lock()
	defer p.manifestMu.Unlock()

	dir, err := p.prepPackSyncTopDir()
	if err != nil {
		return err
	}

	fh, err := dir.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(fh, &err)
	_, err = fh.Write([]byte(nm + "\n"))
	if err != nil {
		return err
	}
	return nil
}

func (p *PackSync) hasIndexSyncedFull(
	ctx context.Context,
	nm IndexName,
) (
	bool,
	error,
) {
	packDir, err := p.prepPackDir()
	if err != nil {
		return false, err
	}
	_, err = packDir.Stat(nm.PackIndexFilename())
	if err == nil {
		return true, nil
	}
	return false, nil
}

func (p *PackSync) hasIndexSyncedPartial(
	ctx context.Context,
	nm IndexName,
) (
	bool,
	error,
) {
	partial, err := p.prepPartialIndexDir()
	if err != nil {
		return false, err
	}
	_, err = partial.Stat(nm.PackIndexFilename())
	if err == nil {
		return true, nil
	}
	return false, nil
}

func (p *PackSync) hasIndexSynced(
	ctx context.Context,
	nm IndexName,
) (
	bool,
	error,
) {
	ok, err := p.hasIndexSyncedFull(ctx, nm)
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}
	return p.hasIndexSyncedPartial(ctx, nm)
}

func (p *PackSync) readPushManifest(
	ctx context.Context,
) (
	map[IndexName]bool,
	error,
) {
	return p.readManifest(ctx, pushManifestFile)
}

func (p *PackSync) readPullManifest(
	ctx context.Context,
) (
	map[IndexName]bool,
	error,
) {
	return p.readManifest(ctx, pullManifestFile)
}

func (p *PackSync) readManifest(
	ctx context.Context,
	fn string,
) (
	map[IndexName]bool,
	error,
) {
	p.manifestMu.Lock()
	defer p.manifestMu.Unlock()

	dir, err := p.prepPackSyncTopDir()
	if err != nil {
		return nil, err
	}
	ret := make(map[IndexName]bool)
	fh, err := dir.Open(fn)
	if err != nil {
		if _, ok := err.(*fs.PathError); ok {
			return ret, nil
		}
		return nil, err
	}
	defer fh.Close()

	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		ret[IndexName(scanner.Text())] = true
	}
	err = scanner.Err()
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (p *PackSync) readIndex(
	ctx context.Context,
	ci *ComingledIndex,
	dir string,
	name string,
	sz int64,
) error {
	indexName, err := fileToIndexName(name)
	if err != nil {
		return err
	}
	fh, err := p.dotGitDir.Open(filepath.Join(dir, name))

	if err != nil {
		return err
	}
	defer fh.Close()

	// For partial indices, hold onto the index data because we might
	// actually fetch the .pack file later.
	var buf bytes.Buffer
	rdr := io.TeeReader(fh, &buf)

	idfx := idxfile.NewMemoryIndex()
	d := idxfile.NewDecoder(rdr)
	err = d.Decode(idfx)
	if err != nil {
		return err
	}
	cnt, err := idfx.Count()
	if err != nil {
		return err
	}

	res := &Index{
		Filename: name,
		Name:     indexName,
		Idx:      idfx,
		Sz:       sz,
		NumObjs:  int(cnt),
		Raw:      buf.Bytes(),
	}

	err = ci.Absorb(res)
	if err != nil {
		return err
	}

	return nil
}

// In the given directory, find and process all pack-*.idx files. If allowList is not nil,
// only read the indices that are in the allowList. Update the given comingled index
// with the new index data.
func (p *PackSync) readIndicesFromDir(
	ctx context.Context,
	ci *ComingledIndex,
	dir string,
	allowList map[IndexName]bool,
) error {
	ls, err := p.dotGitDir.ReadDir(dir)

	if err != nil {
		// It's OK if the directory doesn't exist
		if _, ok := err.(*fs.PathError); ok {
			return nil
		}
		return err
	}

	for _, fi := range ls {
		if fi.IsDir() {
			continue
		}
		gen := ParsePackFileGeneric(fi.Name())
		if gen == nil || gen.Type != PackFileTypeIndex {
			continue
		}
		if allowList != nil && !allowList[gen.Name] {
			continue
		}
		err := p.readIndex(ctx, ci, dir, fi.Name(), fi.Size())
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *PackSync) prepPackDir() (billy.Filesystem, error) {
	return p.prepDir(&p.packDir, packDirName, 0755)
}

func (p *PackSync) prepPackSyncTopDir() (billy.Filesystem, error) {
	return p.prepDir(&p.topDir, p.packSyncTopDirName(), 0755)
}

func (p *PackSync) prepPartialIndexDir() (billy.Filesystem, error) {
	return p.prepDir(&p.partialIndexDir, p.partialIndexDirName(), 0755)
}

func (p *PackSync) prepDir(
	slot *billy.Filesystem,
	dir string,
	perm fs.FileMode,
) (billy.Filesystem, error) {
	p.dirMu.Lock()
	defer p.dirMu.Unlock()
	if *slot != nil {
		return *slot, nil
	}

	err := p.dotGitDir.MkdirAll(dir, perm)
	if err != nil {
		return nil, err
	}
	ret, err := p.dotGitDir.Chroot(dir)
	if err != nil {
		return nil, err
	}
	*slot = ret
	return ret, nil
}

func (p *PackSync) fetchPackData(ctx context.Context, idx *Index) (err error) {
	dir, err := p.prepPackDir()
	if err != nil {
		return err
	}

	p.tl.Log(ctx, (TermLogNoNewline | TermLogCr).ToMsg(
		MsgFetchObjects+"pck "+idx.Name.String()[0:8]+" ... "))

	doFetchAndWrite := func() error {
		fh, err := dir.OpenFile(idx.PackDataFilename(), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer ioutil.CheckClose(fh, &err)

		err = p.rem.FetchPackData(ctx, idx.Name, fh)
		if err != nil {
			return err
		}
		return nil
	}
	err = doFetchAndWrite()
	if err != nil {
		return err
	}

	err = p.dotGitDir.Rename(
		idx.PartialIndexPath(p.partialIndexDirName()),
		idx.FullIndexPath(),
	)
	if err != nil {
		return err
	}

	return nil
}

func (p *PackSync) syncPartialIndex(
	ctx context.Context,
	hash plumbing.Hash,
) error {

	idx := p.fetchCI.Get(hash)
	if idx == nil {
		return plumbing.ErrObjectNotFound
	}
	var minIdx *Index
	for _, i := range idx {
		if minIdx == nil || i.NumObjs < minIdx.NumObjs {
			minIdx = i
		}
	}
	if minIdx == nil {
		return plumbing.ErrObjectNotFound
	}

	err := p.fetchPackData(ctx, minIdx)
	if err != nil {
		return err
	}

	// Since we externally changed the local get directory (by syncing over
	// the above packfile).
	p.localGit.Reindex()

	return nil
}

func (p *PackSync) readLastIndexSyncTime() (time.Time, error) {
	var zed time.Time
	dir, err := p.prepPackSyncTopDir()
	if err != nil {
		return zed, err
	}
	return p.readTime(dir, partialFetchTimeFile)
}

func (p *PackSync) readLastPackPushTime() (time.Time, error) {
	var zed time.Time
	dir, err := p.prepPackSyncTopDir()
	if err != nil {
		return zed, err
	}
	return p.readTime(dir, packPushTimeFile)
}

func (p *PackSync) writeLastPackPushTime(
	t time.Time,
) error {
	dir, err := p.prepPackSyncTopDir()
	if err != nil {
		return err
	}
	return p.writeTime(dir, packPushTimeFile, t)
}

func (p *PackSync) readTime(
	dir billy.Filesystem,
	fn string,
) (
	time.Time,
	error,
) {
	var zed time.Time
	fh, err := dir.Open(fn)
	if err != nil {
		if _, ok := err.(*fs.PathError); ok {
			return zed, nil
		}
		return zed, err
	}

	defer fh.Close()
	var lines []string
	scanner := bufio.NewScanner(fh)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	err = scanner.Err()
	if err != nil {
		return zed, err
	}

	// Ignore a bad file and just return 0 time
	if len(lines) != 1 {
		return zed, nil
	}
	raw, err := strconv.ParseInt(lines[0], 10, 64)
	if err != nil {
		return zed, nil
	}

	tm := time.UnixMicro(raw).UTC()
	return tm, nil
}

func (p *PackSync) writeLastIndexSyncTime(
	t time.Time,
) error {
	dir, err := p.prepPackSyncTopDir()
	if err != nil {
		return err
	}
	return p.writeTime(dir, partialFetchTimeFile, t)
}

func (p *PackSync) writeTime(
	dir billy.Filesystem,
	fn string,
	t time.Time,
) error {
	fh, err := dir.OpenFile(fn, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(fh, &err)
	dat := strconv.FormatInt(t.UnixMicro(), 10) + "\n"
	_, err = fh.Write([]byte(dat))
	if err != nil {
		return err
	}
	return nil
}

func (p *PackSync) writeNewIndex(
	ctx context.Context,
	i RawIndex,
	lastTime time.Time,
) error {
	dir, err := p.prepPartialIndexDir()
	if err != nil {
		return err
	}
	fn := i.Name.PackIndexFilename()
	fh, err := dir.OpenFile(fn, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	defer ioutil.CheckClose(fh, &err)
	_, err = fh.Write(i.Data)
	if err != nil {
		return err
	}
	return nil
}

func (o *PackSync) updateComingledIndex(
	ctx context.Context,
	ci *ComingledIndex,
	i RawIndex,
) error {
	idx, err := i.Import()
	if err != nil {
		return err
	}
	err = ci.Absorb(idx)
	if err != nil {
		return err
	}
	return nil
}

func (o *PackSync) refreshPartialIndex(
	ctx context.Context,
	ci *ComingledIndex,
) (
	bool,
	error,
) {
	o.rpiMu.Lock()
	defer o.rpiMu.Unlock()

	if o.didRpiRefresh {
		return false, nil
	}

	tm, err := o.readLastIndexSyncTime()
	if err != nil {
		return false, err
	}
	newI, err := o.rem.FetchNewIndices(ctx, tm)
	if err != nil {
		return false, err
	}

	num := 0

	for _, i := range newI {

		err = o.writeIndexToPullManifest(ctx, i.Name)
		if err != nil {
			return false, err
		}

		has, err := o.hasIndexSynced(ctx, i.Name)
		if err != nil {
			return false, err
		}
		if has {
			continue
		}

		err = o.writeNewIndex(ctx, i, tm)
		if err != nil {
			return false, err
		}

		err = o.updateComingledIndex(ctx, ci, i)
		if err != nil {
			return false, err
		}

		if i.CTime.After(tm) {
			err = o.writeLastIndexSyncTime(i.CTime)
			if err != nil {
				return false, err
			}
			tm = i.CTime
		}
		num++
	}
	o.didRpiRefresh = true

	return (num > 0), nil
}

type PackSyncStorageBase struct {
	// The *remote* storage that we are fetching from / pushing to
	storage.Storer
	ps *PackSync
}

type PackSyncStoragePush struct {
	*PackSyncStorageBase
	skips map[plumbing.Hash]plumbing.EncodedObject
}

func NewPackSyncStoragePush(b *PackSyncStorageBase) *PackSyncStoragePush {
	m := make(map[plumbing.Hash]plumbing.EncodedObject)
	return &PackSyncStoragePush{
		PackSyncStorageBase: b,
		skips:               m,
	}
}

var _ storage.Storer = (*PackSyncStoragePush)(nil)

func NewPackSyncStorage(
	s storage.Storer,
	ps *PackSync,
	op GitOpType,
) (storage.Storer, error) {
	if ps == nil || op == GitOpTypeFetch {
		return s, nil
	}
	if op != GitOpTypePush {
		return nil, InternalError("unsupported git operation")
	}
	base := &PackSyncStorageBase{
		Storer: s,
		ps:     ps,
	}
	return NewPackSyncStoragePush(base), nil
}

func (p *PackSync) listPackPairsAfterTime(
	m MetaContext,
	tm time.Time,
	dir billy.Filesystem,
) (
	[]IndexName,
	error,
) {
	finfos, err := dir.ReadDir(".")
	if err != nil {
		return nil, err
	}
	type pair struct {
		typ PackFileType
		tm  time.Time
	}
	tab := make(map[IndexName]*pair)
	for _, finfo := range finfos {
		if finfo.IsDir() {
			continue
		}
		// Don't push packs bigger than 1G (for now)
		if finfo.Size() > 1024*1024*1024 {
			continue
		}
		gen := ParsePackFileGeneric(finfo.Name())
		if gen == nil {
			continue
		}
		p, ok := tab[gen.Name]
		if !ok {
			p = &pair{}
		}
		p.typ |= gen.Type
		tab[gen.Name] = p
		if finfo.ModTime().After(p.tm) {
			p.tm = finfo.ModTime()
		}
		tab[gen.Name] = p
	}
	var ret []IndexName
	for k, v := range tab {
		if v.typ == PackFileTypeBoth && !v.tm.Before(tm) {
			ret = append(ret, k)
		}
	}
	return ret, nil
}

func (p *PackSync) doPushPair(
	m MetaContext,
	dir billy.Filesystem,
	idx IndexName,
) error {
	ctx := m.Ctx()
	has, err := p.rem.HasIndex(ctx, idx)
	if err != nil {
		return err
	}
	if has {
		return nil
	}
	fh, err := dir.Open(idx.PackDataFilename())
	if err != nil {
		return err
	}
	defer fh.Close()

	err = p.rem.PushPackData(ctx, idx, fh)
	if err != nil {
		return err
	}

	fh, err = dir.Open(idx.PackIndexFilename())
	if err != nil {
		return err
	}
	defer fh.Close()

	err = p.rem.PushPackIndex(ctx, idx, fh)
	if err != nil {
		return err
	}

	err = p.writeIndexToPushManifest(ctx, idx)
	if err != nil {
		return err
	}

	return nil
}

func (p *PackSync) doPush(
	m MetaContext,
	dir billy.Filesystem,
	indices []IndexName,
) error {
	n := len(indices)
	if n == 0 {
		return nil
	}

	update := func(i int) {
		pct := float64(100) * float64(i) / float64(n)
		tmpl := "🔄 Pushing pack files %.2f%% (%d of %d) ... "
		m.TermLogf(TermLogCr|TermLogNoNewline,
			tmpl, pct, i, n)
	}

	for i, idx := range indices {
		update(i)
		err := p.doPushPair(m, dir, idx)
		if err != nil {
			m.TermLogf(TermLogStd, "❌ failed: %v", err)
			return err
		}
	}
	update(n)
	m.TermLogf(TermLogStd, "✅ done")

	return nil
}

// Push packs from local to remote, but only those that were made after the
// last time we pushed, and those that server doesn't have. We figure out which
// ones the server has via the FetchAllIndexNames method on the PackSyncRemoter
// interface.
func (p *PackSync) pushNewPacks(
	m MetaContext,
) error {
	dir, err := p.prepPackDir()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	tm, err := p.readLastPackPushTime()
	if err != nil {
		return err
	}
	indices, err := p.listPackPairsAfterTime(m, tm, dir)
	if err != nil {
		return err
	}
	if len(indices) == 0 {
		return nil
	}
	err = p.doPush(m, dir, indices)
	if err != nil {
		return err
	}
	err = p.writeLastPackPushTime(now)
	if err != nil {
		return err
	}
	return nil
}

func (p *PackSync) InitPush(
	m MetaContext,
) error {

	err := p.pushNewPacks(m)
	if err != nil {
		return err
	}

	err = p.indexForPush(m)
	if err != nil {
		return err
	}

	return nil
}

func (p *PackSync) indexForPush(
	m MetaContext,
) (err error) {

	m.TermLogf(TermLogNoNewline, "️☝️  Indexing packfiles ... ")
	defer func() { m.TermLogStatus(err) }()

	p.initPushMu.Lock()
	defer p.initPushMu.Unlock()

	p.pushCI = NewComingledIndex()
	ctx := m.Ctx()

	// Anything fetched into our partial directory, we know is on the server
	err = p.readIndicesFromDir(ctx, p.pushCI, p.partialIndexDirName(), nil)
	if err != nil {
		return err
	}

	// read the manifest of packs that we pushed to the server
	files, err := p.readPushManifest(ctx)
	if err != nil {
		return err
	}

	// read in the indices from the above
	err = p.readIndicesFromDir(ctx, p.pushCI, packDirName, files)
	if err != nil {
		return err
	}

	// finally, refresh our partial pack list with whatever is on the server.
	// it might have gotten updated somewhere else. But pay attention to not
	// sync down what we just pushed up in pushNewPacks.
	_, err = p.refreshPartialIndex(ctx, p.pushCI)
	if err != nil {
		return err
	}

	// The pull manifest is the list of all files we have ever pulled down
	// from the server. These might have been moved over to the full pack
	// directory, so we need to check for them there.
	files, err = p.readPullManifest(ctx)
	if err != nil {
		return err
	}

	err = p.readIndicesFromDir(ctx, p.pushCI, packDirName, files)
	if err != nil {
		return err
	}

	p.didInitPush = true

	return nil
}

func (p *PackSync) isInitPush() bool {
	p.initPushMu.RLock()
	defer p.initPushMu.RUnlock()
	return p.didInitPush
}

func (p *PackSync) hasObject(
	h plumbing.Hash,
) bool {
	i := p.pushCI.Get(h)
	return i != nil
}

func (p *PackSyncStoragePush) HasEncodedObject(h plumbing.Hash) error {
	_, err := p.getEncodedObject(plumbing.AnyObject, h)
	return err
}

func (p *PackSyncStoragePush) EncodedObjectSize(h plumbing.Hash) (int64, error) {
	obj, err := p.getEncodedObject(plumbing.AnyObject, h)
	if err != nil {
		return 0, err
	}
	return obj.Size(), nil
}

func (p *PackSyncStoragePush) EncodedObject(
	t plumbing.ObjectType,
	h plumbing.Hash,
) (plumbing.EncodedObject, error) {
	return p.getEncodedObject(t, h)
}

func (p *PackSyncStoragePush) getEncodedObject(
	t plumbing.ObjectType,
	h plumbing.Hash,
) (plumbing.EncodedObject, error) {

	if !p.ps.isInitPush() {
		return nil, InternalError("not initialized")
	}

	if obj, ok := p.skips[h]; ok {
		return obj, nil
	}

	// Check for a loose object on the server -- but this won't check for
	// objects in packs.
	obj, err := p.Storer.EncodedObject(t, h)
	if err == nil {
		return obj, err
	}
	if err != plumbing.ErrObjectNotFound {
		return nil, err
	}

	// If the server has the object (in a pack file, since it failed the loose-object
	// lookup just above), then it's ok to return the object from the local dot git
	// dir.
	if !p.ps.hasObject(h) {
		return nil, plumbing.ErrObjectNotFound
	}
	return p.ps.localGit.EncodedObject(t, h)
}

func (p *PackSyncStoragePush) SetEncodedObject(
	obj plumbing.EncodedObject,
) (plumbing.Hash, error) {
	// Skip if the server already has the object in its
	// packs
	hsh := obj.Hash()
	if p.ps.hasObject(hsh) {
		p.skips[hsh] = obj
		return hsh, nil
	}
	return p.Storer.SetEncodedObject(obj)
}
