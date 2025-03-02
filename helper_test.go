package remhelp

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/stretchr/testify/require"
)

func checkSimpleRepo(t *testing.T, m *MemoryStorage) {

	require.Greater(t, len(m.Objects), 4)
	refStorage := m.ReferenceStorage
	require.Equal(t, len(refStorage), 2)

	head, err := refStorage.Reference(plumbing.HEAD)
	require.NoError(t, err)
	require.Equal(t, head.Type(), plumbing.SymbolicReference)
	require.Equal(t, head.Target().String(), "refs/heads/main")

	master, err := refStorage.Reference(plumbing.ReferenceName("refs/heads/main"))
	require.NoError(t, err)
	require.Equal(t, master.Type(), plumbing.HashReference)

	hsh := master.Hash()
	_, found := m.Objects[hsh]
	require.True(t, found)
}

// TestSimplePushFetch tests the test. It makes sure we can compile and install a new tap binary
// via CLI-out to go install.
func TestSimplePushFetch(t *testing.T) {
	te, err := NewTestEnv()
	defer te.Cleanup()
	require.NoError(t, err)
	err = te.Init()
	require.NoError(t, err)

	// populate the scratch repo with some files, stuff, etc.
	sr := te.NewScratchRepo(t)
	sr.Mkdir(t, "a/b")
	sr.Mkdir(t, "c")
	sr.WriteFile(t, "a/b/1", "11111")
	sr.WriteFile(t, "a/2", "2222")
	sr.WriteFile(t, "c/3", "3333")

	big := make([]byte, 1024*1024)
	_, err = rand.Read(big)
	require.NoError(t, err)

	sr.WriteFileBinary(t, "biggie", big)

	sr.Git(t, "init")

	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "initial commit")
	sr.WriteFile(t, "a/4", "444444")
	sr.Git(t, "add", "a/4")
	sr.Git(t, "commit", "-m", "add a/4")
	sr.Git(t, "remote", "add", "origin", sr.Origin())
	sr.Git(t, "push", "-vvvv", "origin", "main")

	// We pushed into in-memory storage via git-remote-helper. To prove this worked,
	// there should be objects now in that in-memory storage.
	checkSimpleRepo(t, te.Sfact.Last)

	// test that we can clone from the remote repo in a new directory
	sr2 := te.NewScratchRepo(t)
	sr2.Git(t, "clone", sr.Origin(), ".")
	sr2.ReadFile(t, "a/b/1", "11111")
	sr2.ReadFile(t, "a/2", "2222")
	sr2.ReadFile(t, "c/3", "3333")
	sr2.ReadFile(t, "a/4", "444444")
	sr2.ReadFileBinary(t, "biggie", big)

	// test a force push
	sr.Git(t, "checkout", "-b", "b1")
	sr.WriteFile(t, "a/b/1", "b1 1")
	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "b1 1")
	sr.Git(t, "push", "origin", "b1")

	sr2.Git(t, "fetch", "origin", "b1")
	sr2.Git(t, "checkout", "b1")
	sr2.ReadFile(t, "a/b/1", "b1 1")

	sr.WriteFile(t, "a/b/1", "b1 2")
	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "b1 2")
	sr.Git(t, "push", "origin", "b1")

	sr2.WriteFile(t, "a/b/1", "b1 3")
	sr2.Git(t, "add", ".")
	sr2.Git(t, "commit", "-m", "b1 3")

	// should fail without the force
	err = sr2.GitWithErr(t, "push", "origin", "b1")
	require.Error(t, err)
	require.IsType(t, &GitError{}, err)
	// should succeed with it
	sr2.Git(t, "push", "-f", "origin", "b1")

	sr.Git(t, "fetch", "origin", "b1")
	sr.Git(t, "reset", "--hard", "origin/b1")
	sr.ReadFile(t, "a/b/1", "b1 3")
}

func mkFile(t *testing.T, sr *TestScratchRepo, prfx string, i int, sz int) string {
	fn := fmt.Sprintf("%sf%d", prfx, i)
	buf := make([]byte, sz)
	for j := 0; j < sz; j++ {
		buf[j] = byte(i)
	}
	buf = append([]byte(prfx), buf...)
	enc := base64.StdEncoding.EncodeToString(buf)
	sr.WriteFile(t, fn, enc)
	return enc
}

func mkFileCluster(t *testing.T, sr *TestScratchRepo, dir string, n int, sz int, doPack bool) {
	sr.Mkdir(t, dir)
	for i := 0; i < n; i++ {
		mkFile(t, sr, dir, i, sz)
	}
	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "small files")
	if doPack {
		sr.Git(t, "repack")
	}
}

func TestPack(t *testing.T) {
	te, err := NewTestEnv()
	defer te.Cleanup()
	require.NoError(t, err)
	err = te.Init()
	require.NoError(t, err)

	// populate the scratch repo with some files, stuff, etc.
	sr := te.NewScratchRepo(t)

	n := 100
	sz := 64

	cluster := func(sr *TestScratchRepo, dir string, doPack bool) {
		mkFileCluster(t, sr, dir, n, sz, doPack)
	}

	sr.Git(t, "init")
	cluster(sr, "a/", true)
	cluster(sr, "b/", true)
	cluster(sr, "c/", false)

	sr.Git(t, "remote", "add", "origin", sr.Origin())
	sr.Git(t, "push", "-vvvv", "origin", "main")

	require.Equal(t, 2, len(te.Sfact.Last.packs))

	// It's 103 object per cluster, so if we only have 103
	// objects, that means the other two clusters (a and b)
	// weren't pushed as objects, but rather were pushed as packs.
	require.Equal(t, 103, len(te.Sfact.Last.Objects))

	sr2 := te.NewScratchRepo(t)
	sr2.Git(t, "clone", sr.Origin(), ".")

	cluster(sr, "d/", true)
	cluster(sr, "e/", false)

	sr.Git(t, "push", "origin", "main")
	require.Equal(t, 3, len(te.Sfact.Last.packs))
	sr2.Git(t, "pull", "origin", "main")

	// Assert that we properly wrote down the partial-fetch-time
	tm := sr2.ReadFileToString(t,
		filepath.Join(".git", packSyncTopDir,
			"origin",
			partialFetchTimeFile),
	)
	tm = strings.TrimSpace(tm)
	i, err := strconv.ParseUint(tm, 10, 64)
	require.NoError(t, err)
	require.Greater(t, i, uint64(0))

	cluster(sr2, "f/", true)
	cluster(sr2, "g/", false)
	sr2.Git(t, "push", "-vvvv", "origin", "main")

	sr.Git(t, "pull", "origin", "main")

	requireEq := func(f string) {
		s := sr.ReadFileToString(t, f)
		s2 := sr2.ReadFileToString(t, f)
		require.Equal(t, s, s2)
	}
	requireEq("f/f13")
	requireEq("g/f17")

}

func TestCleanTmps(t *testing.T) {
	te, err := NewTestEnv()
	defer te.Cleanup()
	require.NoError(t, err)
	err = te.Init()
	require.NoError(t, err)

	// populate the scratch repo with some files, stuff, etc.
	sr := te.NewScratchRepo(t)
	sr.Git(t, "init")
	sr.Mkdir(t, "a/b")
	sr.WriteFile(t, "a/b/1", "11111")
	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "commit 1")
	sr.Git(t, "remote", "add", "origin", sr.Origin())
	sr.Git(t, "push", "-vvvv", "origin", "main")

	sr2 := te.NewScratchRepo(t)
	sr2.Git(t, "clone", sr.Origin(), ".")

	dir, err := os.ReadDir(filepath.Join(sr2.dir, ".git", "refs", "remotes"))
	require.NoError(t, err)
	var remotes []string
	for _, d := range dir {
		if d.IsDir() && d.Name() != "." && d.Name() != ".." {
			remotes = append(remotes, d.Name())
		}
	}
	require.Equal(t, 1, len(remotes))
	require.Equal(t, "origin", remotes[0])

}

func TestPackOnlyPush(t *testing.T) {
	te, err := NewTestEnv()
	defer te.Cleanup()
	require.NoError(t, err)
	err = te.Init()
	require.NoError(t, err)

	// populate the scratch repo with some files, stuff, etc.
	sr := te.NewScratchRepo(t)

	n := 100
	sz := 64

	cluster := func(sr *TestScratchRepo, dir string, doPack bool) {
		mkFileCluster(t, sr, dir, n, sz, doPack)
	}

	sr.Git(t, "init")
	cluster(sr, "a/", true)

	sr.Git(t, "remote", "add", "origin", sr.Origin())
	sr.Git(t, "push", "-vvvv", "origin", "main")

	var dat string
	for i := 1; i < 5; i++ {
		dat = mkFile(t, sr, "a/", 100+i, sz)
		sr.Git(t, "add", ".")
		sr.Git(t, "commit", "-m", "one more")
		sr.Git(t, "push", "-vvvv", "origin", "main")
	}

	require.Equal(t, 1, len(te.Sfact.Last.packs))

	sr2 := te.NewScratchRepo(t)
	sr2.Git(t, "clone", sr.Origin(), ".")
	sr2.ReadFile(t, "a/f104", dat)

}

func TestDeleteRemoteBranch(t *testing.T) {
	te, err := NewTestEnv()
	defer te.Cleanup()
	require.NoError(t, err)
	err = te.Init()
	require.NoError(t, err)

	// populate the scratch repo with some files, stuff, etc.
	sr := te.NewScratchRepo(t)

	sr.Git(t, "init")
	sr.Git(t, "checkout", "-b", "b1")
	sr.WriteFile(t, "a", "11111")
	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "commit 1")
	sr.Git(t, "remote", "add", "origin", sr.Origin())
	sr.Git(t, "push", "origin", "b1")
	sr.Git(t, "push", "origin", ":b1")
}

func TestOneArgClone(t *testing.T) {
	te, err := NewTestEnv()
	defer te.Cleanup()
	require.NoError(t, err)
	err = te.Init()
	require.NoError(t, err)
	sr := te.NewScratchRepo(t)
	sr.Git(t, "init")
	sr.WriteFile(t, "a", "11111")
	sr.Git(t, "add", ".")
	sr.Git(t, "commit", "-m", "commit 1")
	sr.Git(t, "remote", "add", "origin", sr.Origin())
	sr.Git(t, "push", "origin", "main")

	sr2 := te.NewScratchRepo(t)
	sr2.Git(t, "clone", sr.Origin())
}
