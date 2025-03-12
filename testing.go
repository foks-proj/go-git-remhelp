package remhelp

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed git-remote-tap/main.go
var GitRemoteTapCode string

const TestProtName = "tap"

type TestScratchRepo struct {
	desc GitRepoDesc
	nm   string
	dir  string
	env  *TestEnv
}

func (r *TestScratchRepo) Origin() string {
	return r.desc.Origin()
}

func (r *TestScratchRepo) Mkdir(t *testing.T, name string) {
	dir := filepath.Join(r.dir, name)
	err := os.MkdirAll(dir, 0755)
	require.NoError(t, err)
}

func (r *TestScratchRepo) inDir(t *testing.T, fn func() error) error {
	cwd, err := os.Getwd()
	require.NoError(t, err)
	err = os.Chdir(r.dir)
	require.NoError(t, err)
	ret := fn()
	err = os.Chdir(cwd)
	require.NoError(t, err)
	return ret
}

func (r *TestScratchRepo) init(t *testing.T) {
	r.Git(t, "init", "-b", "main")
}

func (r *TestScratchRepo) setenv(t *testing.T) {
	r.env.setenv(t)
}

type GitError struct {
	Err  error
	Args []string
	Msg  string
}

func (g *GitError) Error() string {
	return fmt.Sprintf("git error: %v\nargs: %v\nmsg: %v", g.Err, g.Args, g.Msg)
}

func (r *TestScratchRepo) GitWithErr(t *testing.T, args ...string) error {
	r.setenv(t)
	return r.inDir(t, func() error {
		git, err := exec.LookPath("git")
		require.NoError(t, err)
		t.Logf("running git %v in %s", args, r.dir)
		cmd := exec.Command(git, args...)
		stderrPipe, err := cmd.StderrPipe()
		require.NoError(t, err)
		err = cmd.Start()
		require.NoError(t, err)
		stderr, err := io.ReadAll(stderrPipe)
		require.NoError(t, err)
		err = cmd.Wait()
		if err != nil {
			return &GitError{
				Err:  err,
				Args: args,
				Msg:  string(stderr),
			}
		}
		return nil
	})
}

func (r *TestScratchRepo) Git(t *testing.T, args ...string) {
	err := r.GitWithErr(t, args...)
	require.NoError(t, err)
}

func (r *TestScratchRepo) WriteFile(t *testing.T, name, content string) {
	path := filepath.Join(r.dir, name)
	fh, err := os.Create(path)
	require.NoError(t, err)
	_, err = fh.WriteString(content)
	require.NoError(t, err)
	err = fh.Close()
	require.NoError(t, err)
}

func (r *TestScratchRepo) WriteFileBinary(t *testing.T, name string, content []byte) {
	path := filepath.Join(r.dir, name)
	fh, err := os.Create(path)
	require.NoError(t, err)
	n, err := fh.Write(content)
	require.Equal(t, len(content), n)
	require.NoError(t, err)
	err = fh.Close()
	require.NoError(t, err)
}

func (r *TestScratchRepo) ReadFile(t *testing.T, name string, content string) {
	path := filepath.Join(r.dir, name)
	fh, err := os.Open(path)
	require.NoError(t, err)
	contents, err := io.ReadAll(fh)
	require.NoError(t, err)
	require.Equal(t, content, string(contents))
}

func (r *TestScratchRepo) ReadFileToString(t *testing.T, name string) string {
	path := filepath.Join(r.dir, name)
	fh, err := os.Open(path)
	require.NoError(t, err)
	contents, err := io.ReadAll(fh)
	require.NoError(t, err)
	return string(contents)
}

func (r *TestScratchRepo) ReadFileWithErr(name string, content string) error {
	path := filepath.Join(r.dir, name)
	fh, err := os.Open(path)
	if err != nil {
		return err
	}
	contents, err := io.ReadAll(fh)
	if err != nil {
		return err
	}
	if content != string(contents) {
		return errors.New("content mismatch")
	}
	return nil
}

func (r *TestScratchRepo) ReadFileBinary(t *testing.T, name string, expected []byte) {
	path := filepath.Join(r.dir, name)
	fh, err := os.Open(path)
	require.NoError(t, err)
	contents, err := io.ReadAll(fh)
	require.NoError(t, err)
	require.Equal(t, expected, contents)
}

type TestMemoryStorageFactory struct {
	fact *MemoryStorageFactory
	Last *MemoryStorage
}

func NewTestMemoryStorageFactory() *TestMemoryStorageFactory {
	return &TestMemoryStorageFactory{
		fact: NewMemoryStorageFactory(),
	}
}

type TestEnv struct {
	Dir      string
	Bin      string
	helper   *TestHelper
	Sfact    *TestMemoryStorageFactory
	EnvIsSet bool
	Desc     GitRepoDesc
}

func randomHexTest(t *testing.T, n int) string {
	ret, err := randomHex(n)
	require.NoError(t, err)
	return ret
}

func (e *TestEnv) setenv(t *testing.T) {
	if e.EnvIsSet {
		return
	}
	path := os.Getenv("PATH")
	path = e.Bin + ":" + path
	err := os.Setenv("PATH", path)
	require.NoError(t, err)
	err = os.Setenv("TAP_SOCKET", e.helper.sockname)
	require.NoError(t, err)
	e.EnvIsSet = true
}

func (e *TestEnv) NewScratchRepo(t *testing.T) *TestScratchRepo {
	parent := filepath.Join(e.Dir, "repos")
	err := os.MkdirAll(parent, 0755)
	require.NoError(t, err)
	desc := e.Desc
	rhex := randomHexTest(t, 8)
	if desc.RepoName == "" {
		desc.RepoName = rhex
	}
	dir := filepath.Join(parent, rhex)
	err = os.MkdirAll(dir, 0755)
	require.NoError(t, err)
	return &TestScratchRepo{
		nm:   rhex,
		dir:  dir,
		env:  e,
		desc: desc,
	}
}

func (f *TestMemoryStorageFactory) New(
	nm RemoteName,
	url RemoteURL,
	lcd LocalCheckoutDir,
) (Storer, error) {
	ret, err := f.fact.New(nm, url, lcd)
	if err != nil {
		return nil, err
	}
	typed, ok := ret.(*MemoryStorage)
	if !ok {
		return nil, fmt.Errorf("bad type: %T", ret)
	}
	f.Last = typed
	return ret, nil
}

var _ StorerFactory = &TestMemoryStorageFactory{}

func NewTestEnv() (*TestEnv, error) {
	return NewTestEnvWithPrefix("remhelp_test")
}

func NewTestEnvWithPrefix(prefix string) (*TestEnv, error) {
	dir, err := os.MkdirTemp("", prefix)
	if err != nil {
		return nil, err
	}
	bin := filepath.Join(dir, "bin")
	ret := &TestEnv{
		Dir:   dir,
		Bin:   bin,
		Sfact: NewTestMemoryStorageFactory(),
	}
	return ret, nil
}

func (e *TestEnv) Cleanup() error {
	err := os.RemoveAll(e.Dir)
	if err != nil {
		return err
	}
	err = e.helper.Stop()
	if err != nil {
		return err
	}
	return nil
}

func (e *TestEnv) installTap(nm string) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	defer os.Chdir(cwd)
	tmp, err := os.MkdirTemp("", "git_remote_"+nm+"_compile")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	err = os.Chdir(tmp)
	if err != nil {
		return err
	}
	parent := "git-remote-" + nm
	err = os.Mkdir(parent, 0755)
	if err != nil {
		return err
	}
	err = os.Chdir(parent)
	if err != nil {
		return err
	}
	err = os.WriteFile("main.go", []byte(GitRemoteTapCode), 0644)
	if err != nil {
		return err
	}
	err = os.WriteFile("go.mod", []byte("module git-remote-"+nm+"\n"), 0644)
	if err != nil {
		return err
	}
	os.Setenv("GOBIN", e.Bin)
	gobin, err := exec.LookPath("go")
	if err != nil {
		return err
	}
	err = EasyExec(gobin, "install")
	if err != nil {
		return err
	}
	return nil
}

func EasyExec(c string, args ...string) error {
	return exec.Command(c, args...).Run()
}

func (e *TestEnv) Init() error {
	return e.InitWithDesc(GitRepoDesc{
		ProtName: TestProtName,
		Host:     "foo.x",
	})
}

func (e *TestEnv) InitWithDesc(d GitRepoDesc) error {
	err := e.installTap(d.ProtName)
	if err != nil {
		return err
	}
	err = e.launchHelper()
	if err != nil {
		return err
	}
	e.Desc = d
	return nil
}

func (e *TestEnv) launchHelper() error {
	sock := filepath.Join(e.Dir, "sock")
	helper := NewTestHelper(sock, e.Sfact)
	err := helper.Run()
	if err != nil {
		return err
	}
	e.helper = helper
	return nil
}

// A git-remote-helper that works only in test. It creates a socket and listens on it,
// expecting the tap process to connect to it. As per that protocol, the first line in
// the transcript is the command line invocation of the tap process. And then it should
// work as a typical remote helper.
type TestHelper struct {
	sockname       string
	sock           net.Listener
	quitCh         chan struct{}
	acceptLoopDone chan error
	sfact          StorerFactory
}

func NewTestHelper(s string, sfact StorerFactory) *TestHelper {
	return &TestHelper{
		sockname: s,
		quitCh:   make(chan struct{}),
		sfact:    sfact,
	}
}

func (t *TestHelper) Stop() error {
	close(t.quitCh)
	if t.sock == nil {
		return nil
	}
	err := t.sock.Close()
	if err != nil {
		return err
	}
	err = os.Remove(t.sockname)
	if err != nil {
		return err
	}
	err = <-t.acceptLoopDone
	if err != nil {
		return err
	}
	return nil
}

func (t *TestHelper) init() error {

	sock, err := net.Listen("unix", t.sockname)
	if err != nil {
		return err
	}
	t.sock = sock
	return nil
}

func (t *TestHelper) serveOne(conn net.Conn) error {

	ctx := context.Background()
	blio := NewSimpleBatchedLineIO(conn, conn)

	// In the tap/testing protocol, the first line of the stream is the arguments
	// that the tap helper was launched with.
	line, _, err := blio.Next(ctx)
	if err != nil {
		return err
	}
	line = strings.TrimSpace(line)
	parts := strings.Split(line, " ")

	if len(parts) != 5 {
		return fmt.Errorf("bad command line: %+v; need 4 args", parts)
	}

	nm := RemoteName(parts[1])
	url := RemoteURL(parts[2])

	lcd, err := NewLocalCheckDirFromWorkingDirAndGitDir(
		LocalPath(parts[3]),
		LocalPath(parts[4]),
	)
	if err != nil {
		return err
	}

	store, err := t.sfact.New(nm, url, lcd)
	if err != nil {
		return err
	}

	hlpr, err := NewRemoteHelper(blio, store, RemoteHelperOptions{DbgLog: &TestLogger{}})
	if err != nil {
		return err
	}

	// This is finally where the test cases get to exercise the code in
	// this module.
	err = hlpr.Run(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (t *TestHelper) acceptLoop() error {

	keepGoing := true

	var ret error
	for keepGoing {
		conn, err := t.sock.Accept()
		if err != nil {
			select {
			case <-t.quitCh:
			default:
				ret = err
			}
			keepGoing = false
		}
		if conn != nil {
			go func() {
				defer conn.Close()
				t.serveOne(conn)
			}()
		}
	}
	return ret
}

func (t *TestHelper) Run() error {

	err := t.init()
	if err != nil {
		return err
	}
	go func() {
		err := t.acceptLoop()
		t.acceptLoopDone <- err
	}()
	return nil
}
