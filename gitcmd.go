package remhelp

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type GitCmdRes struct {
	Stdout []byte
	Err    error
}

type GitCmdArg struct {
	Git  string
	Dir  string
	Args []string
}

func GitCmd(
	arg GitCmdArg,
) (*GitCmdRes, error) {
	prog := arg.Git
	if prog == "" {
		prog = "git"
	}
	if len(arg.Args) == 0 {
		return nil, errors.New("empty args to git not allowed")
	}
	cmd := exec.Command(prog, arg.Args...)
	cmd.Dir = arg.Dir
	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = os.Stderr
	err := cmd.Run()
	return &GitCmdRes{
		Stdout: outBuf.Bytes(),
		Err:    err,
	}, nil
}

type GitCountObjectsRes struct {
	Unpacked      int
	Packed        int
	Packs         int
	PrunePackable int
	Garbage       int
}

func GitCountObjects(
	arg GitCmdArg,
) (
	*GitCountObjectsRes,
	error,
) {
	arg.Args = []string{"count-objects", "-v"}
	res, err := GitCmd(arg)
	if err != nil {
		return nil, err
	}
	if res.Err != nil {
		return nil, res.Err
	}

	var lines []string
	sc := bufio.NewScanner(strings.NewReader(string(res.Stdout)))
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}

	split := func(line string) (string, int) {
		parts := strings.Split(line, ": ")
		if len(parts) != 2 {
			return "", 0
		}
		key := parts[0]
		val, err := strconv.Atoi(strings.TrimSpace(parts[1]))
		if err != nil {
			return "", 0
		}
		return key, val
	}
	var ret GitCountObjectsRes

	for _, line := range lines {
		key, val := split(line)
		switch key {
		case "count":
			ret.Unpacked = val
		case "in-pack":
			ret.Packed = val
		case "packs":
			ret.Packs = val
		case "prune-packable":
			ret.PrunePackable = val
		case "garbage":
			ret.Garbage = val
		}
	}
	return &ret, nil
}

func GitRepackAlways(
	arg GitCmdArg,
) (
	*GitCmdRes,
	error,
) {
	if len(arg.Args) == 0 {
		arg.Args = []string{"repack"}
	}
	return GitCmd(arg)
}

type GitRepackArg struct {
	Git    string
	Dir    string
	Thresh int
	Opts   []string
}

func GitRepack(
	m MetaContext,
	arg GitRepackArg,
) (
	bool,
	error,
) {
	if arg.Thresh == 0 {
		return false, nil
	}
	cres, err := GitCountObjects(GitCmdArg{Git: arg.Git, Dir: arg.Dir})
	if err != nil {
		return false, err
	}
	loose := cres.Unpacked
	if loose <= arg.Thresh {
		return false, nil
	}
	args := []string{"repack", "-d"}
	args = append(args, arg.Opts...)

	m.TermLogf(TermLogNoNewline, "📦 Repacking (%d > %d loose objects) ... ",
		cres.Unpacked, arg.Thresh)

	ret, err := GitRepackAlways(GitCmdArg{
		Git:  arg.Git,
		Dir:  arg.Dir,
		Args: args,
	})

	if err != nil {
		m.TermLogf(TermLogStd, "❌ failed: %v", err)
		return false, err
	}
	if ret.Err != nil {
		m.TermLogf(TermLogStd, "❌ failed: %v", err)
		return false, ret.Err
	}
	m.TermLogf(TermLogStd, "✅ done")
	return true, nil
}

var GitObjPackDir = filepath.Join("objects", "pack")
