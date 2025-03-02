package remhelp

import (
	"fmt"
	"strings"
)

type ParseError struct {
	msg string
	cmd []string
}

func (e ParseError) Error() string {
	return fmt.Sprintf("%s (%s)", e.msg, strings.Join(e.cmd, " "))
}

type NotImplementedError struct{}

func (e NotImplementedError) Error() string {
	return "not implemented"
}

type BadIndexNameError struct{}

func (e BadIndexNameError) Error() string {
	return "bad index name"
}

type InternalError string

func (e InternalError) Error() string {
	return "internal error: " + string(e)
}

type BadGitPathError struct {
	Path LocalPath
}

func (e BadGitPathError) Error() string {
	return fmt.Sprintf("bad git path: %s", e.Path)
}
