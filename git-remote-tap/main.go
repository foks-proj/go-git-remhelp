package main

import (
	"errors"
	"io"
	"net"
	"os"
	"strings"
)

// tap is a little program that is run with 2 arguments. It needs to have the TAP_SOCKET environment
// variable set, and it looks there to get the path to a unix domain socket.
//
// It then works by: (1) connecting to that unix domain socket; (2) writing the os.Args we were launched
// with down into the socket, so that the remote helper on other end can see the invocation parameters;
// and then (3a) proxying data read from standard input into that socket; and (3b) proxying data read from that socket to standard output.
// It will exit as soon as it get EOF on standard input, potentially without flushing the other
// direction before it goes.
//
// The intent of this little program is to allow us to debug a git-remote-helper we are writing
// in go by calling out to git, having git call out to this program (tap), and having this
// program write data via unix domain socket back into the programm being run in test or debug.

func proxy(in io.Reader, out io.Writer, ch chan<- error) {
	_, err := io.Copy(out, in)
	if ch != nil {
		ch <- err
	}
}

func run() error {
	file := os.Getenv("TAP_SOCKET")
	if len(file) == 0 {
		return errors.New("need to set the TAP_SOCKET environment variable")
	}
	conn, err := net.Dial("unix", file)
	if err != nil {
		return err
	}
	defer conn.Close()
	wd, err := os.Getwd()
	if err != nil {
		return err
	}
	args := append(os.Args, wd)
	gitDir := os.Getenv("GIT_DIR")
	args = append(args, gitDir)

	// First write out the args that we were launched with.
	if _, err := conn.Write([]byte(strings.Join(args, " ") + "\n")); err != nil {
		return err
	}
	ch := make(chan error)
	go proxy(conn, os.Stdout, nil)
	go proxy(os.Stdin, conn, ch)
	return <-ch
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
