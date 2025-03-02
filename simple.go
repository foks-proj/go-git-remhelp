package remhelp

import (
	"bufio"
	"context"
	"io"
)

type SimpleBatchedLineIO struct {
	in    io.Reader
	out   io.Writer
	bufio *bufio.Scanner
}

func NewSimpleBatchedLineIO(in io.Reader, out io.Writer) *SimpleBatchedLineIO {
	return &SimpleBatchedLineIO{
		in:    in,
		out:   out,
		bufio: bufio.NewScanner(in),
	}
}

type SimpleLineOuputter struct {
	out io.Writer
}

func (s *SimpleLineOuputter) Output(lines []string, err error) error {
	if err != nil {
		return err
	}
	for _, line := range lines {
		_, err := s.out.Write([]byte(line + "\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func newSimpleLineOuputter(out io.Writer) LineOutputter {
	return &SimpleLineOuputter{out: out}
}

func (s *SimpleBatchedLineIO) Next(ctx context.Context) (string, LineOutputter, error) {

	if s.bufio.Scan() {
		txt := s.bufio.Text()
		return txt, newSimpleLineOuputter(s.out), nil
	}

	err := s.bufio.Err()
	if err != nil {
		return "", nil, err
	}

	return "", nil, io.EOF
}

var _ LineOutputter = (*SimpleLineOuputter)(nil)
var _ BatchedLineIOer = (*SimpleBatchedLineIO)(nil)
