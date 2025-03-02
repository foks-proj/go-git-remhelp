package remhelp

import (
	"context"
	"fmt"
	"os"

	"github.com/go-git/go-git/v5/plumbing"
)

type DebugLogger interface {
	Log(s string)
}

func LogInput(l DebugLogger, s string) {
	if l == nil {
		return
	}
	l.Log("< " + s + "\n")
}

func LogOutput(l DebugLogger, s string) {
	if l == nil {
		return
	}
	l.Log("> " + s + "\n")
}

type TestLogger struct {
}

func (t *TestLogger) Log(s string) {
	fmt.Fprintf(os.Stderr, "(remhelp) %s", s)
}

type BatchedIOLoggedO struct {
	blio BatchedLineIOer
	log  DebugLogger
}

type LogWrapperOutput struct {
	outputter LineOutputter
	log       DebugLogger
}

func newLogWrapper(outputter LineOutputter, log DebugLogger) LineOutputter {
	return &LogWrapperOutput{
		outputter: outputter,
		log:       log,
	}
}

func (l *LogWrapperOutput) Output(lines []string, err error) error {
	if err != nil {
		l.log.Log(fmt.Sprintf("error: %s\n", err))
	}
	for _, line := range lines {
		LogOutput(l.log, line)
	}
	err = l.outputter.Output(lines, err)
	if err != nil {
		return err
	}
	return nil
}

var _ LineOutputter = (*LogWrapperOutput)(nil)

var _ BatchedLineIOer = (*BatchedIOLoggedO)(nil)

func (t *BatchedIOLoggedO) Next(
	ctx context.Context,
) (string, LineOutputter, error) {
	line, outputter, err := t.blio.Next(ctx)
	if err != nil {
		return "", nil, err
	}
	outputter = newLogWrapper(outputter, t.log)
	return line, outputter, nil
}

func NewBatchedIOLoggedO(
	blio BatchedLineIOer,
	log DebugLogger,
) BatchedLineIOer {
	return &BatchedIOLoggedO{
		blio: blio,
		log:  log,
	}
}

type TermLogBitfield uint32

type TermLogLine struct {
	Msg  string
	Opts TermLogBitfield
}

const (
	TermLogStd       TermLogBitfield = 0
	TermLogCr        TermLogBitfield = 1
	TermLogNoNewline TermLogBitfield = 2
)

func (b TermLogBitfield) ToMsg(msg string) TermLogLine {
	return TermLogLine{
		Msg:  msg,
		Opts: b,
	}
}

type TermLogger interface {
	Log(ctx context.Context, t TermLogLine)
}

func TermLogf(
	ctx context.Context,
	t TermLogger,
	opts TermLogBitfield,
	format string,
	args ...interface{},
) {
	msg := fmt.Sprintf(format, args...)
	t.Log(ctx, opts.ToMsg(msg))
}

type NilTermLogger struct{}

func (n *NilTermLogger) Log(ctx context.Context, t TermLogLine) {}

var _ TermLogger = (*NilTermLogger)(nil)

const MsgFetchObjects = "🐕 Fetching from remote ... "

func TermLogMsgFetch(
	ctx context.Context,
	tl TermLogger,
	typ string,
	hash plumbing.Hash,
) {
	TermLogf(ctx, tl, (TermLogCr | TermLogNoNewline),
		MsgFetchObjects+typ+" "+hash.String()[0:8]+" ... ")
}
