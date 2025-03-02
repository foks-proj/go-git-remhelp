package remhelp

import (
	"context"
	"io"
	"strings"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
)

const (
	cmdStringPush  = "push"
	cmdStringFetch = "fetch"
	cmdStringList  = "list"
	cmdStringCap   = "capabilities"
)

// Note that for I/O in this system, the input and ouptut are intertwined. Every line of input
// may or may not have a corresponding set of output lines. For instance, a capabilities command's
// first line of input has a corresponding n lines of output, one for each "capabitlity."
// For a batch command like push, the last line of input (the empty line) has a corresponding set of
// output, one for each input line, showing results of each push. Hence, the TextInterfacer line
// source returns a single input line, and a LineOutputter instance that allows the machinery
// to output 0-n lines of output.
// OR, the answer might have been an error, in which case, bubble that back up.
type LineOutputter interface {
	Output([]string, error) error
}

type BatchedLineIOer interface {
	Next(ctx context.Context) (string, LineOutputter, error)
}

type Parser struct {
	blio BatchedLineIOer
	eof  bool
}

func NewParser(tio BatchedLineIOer) *Parser {
	return &Parser{
		blio: tio,
	}
}

func (p *Parser) next(ctx context.Context, newCommand bool) ([]string, LineOutputter, error) {
	if p.eof {
		return nil, nil, nil
	}
	line, outputter, err := p.blio.Next(ctx)
	if err == io.EOF {
		p.eof = true
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if line == "" {
		if newCommand {
			p.eof = true
		}
		return nil, outputter, nil
	}
	parts := strings.Split(line, " ")
	return parts, outputter, nil
}

func (p *Parser) parseBatch(
	ctx context.Context,
	parts []string,
	outputter LineOutputter,
	parseOne func([]string) error,
) (LineOutputter, error) {
	for len(parts) > 0 {
		err := outputter.Output(nil, nil)
		if err != nil {
			return nil, err
		}
		err = parseOne(parts)
		if err != nil {
			return nil, err
		}
		parts, outputter, err = p.next(ctx, false)
		if err != nil {
			return nil, err
		}
	}
	return outputter, nil
}

func (p *Parser) parseFetch(
	ctx context.Context,
	parts []string,
	outputter LineOutputter,
) (*FetchCmd, error) {
	cmd := &FetchCmd{}

	parseOne := func(parts []string) error {
		if len(parts) != 3 || parts[0] != cmdStringFetch {
			return ParseError{
				cmd: parts,
				msg: "bad get command",
			}
		}
		cmd.Args = append(cmd.Args, FetchArg{
			Hash: plumbing.NewHash(parts[1]),
			Name: plumbing.ReferenceName(parts[2]),
		})
		return nil
	}
	var err error
	outputter, err = p.parseBatch(ctx, parts, outputter, parseOne)
	if err != nil {
		return nil, err
	}
	cmd.Output = outputter
	return cmd, nil
}

func (p *Parser) parseList(
	ctx context.Context,
	parts []string,
	outputter LineOutputter,
) (*ListCmd, error) {
	cmd := &ListCmd{}

	if parts[0] != cmdStringList {
		return nil, ParseError{
			cmd: parts,
			msg: "bad list command",
		}
	}
	parts = parts[1:]
	if len(parts) == 1 && parts[0] == "for-push" {
		cmd.ForPush = true
		parts = nil
	}

	if len(parts) > 0 {
		return nil, ParseError{
			cmd: parts,
			msg: "bad list command",
		}
	}
	cmd.Output = outputter
	return cmd, nil
}

func (p *Parser) parsePush(
	ctx context.Context,
	parts []string,
	outputter LineOutputter,
) (*PushCmd, error) {

	cmd := &PushCmd{}

	parseOne := func(parts []string) error {
		if len(parts) != 2 || parts[0] != cmdStringPush {
			return ParseError{
				cmd: parts,
				msg: "bad push command",
			}
		}
		rs := config.RefSpec(parts[1])
		err := rs.Validate()
		if err != nil {
			return err
		}
		cmd.Refs = append(cmd.Refs, rs)
		return nil
	}

	var err error
	outputter, err = p.parseBatch(ctx, parts, outputter, parseOne)
	if err != nil {
		return nil, err
	}
	cmd.Output = outputter
	return cmd, nil
}

func (p *Parser) parseCapabilities(
	ctx context.Context,
	parts []string,
	outputter LineOutputter,
) (*CapabilitiesCmd, error) {
	if len(parts) != 1 || parts[0] != cmdStringCap {
		return nil, ParseError{
			cmd: parts,
			msg: "bad capabilities command",
		}
	}
	return &CapabilitiesCmd{BaseCmd: BaseCmd{Output: outputter}}, nil
}

// Next returns the next command from the input. On EOF, it returns nil for both
// the command and the error. For all other errors, it returns nil and the error.
// Should be called in a loop, like so, more or less:
//
//		for {
//		  cmd, err := p.Next()
//		  if err != nil {
//		    return err
//		  }
//		  if cmd == nil {
//		    break
//		 }
//	}
func (p *Parser) Next(ctx context.Context) (Cmd, error) {

	parts, outputter, err := p.next(ctx, true)
	if err != nil {
		return nil, err
	}

	// End of conversation. We must be sure not to drop the outputter
	// on the floor so we can return out to the helper.
	if len(parts) == 0 {
		if outputter != nil {
			err = outputter.Output(nil, nil)
		}
		return nil, err
	}

	switch parts[0] {
	case cmdStringCap:
		return p.parseCapabilities(ctx, parts, outputter)
	case cmdStringList:
		return p.parseList(ctx, parts, outputter)
	case cmdStringPush:
		return p.parsePush(ctx, parts, outputter)
	case cmdStringFetch:
		return p.parseFetch(ctx, parts, outputter)
	default:
		return &UnknownCmd{
			BaseCmd: BaseCmd{Output: outputter},
			Cmd:     parts[0],
		}, nil
	}
}
