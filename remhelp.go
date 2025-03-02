package remhelp

import (
	"context"
)

type RemoteHelperOptions struct {
	DbgLog           DebugLogger // For development/internal logging
	TermLog          TermLogger  // For user-facing logging (on the terminal)
	GitCmd           string      // optional -- where to find the 'git' command if not the default
	RepackThreshhold int         // optional -- how many loose objects before we pack
}

// A RemoteHelper takes I/O from git (some via argv, and others via stdin/stdout),
// and also a storageEngine, and performs the git-remote-helper-protocol. This class
// is responsible for all of the specifics of fetch/push mechanics, etc. The storage by
// comparison is dumb.
type RemoteHelper struct {

	// args passed in via contructor
	blio  BatchedLineIOer
	store Storer
	opts  RemoteHelperOptions

	// internal state variables
	e      *ExecContext
	parser *Parser
}

func (r *RemoteHelper) Run(ctx context.Context) error {
	err := r.init(ctx)
	if err != nil {
		return err
	}
	err = r.runLoop(ctx)
	if err != nil {
		return err
	}
	return nil
}

type cmdErr struct {
	err error
	cmd Cmd
}

func (r *RemoteHelper) runLoop(ctx context.Context) error {
	mctx := NewMetaContext(ctx, r.e)
	for {

		ch := make(chan cmdErr)
		go func() {
			cmd, err := r.parser.Next(ctx)
			ch <- cmdErr{err, cmd}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()

		case msg := <-ch:
			err := msg.err
			if err != nil {
				return err
			}
			if msg.cmd == nil {
				return nil
			}
			err = msg.cmd.Run(mctx)
			if err != nil {
				return err
			}
		}
	}
}

// init the RemoteHelper, creating an execution context and configuring
// input/output
func (r *RemoteHelper) init(ctx context.Context) error {

	blio := r.blio
	if r.opts.DbgLog != nil {
		blio = NewBatchedIOLoggedO(blio, r.opts.DbgLog)
	}

	// Create the execution context
	r.e = &ExecContext{
		storage: r.store,
		opts:    r.opts,
	}

	r.parser = NewParser(blio)

	return nil
}

func NewRemoteHelper(
	blio BatchedLineIOer,
	store Storer,
	opts RemoteHelperOptions,
) (*RemoteHelper, error) {

	return &RemoteHelper{
		blio:  blio,
		store: store,
		opts:  opts,
	}, nil
}
