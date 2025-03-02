package remhelp

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {

	input := `capabilities
push refs/heads/master:refs/heads/master
push :refs/heads/dongs
push HEAD:refs/heads/branch

fetch f1d2d2f924e986ac86fdf7b36c94bcdf32beec15 refs/heads/master
fetch e242ed3bffccdf271b7fbaf34ed72d089537b42f foo.txt
fetch 49f6005c9dbdfe7f845e2e9a77db530159fed56a HEAD

list for-push
push +refs/heads/foo:refs/heads/bar

fetch fa9ec633aced964ee283c30340faafcb2c79a0d7 refs/heads/master

`
	buf := bytes.NewBufferString(input)
	out := &bytes.Buffer{}
	blio := NewSimpleBatchedLineIO(buf, out)
	p := NewParser(blio)
	ctx := context.Background()

	var cmds []Cmd

	for {
		cmd, err := p.Next(ctx)
		require.NoError(t, err)
		if cmd == nil {
			break
		}
		cmd.zeroBaseClass()
		cmds = append(cmds, cmd)
	}

	expected := []Cmd{
		&CapabilitiesCmd{},
		&PushCmd{
			Refs: []config.RefSpec{
				"refs/heads/master:refs/heads/master",
				":refs/heads/dongs",
				"HEAD:refs/heads/branch",
			},
		},
		&FetchCmd{
			Args: []FetchArg{
				{
					Hash: plumbing.NewHash("f1d2d2f924e986ac86fdf7b36c94bcdf32beec15"),
					Name: "refs/heads/master",
				},
				{
					Hash: plumbing.NewHash("e242ed3bffccdf271b7fbaf34ed72d089537b42f"),
					Name: "foo.txt",
				},
				{
					Hash: plumbing.NewHash("49f6005c9dbdfe7f845e2e9a77db530159fed56a"),
					Name: "HEAD",
				},
			},
		},
		&ListCmd{
			ForPush: true,
		},
		&PushCmd{
			Refs: []config.RefSpec{
				"+refs/heads/foo:refs/heads/bar",
			},
		},
		&FetchCmd{
			Args: []FetchArg{
				{
					Hash: plumbing.NewHash("fa9ec633aced964ee283c30340faafcb2c79a0d7"),
					Name: "refs/heads/master",
				},
			},
		},
	}
	require.Equal(t, expected, cmds)
}
