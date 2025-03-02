package remhelp

import "strings"

type GitRepoDesc struct {
	ProtName string
	Host     string
	As       string
	RepoName string
}

func (d *GitRepoDesc) Origin() string {
	parts := []string{
		d.ProtName + ":/",
		d.Host,
	}
	if d.As != "" {
		parts = append(parts, d.As)
	}
	parts = append(parts, d.RepoName)
	return strings.Join(parts, "/")
}
