package remhelp

import (
	"strings"
)

type OutputSet struct {
	lines []string
}

func NewOutputSet() *OutputSet {
	return &OutputSet{}
}

func (o *OutputSet) Len() int {
	return len(o.lines)
}

func (o *OutputSet) Vec(v []string) {
	o.lines = append(o.lines, strings.Join(v, " "))
}

func (o *OutputSet) Parts(v ...string) {
	o.lines = append(o.lines, strings.Join(v, " "))
}

func (o *OutputSet) Line(v string) {
	o.lines = append(o.lines, v)
}

func (o *OutputSet) Lines(v ...string) *OutputSet {
	o.lines = append(o.lines, v...)
	return o
}

func (o *OutputSet) Empty() *OutputSet {
	o.lines = append(o.lines, "")
	return o
}

func (o *OutputSet) Res() []string {
	return o.lines
}
