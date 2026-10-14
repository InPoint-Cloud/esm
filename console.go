// SPDX-License-Identifier: Apache-2.0
// See LICENSE.md in the repository root for license details.

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/cheggaaa/pb/v3"
	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

// console writes the progress bars and the log to stderr. The bars are redrawn in place, so a log line
// written in between is overwritten and leaves a copy of the bars behind. A log line is written in place
// of the bars instead, and the bars are drawn again below it.
type console struct {
	mu    sync.Mutex
	out   io.Writer
	frame []byte // the last drawing of the bars, without the cursor movement in front of it
	lines int    // number of lines of frame, the cursor is below them
	drawn bool   // frame is on the screen
	tty   bool   // out is a terminal, otherwise the bars are not redrawn in place
}

// stderr understands the ansi escape codes on windows too
var stderrConsole = &console{out: colorable.NewColorableStderr(), tty: stderrIsTerminal()}

func stderrIsTerminal() bool {
	return isatty.IsTerminal(os.Stderr.Fd()) || isatty.IsCygwinTerminal(os.Stderr.Fd())
}

// Write writes a log line
func (c *console) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.drawn || !c.tty {
		return c.out.Write(p)
	}
	// back to the first line of the bars and clear them
	clear := "\r\033[J"
	if c.lines > 0 {
		clear = fmt.Sprintf("\033[%dA\r\033[J", c.lines)
	}
	io.WriteString(c.out, clear)
	n, err := c.out.Write(p)
	c.out.Write(c.frame)
	return n, err
}

// barWriter is the output of the progress bars
type barWriter struct{ c *console }

func (w barWriter) Write(p []byte) (int, error) {
	c := w.c
	c.mu.Lock()
	defer c.mu.Unlock()
	if i := bytes.IndexByte(p, '\r'); i >= 0 {
		c.frame = append(c.frame[:0], p[i:]...)
		c.lines = bytes.Count(c.frame, []byte{'\n'})
		c.drawn = true
	}
	return c.out.Write(p)
}

// startBar starts a single progress bar on the console
func (c *console) startBar(bar *pb.ProgressBar) {
	if c.tty {
		// the bar only redraws in place on a terminal, a writer is not one
		bar.Set(pb.Terminal, true).Set(pb.ReturnSymbol, "\r").Set(pb.Color, true)
	}
	bar.SetWriter(barWriter{c})
	bar.Start()
}

// startPool starts drawing the progress bars on the console
func (c *console) startPool(bars ...*pb.ProgressBar) (*pb.Pool, error) {
	pool := pb.NewPool(bars...)
	pool.Output = barWriter{c}
	return pool, pool.Start()
}

// stopBars is called when the bars are finished, the following log lines are written below them
func (c *console) stopBars() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.drawn = false
	c.frame = nil
	c.lines = 0
}
