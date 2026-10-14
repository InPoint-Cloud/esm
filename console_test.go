package main

import (
	"bytes"
	"testing"
)

func TestConsoleWritesLogAboveBars(t *testing.T) {
	var out bytes.Buffer
	c := &console{out: &out, tty: true}
	bars := barWriter{c}

	c.Write([]byte("before the bars\n"))
	bars.Write([]byte("\rRead 1 / 6\n\rBulk 0 / 6\n"))
	bars.Write([]byte("\033[2A\rRead 6 / 6\n\rBulk 0 / 6\n"))
	c.Write([]byte("rejected\n"))
	c.stopBars()
	c.Write([]byte("finished\n"))

	want := "before the bars\n" +
		"\rRead 1 / 6\n\rBulk 0 / 6\n" +
		"\033[2A\rRead 6 / 6\n\rBulk 0 / 6\n" +
		// the bars are cleared, the log line is written in their place and the bars below it
		"\033[2A\r\033[Jrejected\n\rRead 6 / 6\n\rBulk 0 / 6\n" +
		"finished\n"
	if got := out.String(); got != want {
		t.Errorf("output\n%q\nwant\n%q", got, want)
	}
}

func TestConsoleSingleBar(t *testing.T) {
	var out bytes.Buffer
	c := &console{out: &out, tty: true}
	barWriter{c}.Write([]byte("\rProgress 3 / 6"))
	c.Write([]byte("rejected\n"))
	if want := "\rProgress 3 / 6\r\033[Jrejected\n\rProgress 3 / 6"; out.String() != want {
		t.Errorf("output %q, want %q", out.String(), want)
	}
}

func TestConsoleWithoutTerminal(t *testing.T) {
	var out bytes.Buffer
	c := &console{out: &out}
	barWriter{c}.Write([]byte("\rProgress 3 / 6\n"))
	c.Write([]byte("rejected\n"))
	if want := "\rProgress 3 / 6\nrejected\n"; out.String() != want {
		t.Errorf("output %q, want %q", out.String(), want)
	}
}
