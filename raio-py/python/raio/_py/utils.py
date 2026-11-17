"""Helpers that know nothing about nodes: class attributes, output streams, child processes."""

from __future__ import annotations

import sys


def declared(cls, wanted):
    """List the class's matching attributes in definition order."""
    names = {}
    for base in reversed(cls.__mro__):
        for name, value in vars(base).items():
            if wanted(value):
                names.setdefault(name)
    return [(name, getattr(cls, name)) for name in names if wanted(getattr(cls, name))]


def flush_output():
    """Write out what this process printed, so that it precedes what its child processes print."""
    sys.stdout.flush()
    sys.stderr.flush()


def buffer_output_by_line():
    """Write each line when it ends, so that the lines of several processes stay whole."""
    for stream in (sys.stdout, sys.stderr):
        stream.reconfigure(line_buffering=True)


def end_processes(children):
    """Stop the child processes that are still running."""
    for child in children:
        if child.poll() is None:
            child.kill()
    for child in children:
        child.wait()
