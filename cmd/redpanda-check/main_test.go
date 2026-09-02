package main

import (
	"io"
	"testing"
)

// TestRootCmd_RejectsUnrecognizedArgument is a regression test for
// redpanda-check#15: this binary has no subcommands of its own (rpk's
// managed-plugin layer owns install/uninstall/upgrade and intercepts them
// before ever exec'ing this binary), so any positional argument -- a typo of
// "upgrade", a stray token, anything -- must be rejected. Before the fix,
// cobra applied no Args validation and silently ran a full check instead.
func TestRootCmd_RejectsUnrecognizedArgument(t *testing.T) {
	for _, arg := range []string{"update", "bogus-nonsense-xyz"} {
		t.Run(arg, func(t *testing.T) {
			cmd := newRootCmd()
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{arg})

			err := cmd.Execute()
			if err == nil {
				t.Fatalf("expected an error for unrecognized argument %q, got nil", arg)
			}
		})
	}
}

// TestRootCmd_VersionFlagStillWorks proves the Args change doesn't break the
// legitimate zero-positional-argument, flags-only invocation.
func TestRootCmd_VersionFlagStillWorks(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{"--version"})

	if err := cmd.Execute(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
