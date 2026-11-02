package main

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type autocompleteFlag struct {
	Name      string `json:"name"`
	Shorthand string `json:"shorthand"`
	Type      string `json:"type"`
	Default   any    `json:"default"`
	Required  bool   `json:"required"`
}

type autocompleteEntry struct {
	Path  string             `json:"path"`
	Short string             `json:"short"`
	Long  string             `json:"long"`
	Args  []string           `json:"args"`
	Use   string             `json:"use"`
	Flags []autocompleteFlag `json:"flags"`
}

func autocomplete(t *testing.T, root *cobra.Command) []autocompleteEntry {
	t.Helper()
	var buf bytes.Buffer
	if err := printAutocomplete(&buf, root); err != nil {
		t.Fatal(err)
	}
	var entries []autocompleteEntry
	if err := json.Unmarshal(buf.Bytes(), &entries); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, buf.String())
	}
	return entries
}

func TestAutocompleteFlags(t *testing.T) {
	root := newRootCmd()
	// Add the help flag and command cobra adds at execution time, plus a
	// hidden flag and a hidden subcommand, so the test sees what --help sees.
	root.InitDefaultHelpFlag()
	root.InitDefaultHelpCmd()
	root.Flags().String("secret", "", "hidden")
	root.Flags().MarkHidden("secret")
	root.AddCommand(&cobra.Command{Use: "internal", Hidden: true, Run: func(*cobra.Command, []string) {}})

	entries := autocomplete(t, root)
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1: %+v", len(entries), entries)
	}
	e := entries[0]
	if e.Path != "check" || e.Short != root.Short || e.Long != root.Long {
		t.Errorf("path/short/long mismatch: %+v", e)
	}

	// Every flag --help shows, except help itself.
	var want []string
	root.LocalFlags().VisitAll(func(f *pflag.Flag) {
		if !f.Hidden && f.Name != "help" {
			want = append(want, f.Name)
		}
	})
	if len(e.Flags) != len(want) || len(want) != 12 {
		t.Fatalf("got %d flags, want %d (12 in --help)", len(e.Flags), len(want))
	}
	if len(e.Args) != len(want) {
		t.Errorf("got %d args, want %d", len(e.Args), len(want))
	}

	byName := map[string]autocompleteFlag{}
	for _, f := range e.Flags {
		byName[f.Name] = f
	}
	for _, skipped := range []string{"help", "secret"} {
		if _, ok := byName[skipped]; ok {
			t.Errorf("flag %q should be skipped", skipped)
		}
	}

	checks := []struct {
		name, typ, short string
		def              any
	}{
		{"admin-url", "stringSlice", "", []any{}},
		{"admin-tls-skip-verify", "bool", "", false},
		{"format", "string", "", "text"},
		{"kubeconfig", "string", "", ""},
		{"namespace", "string", "n", ""},
		{"all", "bool", "a", false},
	}
	for _, c := range checks {
		f, ok := byName[c.name]
		if !ok {
			t.Errorf("missing flag %q", c.name)
			continue
		}
		if f.Type != c.typ || f.Shorthand != c.short {
			t.Errorf("%s: type %q shorthand %q, want %q %q", c.name, f.Type, f.Shorthand, c.typ, c.short)
		}
		gotDef, _ := json.Marshal(f.Default)
		wantDef, _ := json.Marshal(c.def)
		if !bytes.Equal(gotDef, wantDef) {
			t.Errorf("%s: default %s, want %s", c.name, gotDef, wantDef)
		}
	}
}

func TestAutocompleteSubcommands(t *testing.T) {
	root := newRootCmd()
	root.InitDefaultHelpCmd()
	root.InitDefaultCompletionCmd()
	sub := &cobra.Command{Use: "sub <target>", Short: "a sub", Run: func(*cobra.Command, []string) {}}
	sub.Flags().Int("count", 3, "a count")
	root.AddCommand(sub)

	entries := autocomplete(t, root)
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2: %+v", len(entries), entries)
	}
	e := entries[1]
	if e.Path != "check_sub" || len(e.Flags) != 1 || e.Flags[0].Name != "count" {
		t.Fatalf("unexpected sub entry: %+v", e)
	}
	if e.Use != "sub <target>" {
		t.Errorf("sub use %q, want the full cobra Use line %q", e.Use, "sub <target>")
	}
	if entries[0].Use != "redpanda-check" {
		t.Errorf("root use %q, want %q", entries[0].Use, "redpanda-check")
	}
	if d, ok := e.Flags[0].Default.(float64); !ok || d != 3 {
		t.Errorf("count default %#v, want number 3", e.Flags[0].Default)
	}

	// A command with no flags still emits an empty array, never null.
	root.AddCommand(&cobra.Command{Use: "bare", Run: func(*cobra.Command, []string) {}})
	var buf bytes.Buffer
	if err := printAutocomplete(&buf, root); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buf.Bytes(), []byte(`"path":"check_bare","short":"","long":"","example":"","args":[],"use":"bare","flags":[]`)) {
		t.Errorf("bare entry missing empty arrays: %s", buf.String())
	}
}

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
