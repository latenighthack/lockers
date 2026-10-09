// Build the pinned generator from source with reviewed, exact source corrections.
// Generated bindings and shared module caches are never edited.
package main

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type sourcePatch struct{ File, Before, After string }

func run() error {
	if len(os.Args) != 4 {
		return fmt.Errorf("expected module version, patch file, output executable")
	}
	version, patchFile, output := os.Args[1], os.Args[2], os.Args[3]
	download := exec.Command("go", "mod", "download", "-json", "latenighthack.com/protoc-gen-kt@"+version)
	encoded, err := download.Output()
	if err != nil {
		return fmt.Errorf("download pinned generator: %w", err)
	}
	var module struct {
		Dir   string
		Error string
	}
	if err = json.Unmarshal(encoded, &module); err != nil {
		return err
	}
	if module.Error != "" || module.Dir == "" {
		return fmt.Errorf("pinned generator unavailable: %s", module.Error)
	}
	output, err = filepath.Abs(output)
	if err != nil {
		return err
	}
	stage := output + ".source"
	if err = os.RemoveAll(stage); err != nil {
		return err
	}
	err = filepath.WalkDir(module.Dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(module.Dir, path)
		if err != nil {
			return err
		}
		target := filepath.Join(stage, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected generator source file: %s", relative)
		}
		bytes, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, bytes, 0644)
	})
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(patchFile)
	if err != nil {
		return err
	}
	var patches []sourcePatch
	if err = json.Unmarshal(raw, &patches); err != nil {
		return err
	}
	for _, patch := range patches {
		path := filepath.Join(stage, patch.File)
		if !filepath.IsLocal(patch.File) {
			return fmt.Errorf("patch path must remain inside generator source")
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if patch.Before == "" || strings.Count(string(raw), patch.Before) != 1 {
			return fmt.Errorf("source patch mismatch in %s at generator %s", patch.File, version)
		}
		if err = os.WriteFile(path, []byte(strings.Replace(string(raw), patch.Before, patch.After, 1)), 0644); err != nil {
			return err
		}
	}
	build := exec.Command("go", "build", "-trimpath", "-o", output, ".")
	build.Dir = stage
	build.Stdout, build.Stderr = os.Stdout, os.Stderr
	return build.Run()
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
