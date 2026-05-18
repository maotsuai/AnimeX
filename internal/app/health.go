package app

import (
	"context"
	"fmt"
	"path"
	"strings"

	"bangumi-pikpak/internal/pikpak"
)

// minVideoBytes is the floor below which we treat a "video" as broken.
// Set to 5 MB per product call: smaller files are basically guaranteed to
// be a placeholder, an error page, or a stub the offline downloader emitted
// before failing.
const minVideoBytes int64 = 5 * 1024 * 1024

var videoExts = map[string]struct{}{
	".mp4":  {},
	".mkv":  {},
	".avi":  {},
	".m4v":  {},
	".mov":  {},
	".ts":   {},
	".flv":  {},
	".webm": {},
	".wmv":  {},
}

// FolderLister is the read-only subset of a storage drive needed for the
// health check. The Adapter satisfies it; tests can fake it.
type FolderLister interface {
	List(parentID string) ([]pikpak.RemoteFile, error)
}

// HealthVerdict is the outcome of one folder check.
type HealthVerdict struct {
	OK       bool
	Reason   string            // human-readable failure on !OK
	BestFile pikpak.RemoteFile // largest video file seen; empty when none
}

// VerifyEpisodeFolder lists the children of an episode folder and applies
// extension + size heuristics. Pure-ish — does not mutate any state.
func VerifyEpisodeFolder(ctx context.Context, lister FolderLister, folderID string) (HealthVerdict, error) {
	if err := ctx.Err(); err != nil {
		return HealthVerdict{}, err
	}
	if lister == nil {
		return HealthVerdict{}, fmt.Errorf("verify: nil folder lister")
	}
	if strings.TrimSpace(folderID) == "" {
		return HealthVerdict{OK: false, Reason: "episode folder id is empty"}, nil
	}
	files, err := lister.List(folderID)
	if err != nil {
		return HealthVerdict{}, err
	}
	var best pikpak.RemoteFile
	for _, f := range files {
		if f.Kind == pikpak.KindFolder {
			continue
		}
		ext := strings.ToLower(path.Ext(f.Name))
		if _, ok := videoExts[ext]; !ok {
			continue
		}
		if f.Size > best.Size {
			best = f
		}
	}
	if best.ID == "" {
		return HealthVerdict{OK: false, Reason: "no video file found in episode folder"}, nil
	}
	if best.Size < minVideoBytes {
		return HealthVerdict{
			OK:       false,
			Reason:   fmt.Sprintf("largest video %q is %.1f MB, below 5 MB threshold", best.Name, float64(best.Size)/(1024*1024)),
			BestFile: best,
		}, nil
	}
	return HealthVerdict{OK: true, BestFile: best}, nil
}

// suffixBroken inserts ".bad" before the extension so media scanners keep
// recognizing the file's container but a "bad" tag is visible to humans.
//
//	"foo.mp4"  -> "foo.bad.mp4"
//	"foo"      -> "foo.bad"
func suffixBroken(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ".bad"
	}
	ext := path.Ext(name)
	if ext == "" {
		return name + ".bad"
	}
	return strings.TrimSuffix(name, ext) + ".bad" + ext
}
