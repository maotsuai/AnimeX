package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"

	"bangumi-pikpak/internal/config"
	"bangumi-pikpak/internal/episode"
	"bangumi-pikpak/internal/mikan"
	"bangumi-pikpak/internal/pikpak"
	"bangumi-pikpak/internal/rss"
	"bangumi-pikpak/internal/sanitize"
	"bangumi-pikpak/internal/storage"
	"bangumi-pikpak/internal/torrent"
)

type ResolvedEntry struct {
	Entry        rss.Entry
	BangumiTitle string
	EpisodeLabel string
	CoverURL     string
	Summary      string
}

type PikPakClient interface {
	Login() error
	EnsureFolder(parentID, name string) (string, error)
	HasOriginalURL(parentID, targetURL string) (bool, error)
	HasChildren(parentID string) (bool, error)
	OfflineDownload(name, fileURL, parentID string) (pikpak.RemoteTask, error)
	DeleteFile(id string) error
}

type EpisodeStore interface {
	EpisodeProcessed(ctx context.Context, title, label string) (bool, error)
	MarkEpisodeDownloaded(ctx context.Context, title, label, folderID, torrentURL string) error
	MarkEpisodeSubmitted(ctx context.Context, title, label, folderID, torrentURL, taskID string) error
	SaveBangumiMetadata(ctx context.Context, title, coverURL, summary string) error
	MarkEpisodeHealthOK(ctx context.Context, title, label, fileID string) error
	MarkEpisodeBrokenAndFailed(ctx context.Context, title, label, reason string) error
	UncheckedCompletedEpisodeRaw(ctx context.Context, title, label string) (folderID, fileID string, found bool, err error)
}

type Runner struct {
	Config      config.Config
	HTTPClient  *http.Client
	Logger      *slog.Logger
	TorrentRoot string
	PikPak      PikPakClient
	Storage     storage.Provider
	Store       EpisodeStore
	EntriesFunc func(context.Context) ([]ResolvedEntry, error)
	Strict      bool
}

type plannedEntry struct {
	ResolvedEntry
	EpisodeLabel string
	LocalPath    string
}

func (r Runner) RunOnce(ctx context.Context) error {
	entries, err := r.entries(ctx)
	if err != nil {
		return err
	}
	r.log().Info("resolved RSS entries", "count", len(entries))

	newEntries := make([]plannedEntry, 0, len(entries))
	seenEpisodes := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		episodeLabel := strings.TrimSpace(entry.EpisodeLabel)
		if episodeLabel == "" {
			var ok bool
			episodeLabel, ok = episode.LabelFromTitle(entry.Entry.Title)
			if !ok {
				r.log().Warn("skip entry because episode number cannot be parsed", "bangumi", entry.BangumiTitle, "entry", entry.Entry.Title)
				if r.Strict {
					return fmt.Errorf("episode number cannot be parsed: %s", entry.Entry.Title)
				}
				continue
			}
		}
		folderName := sanitize.Name(entry.BangumiTitle)
		episodeLabel = sanitize.Name(episodeLabel)
		key := folderName + "\x00" + episodeLabel
		if _, exists := seenEpisodes[key]; exists {
			r.log().Info("skip duplicate episode release by RSS order", "bangumi", folderName, "episode", episodeLabel, "entry", entry.Entry.Title, "torrent", entry.Entry.TorrentURL)
			continue
		}
		seenEpisodes[key] = struct{}{}

		processed := false
		if r.Store != nil {
			var err error
			processed, err = r.Store.EpisodeProcessed(ctx, folderName, episodeLabel)
			if err != nil {
				r.log().Warn("mysql episode state check failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
			}
		} else {
			processed = torrent.MarkerExists(r.torrentRoot(), folderName, episodeLabel)
		}
		if processed {
			r.log().Info("episode already processed", "bangumi", folderName, "episode", episodeLabel, "state", map[bool]string{true: "mysql", false: "local"}[r.Store != nil])
			continue
		}
		localPath, err := torrent.LocalEpisodePath(r.torrentRoot(), folderName, episodeLabel, entry.Entry.TorrentURL)
		if err != nil {
			r.log().Warn("skip entry with invalid torrent url", "title", entry.Entry.Title, "error", err)
			if r.Strict {
				return fmt.Errorf("invalid torrent url for %s: %w", entry.Entry.Title, err)
			}
			continue
		}
		r.log().Info("detected new torrent", "bangumi", folderName, "episode", episodeLabel, "entry", entry.Entry.Title, "torrent", entry.Entry.TorrentURL, "local_path", localPath)
		newEntries = append(newEntries, plannedEntry{ResolvedEntry: entry, EpisodeLabel: episodeLabel, LocalPath: localPath})
	}
	if len(newEntries) == 0 {
		r.log().Info("RSS source has no new updates", "checked", len(entries))
		return nil
	}

	provider := r.storageProvider()
	if provider == nil {
		return fmt.Errorf("storage provider is not initialized")
	}
	if provider.Name() == "pikpak" {
		r.log().Info("new torrents detected, logging in to PikPak", "count", len(newEntries), "username", r.Config.Username)
	} else {
		r.log().Info("new torrents detected, initializing storage provider", "count", len(newEntries), "provider", provider.Name())
	}
	if err := provider.Login(ctx); err != nil {
		r.log().Error("storage provider login/init failed", "provider", provider.Name(), "error", err)
		if provider.Name() == "pikpak" {
			return fmt.Errorf("pikpak login: %w", err)
		}
		return fmt.Errorf("%s init: %w", provider.Name(), err)
	}
	if provider.Name() == "pikpak" {
		r.log().Info("PikPak login succeeded", "username", r.Config.Username)
	} else {
		r.log().Info("storage provider ready", "provider", provider.Name())
	}

	for _, entry := range newEntries {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		folderName := sanitize.Name(entry.BangumiTitle)
		r.log().Info("processing new bangumi torrent", "bangumi", folderName, "episode", entry.EpisodeLabel, "entry", entry.Entry.Title, "torrent", entry.Entry.TorrentURL)

		bangumiFolder, err := provider.EnsureBangumi(ctx, folderName)
		if err != nil {
			r.log().Error("ensure storage bangumi folder failed", "provider", provider.Name(), "bangumi", folderName, "error", err)
			if r.Strict {
				return fmt.Errorf("ensure storage bangumi folder %s: %w", folderName, err)
			}
			continue
		}
		r.log().Info("storage bangumi folder ready", "provider", provider.Name(), "bangumi", folderName, "folder_id", bangumiFolder.ID, "path", bangumiFolder.Path)

		episodeFolder, err := provider.EnsureEpisode(ctx, bangumiFolder, entry.EpisodeLabel)
		if err != nil {
			r.log().Error("ensure storage episode folder failed", "provider", provider.Name(), "bangumi", folderName, "episode", entry.EpisodeLabel, "error", err)
			if r.Strict {
				return fmt.Errorf("ensure storage episode folder %s/%s: %w", folderName, entry.EpisodeLabel, err)
			}
			continue
		}
		r.log().Info("storage episode folder ready", "provider", provider.Name(), "bangumi", folderName, "episode", entry.EpisodeLabel, "folder_id", episodeFolder.ID, "path", episodeFolder.Path)

		hasChildren, err := provider.HasChildren(ctx, episodeFolder)
		if err != nil {
			r.log().Warn("remote episode folder check failed", "bangumi", folderName, "episode", entry.EpisodeLabel, "error", err)
		}
		if hasChildren {
			r.handleExistingFolder(ctx, provider, folderName, entry.EpisodeLabel, episodeFolder, entry.Entry.TorrentURL)
			continue
		}

		name := filepath.Base(entry.LocalPath)
		task, err := provider.SubmitDownload(ctx, storage.DownloadTask{Name: name, SourceURL: entry.Entry.TorrentURL, BangumiTitle: folderName, EpisodeLabel: entry.EpisodeLabel, Folder: episodeFolder})
		if err != nil {
			r.log().Error("offline download failed", "bangumi", folderName, "episode", entry.EpisodeLabel, "error", err)
			if r.Strict {
				return fmt.Errorf("submit download to %s for %s/%s: %w", provider.Name(), folderName, entry.EpisodeLabel, err)
			}
			continue
		}
		r.markSubmitted(ctx, folderName, entry.EpisodeLabel, episodeFolder.ID, entry.Entry.TorrentURL, task.ID, provider.Name())
		if entry.CoverURL != "" || entry.Summary != "" {
			r.saveMetadata(ctx, folderName, entry.CoverURL, entry.Summary)
		}
		r.log().Info("submitted offline download", "bangumi", folderName, "episode", entry.EpisodeLabel, "task", task.ID, "torrent", entry.Entry.TorrentURL)
	}
	return nil
}

// handleExistingFolder runs when the cycle finds the episode folder already
// has children. For PikPak rows that are completed but never health-checked
// (legacy / Phase-2-upgrade), we run a one-time health check inline.
//
// We deliberately do NOT auto-resubmit from this path: legacy torrent_url
// values may be stale (the Mikan link is gone, the seed has died). Instead
// we mark the row 'failed' so the admin can manually retry from the
// failed-episodes dashboard.
func (r Runner) handleExistingFolder(ctx context.Context, provider storage.Provider, folderName, episodeLabel string, episodeFolder storage.Folder, torrentURL string) {
	r.log().Info("skip episode because remote folder already has files", "bangumi", folderName, "episode", episodeLabel)
	if r.Store == nil {
		r.markCompletedFallback(ctx, folderName, episodeLabel, episodeFolder.ID, torrentURL)
		return
	}
	folderID, fileID, found, err := r.Store.UncheckedCompletedEpisodeRaw(ctx, folderName, episodeLabel)
	if err != nil {
		r.log().Warn("cycle: unchecked-episode lookup failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
		return
	}
	if !found {
		// Either no row, or already health-checked. Just record the legacy
		// "completed" fact for rows that came from non-PikPak providers.
		r.markCompletedFallback(ctx, folderName, episodeLabel, episodeFolder.ID, torrentURL)
		return
	}
	checkFolderID := folderID
	if strings.TrimSpace(checkFolderID) == "" {
		checkFolderID = episodeFolder.ID
	}
	lister, ok := r.folderLister()
	if !ok {
		// No PikPak adapter available (e.g. provider is local/aria2). Trust the row.
		_ = r.Store.MarkEpisodeHealthOK(ctx, folderName, episodeLabel, fileID)
		return
	}
	verdict, err := VerifyEpisodeFolder(ctx, lister, checkFolderID)
	if err != nil {
		r.log().Warn("cycle: health verify errored, will retry next cycle", "bangumi", folderName, "episode", episodeLabel, "error", err)
		return
	}
	if verdict.OK {
		if err := r.Store.MarkEpisodeHealthOK(ctx, folderName, episodeLabel, verdict.BestFile.ID); err != nil {
			r.log().Warn("cycle: mark health ok failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
		} else {
			r.log().Info("cycle: legacy episode verified OK", "bangumi", folderName, "episode", episodeLabel, "file", verdict.BestFile.ID)
		}
		return
	}
	if err := r.Store.MarkEpisodeBrokenAndFailed(ctx, folderName, episodeLabel, "legacy health check failed: "+verdict.Reason); err != nil {
		r.log().Warn("cycle: mark broken failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
		return
	}
	r.log().Warn("cycle: legacy episode broken; marked failed for admin retry", "bangumi", folderName, "episode", episodeLabel, "reason", verdict.Reason)
}

// folderLister returns a PikPak-flavored FolderLister if the runner has a
// PikPak client wired up; otherwise (false, nil). Non-PikPak providers
// (drive115, aria2) skip the health check altogether.
func (r Runner) folderLister() (FolderLister, bool) {
	if r.PikPak == nil {
		return nil, false
	}
	if lister, ok := r.PikPak.(FolderLister); ok {
		return lister, true
	}
	return nil, false
}

func (r Runner) storageProvider() storage.Provider {
	if r.Storage != nil {
		return r.Storage
	}
	if r.PikPak != nil {
		return storage.NewPikPakProvider(r.PikPak, r.Config.Path)
	}
	return nil
}

func (r Runner) entries(ctx context.Context) ([]ResolvedEntry, error) {
	if r.EntriesFunc != nil {
		return r.EntriesFunc(ctx)
	}
	r.log().Info("fetching RSS feed", "rss", r.Config.RSS)
	entries, err := rss.Fetch(r.httpClient(), r.Config.RSS)
	if err != nil {
		return nil, err
	}
	r.log().Info("RSS feed parsed", "count", len(entries))

	resolved := make([]ResolvedEntry, 0, len(entries))
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		meta, err := mikan.FetchEpisodeMetadata(r.httpClient(), entry.Link)
		if err != nil {
			r.log().Warn("skip entry because mikan title cannot be resolved", "entry", entry.Title, "error", err)
			continue
		}
		r.log().Info("recognized bangumi", "entry", entry.Title, "bangumi", meta.Title, "torrent", entry.TorrentURL)
		resolved = append(resolved, ResolvedEntry{Entry: entry, BangumiTitle: meta.Title, CoverURL: meta.CoverURL})
	}
	return resolved, nil
}

func (r Runner) httpClient() *http.Client {
	if r.HTTPClient != nil {
		return r.HTTPClient
	}
	return http.DefaultClient
}

func (r Runner) torrentRoot() string {
	if r.TorrentRoot != "" {
		return r.TorrentRoot
	}
	return "torrent"
}

func (r Runner) log() *slog.Logger {
	if r.Logger != nil {
		return r.Logger
	}
	return slog.Default()
}

// markSubmitted is the new submit-time write path. PikPak callers go through
// MarkEpisodeSubmitted (status='submitted', stores task ID). Non-PikPak
// providers (drive115, aria2-local, aria2-nas) and the no-MySQL fallback
// keep the legacy "immediately completed" semantics — the Poller does not
// run for them.
func (r Runner) markSubmitted(ctx context.Context, folderName, episodeLabel, episodeFolderID, torrentURL, taskID, providerName string) {
	if r.Store == nil {
		if err := torrent.MarkDownloaded(r.torrentRoot(), folderName, episodeLabel, torrentURL); err != nil {
			r.log().Warn("write local episode marker failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
		}
		return
	}
	if providerName == "pikpak" && strings.TrimSpace(taskID) != "" {
		if err := r.Store.MarkEpisodeSubmitted(ctx, folderName, episodeLabel, episodeFolderID, torrentURL, taskID); err != nil {
			r.log().Warn("write mysql episode submitted failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
		}
		return
	}
	if err := r.Store.MarkEpisodeDownloaded(ctx, folderName, episodeLabel, episodeFolderID, torrentURL); err != nil {
		r.log().Warn("write mysql episode state failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
	}
}

// markCompletedFallback is used by the existing-folder branch when the
// runner has no MySQL store (or for non-PikPak providers where there's
// nothing more to verify).
func (r Runner) markCompletedFallback(ctx context.Context, folderName, episodeLabel, episodeFolderID, torrentURL string) {
	if r.Store != nil {
		if err := r.Store.MarkEpisodeDownloaded(ctx, folderName, episodeLabel, episodeFolderID, torrentURL); err != nil {
			r.log().Warn("write mysql episode state failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
		}
		return
	}
	if err := torrent.MarkDownloaded(r.torrentRoot(), folderName, episodeLabel, torrentURL); err != nil {
		r.log().Warn("write local episode marker failed", "bangumi", folderName, "episode", episodeLabel, "error", err)
	}
}

func (r Runner) saveMetadata(ctx context.Context, folderName, coverURL, summary string) {
	if r.Store != nil {
		if err := r.Store.SaveBangumiMetadata(ctx, folderName, coverURL, summary); err != nil {
			r.log().Warn("write mysql bangumi metadata failed", "bangumi", folderName, "error", err)
		}
		return
	}
	if err := torrent.SaveBangumiMetadata(r.torrentRoot(), folderName, torrent.BangumiMetadata{Title: folderName, CoverURL: coverURL, Summary: summary}); err != nil {
		r.log().Warn("write bangumi metadata failed", "bangumi", folderName, "error", err)
	}
}

