package app

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"bangumi-pikpak/internal/pikpak"
	"bangumi-pikpak/internal/storage"
	"bangumi-pikpak/internal/store"
)

// PikPak phase strings, mirrored from third_party/pikpak-go/types.go.
// Kept as package-private constants here to avoid coupling internal/app
// to the pikpakgo package directly.
const (
	phaseComplete = "PHASE_TYPE_COMPLETE"
	phaseRunning  = "PHASE_TYPE_RUNNING"
	phaseError    = "PHASE_TYPE_ERROR"
	phasePending  = "PHASE_TYPE_PENDING"
)

// PollerPikPak is the subset of the PikPak adapter the Poller needs.
type PollerPikPak interface {
	Login() error
	OfflineTasks(maxTasks int) ([]pikpak.RemoteOfflineTask, error)
	RetryOffline(taskID string) error
	RenameRemote(id, newName string) error
	List(parentID string) ([]pikpak.RemoteFile, error)
}

// Poller polls PikPak's OfflineList every Interval seconds and reconciles
// the open episodes table with reality.
//
// MaxRetriesFunc is queried on every tick so an admin change to the
// pikpak_offline_retry_count config takes effect without restarting the
// goroutine. If nil, MaxRetries is used.
type Poller struct {
	PikPak         PollerPikPak
	Provider       storage.Provider
	Store          *store.MySQLStore
	Logger         *slog.Logger
	Interval       time.Duration
	MaxRetries     int
	MaxRetriesFunc func() int
	// Trigger fires the poller immediately and resets the timer. Buffered
	// channel of size 1; admin "立即刷新" sends here.
	Trigger chan struct{}
}

func (p Poller) maxRetries() int {
	if p.MaxRetriesFunc != nil {
		return p.MaxRetriesFunc()
	}
	return p.MaxRetries
}

// TriggerPoller is a non-blocking nudge to the Poller. Safe to call when
// ch is nil.
func TriggerPoller(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Loop runs until ctx is canceled.
func (p Poller) Loop(ctx context.Context) {
	interval := p.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	log := p.logger()

	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		p.tick(ctx, log)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(interval)
		select {
		case <-ctx.Done():
			log.Info("pikpak poller stopped")
			return
		case <-timer.C:
		case <-p.Trigger:
			log.Info("pikpak poller manually triggered; timer reset")
		}
	}
}

func (p Poller) tick(ctx context.Context, log *slog.Logger) {
	if p.Store == nil || p.PikPak == nil {
		return
	}
	open, err := p.Store.OpenEpisodes(ctx)
	if err != nil {
		log.Warn("poller: list open episodes failed", "error", err)
		return
	}
	if len(open) == 0 {
		return
	}
	if err := p.PikPak.Login(); err != nil {
		log.Warn("poller: pikpak login failed", "error", err)
		return
	}
	tasks, err := p.PikPak.OfflineTasks(500)
	if err != nil {
		log.Warn("poller: list offline tasks failed", "error", err)
		return
	}
	byID := make(map[string]pikpak.RemoteOfflineTask, len(tasks))
	for _, t := range tasks {
		if t.ID == "" {
			continue
		}
		byID[t.ID] = t
	}
	for _, ep := range open {
		if err := ctx.Err(); err != nil {
			return
		}
		if ep.PikPakTaskID == "" {
			continue // legacy row written before Phase 2; nothing to track
		}
		task, ok := byID[ep.PikPakTaskID]
		switch {
		case !ok && time.Since(ep.SubmittedAt) > 10*time.Minute:
			p.fail(ctx, log, ep, "task disappeared from pikpak offline list")
		case !ok:
			// PHASE_TYPE_PENDING tasks may not show in the running/complete/error
			// filter; give them up to 10 minutes before declaring them gone.
		case task.Phase == phaseComplete:
			p.handleComplete(ctx, log, ep, task)
		case task.Phase == phaseError:
			p.handleError(ctx, log, ep, task)
		case task.Phase == phaseRunning, task.Phase == phasePending:
			if err := p.Store.UpdateEpisodePhaseByTaskID(ctx, ep.PikPakTaskID, "downloading", "", ""); err != nil {
				log.Warn("poller: mark downloading failed", "task", ep.PikPakTaskID, "error", err)
			}
		}
	}
}

func (p Poller) handleComplete(ctx context.Context, log *slog.Logger, ep store.OpenEpisode, task pikpak.RemoteOfflineTask) {
	verdict, err := VerifyEpisodeFolder(ctx, p.PikPak, ep.PikPakFolderID)
	if err != nil {
		log.Warn("poller: health verify errored; will retry next tick", "task", ep.PikPakTaskID, "error", err)
		return
	}
	if verdict.OK {
		fileID := verdict.BestFile.ID
		if fileID == "" {
			fileID = task.FileID
		}
		if err := p.Store.UpdateEpisodePhaseByTaskID(ctx, ep.PikPakTaskID, "completed", fileID, ""); err != nil {
			log.Warn("poller: mark completed failed", "task", ep.PikPakTaskID, "error", err)
			return
		}
		log.Info("episode completed",
			"bangumi", ep.Title, "episode", ep.Label,
			"file", verdict.BestFile.ID,
			"size_mb", verdict.BestFile.Size/(1024*1024))
		return
	}
	// Broken file: try to auto-resubmit if budget allows.
	if ep.RetryCount >= p.maxRetries() {
		p.fail(ctx, log, ep, "health check failed and retry budget exhausted: "+verdict.Reason)
		return
	}
	if verdict.BestFile.ID != "" {
		newName := suffixBroken(verdict.BestFile.Name)
		if err := p.PikPak.RenameRemote(verdict.BestFile.ID, newName); err != nil {
			log.Warn("poller: rename broken file failed; aborting auto-resubmit",
				"file", verdict.BestFile.ID, "error", err)
			p.fail(ctx, log, ep, "rename .bad failed: "+err.Error())
			return
		}
		log.Info("renamed broken file with .bad suffix",
			"file", verdict.BestFile.ID, "old_name", verdict.BestFile.Name, "new_name", newName)
	}
	if p.Provider == nil {
		p.fail(ctx, log, ep, "auto-resubmit unavailable: storage provider is nil")
		return
	}
	if strings.TrimSpace(ep.TorrentURL) == "" {
		p.fail(ctx, log, ep, "auto-resubmit unavailable: torrent URL not stored")
		return
	}
	newTask, err := p.Provider.SubmitDownload(ctx, storage.DownloadTask{
		Name:         ep.Label,
		SourceURL:    ep.TorrentURL,
		BangumiTitle: ep.Title,
		EpisodeLabel: ep.Label,
		Folder:       storage.Folder{ID: ep.PikPakFolderID, Name: ep.Label},
	})
	if err != nil {
		p.fail(ctx, log, ep, "auto-resubmit after broken file errored: "+err.Error())
		return
	}
	if err := p.Store.MarkEpisodeResubmittedFromBroken(ctx, ep.Title, ep.Label, newTask.ID, verdict.BestFile.ID, verdict.Reason); err != nil {
		log.Warn("poller: mark resubmitted-from-broken failed",
			"task", newTask.ID, "error", err)
		return
	}
	log.Info("episode auto-resubmitted after broken file",
		"bangumi", ep.Title, "episode", ep.Label,
		"old_task", ep.PikPakTaskID, "new_task", newTask.ID,
		"reason", verdict.Reason)
}

func (p Poller) handleError(ctx context.Context, log *slog.Logger, ep store.OpenEpisode, task pikpak.RemoteOfflineTask) {
	if ep.RetryCount >= p.maxRetries() {
		msg := strings.TrimSpace(task.Message)
		if msg == "" {
			msg = "pikpak phase=ERROR after retries exhausted"
		}
		p.fail(ctx, log, ep, msg)
		return
	}
	if err := p.PikPak.RetryOffline(ep.PikPakTaskID); err != nil {
		p.fail(ctx, log, ep, "retry rejected: "+err.Error())
		return
	}
	if _, err := p.Store.IncrementEpisodeRetry(ctx, ep.PikPakTaskID); err != nil {
		log.Warn("poller: increment retry failed", "task", ep.PikPakTaskID, "error", err)
	}
	log.Info("episode retry triggered",
		"bangumi", ep.Title, "episode", ep.Label,
		"task", ep.PikPakTaskID, "retry", ep.RetryCount+1)
}

func (p Poller) fail(ctx context.Context, log *slog.Logger, ep store.OpenEpisode, reason string) {
	if err := p.Store.UpdateEpisodePhaseByTaskID(ctx, ep.PikPakTaskID, "failed", "", reason); err != nil {
		log.Warn("poller: mark failed failed", "task", ep.PikPakTaskID, "error", err)
		return
	}
	log.Warn("episode marked failed",
		"bangumi", ep.Title, "episode", ep.Label,
		"task", ep.PikPakTaskID, "reason", reason)
}

func (p Poller) logger() *slog.Logger {
	if p.Logger != nil {
		return p.Logger
	}
	return slog.Default()
}
