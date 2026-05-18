package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"bangumi-pikpak/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLStore struct {
	db *sql.DB
}

type BangumiRecord struct {
	Title            string
	PikPakFolderID   string
	CoverURL         string
	Summary          string
	BangumiSubjectID int
	MikanBangumiID   int
	Subscribed       bool
}

type Snapshot struct {
	Key       string
	Payload   []byte
	UpdatedAt time.Time
}

func OpenMySQL(ctx context.Context, cfg config.Config) (*MySQLStore, error) {
	if !cfg.MySQLConfigured() {
		return nil, nil
	}
	dsn := strings.TrimSpace(cfg.MySQLDSN)
	if dsn == "" {
		host := strings.TrimSpace(cfg.MySQLHost)
		if host == "" {
			host = "127.0.0.1"
		}
		port := cfg.MySQLPort
		if port == 0 {
			port = 3306
		}
		params := url.Values{}
		params.Set("charset", "utf8mb4")
		params.Set("parseTime", "true")
		params.Set("loc", "Local")
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?%s", cfg.MySQLUsername, cfg.MySQLPassword, host, port, cfg.MySQLDatabase, params.Encode())
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	s := &MySQLStore{db: db}
	if err := s.EnsureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *MySQLStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *MySQLStore) EnsureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS bangumi (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			title VARCHAR(512) NOT NULL,
			pikpak_folder_id VARCHAR(128) NOT NULL DEFAULT '',
			cover_url TEXT NULL,
			summary TEXT NULL,
			bangumi_subject_id BIGINT NOT NULL DEFAULT 0,
			mikan_bangumi_id BIGINT NOT NULL DEFAULT 0,
			subscribed TINYINT(1) NOT NULL DEFAULT 0,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uq_bangumi_title (title),
			KEY idx_pikpak_folder_id (pikpak_folder_id),
			KEY idx_bangumi_subject_id (bangumi_subject_id),
			KEY idx_mikan_bangumi_id (mikan_bangumi_id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE IF NOT EXISTS episodes (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			bangumi_id BIGINT NOT NULL,
			label VARCHAR(128) NOT NULL,
			pikpak_folder_id VARCHAR(128) NOT NULL DEFAULT '',
			torrent_url TEXT NULL,
			downloaded_at TIMESTAMP NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uq_episode (bangumi_id, label),
			CONSTRAINT fk_episodes_bangumi FOREIGN KEY (bangumi_id) REFERENCES bangumi(id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE IF NOT EXISTS subscriptions (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			bangumi_id BIGINT NOT NULL,
			language INT NOT NULL DEFAULT 0,
			source VARCHAR(32) NOT NULL DEFAULT 'mikan',
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			UNIQUE KEY uq_subscription (bangumi_id, source),
			CONSTRAINT fk_subscriptions_bangumi FOREIGN KEY (bangumi_id) REFERENCES bangumi(id) ON DELETE CASCADE
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
		`CREATE TABLE IF NOT EXISTS cache_snapshots (
			cache_key VARCHAR(128) PRIMARY KEY,
			payload LONGTEXT NOT NULL,
			updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure mysql schema: %w", err)
		}
	}
	episodeColumns := []struct{ name, def string }{
		{"status", "VARCHAR(32) NOT NULL DEFAULT 'pending'"},
		{"pikpak_task_id", "VARCHAR(64) NOT NULL DEFAULT ''"},
		{"pikpak_file_id", "VARCHAR(64) NOT NULL DEFAULT ''"},
		{"submitted_at", "TIMESTAMP NULL"},
		{"failed_at", "TIMESTAMP NULL"},
		{"failed_reason", "VARCHAR(512) NOT NULL DEFAULT ''"},
		{"retry_count", "INT NOT NULL DEFAULT 0"},
		{"health_checked_at", "TIMESTAMP NULL"},
		{"health_status", "VARCHAR(16) NOT NULL DEFAULT ''"},
	}
	for _, col := range episodeColumns {
		if err := s.ensureColumn(ctx, "episodes", col.name, col.def); err != nil {
			return err
		}
	}
	if err := s.ensureIndex(ctx, "episodes", "idx_episodes_status", "status"); err != nil {
		return err
	}
	if err := s.ensureIndex(ctx, "episodes", "idx_episodes_pikpak_task_id", "pikpak_task_id"); err != nil {
		return err
	}
	// Backfill: legacy rows that were marked downloaded the moment SubmitDownload
	// returned (the v1 lie) become 'completed'. Idempotent — only touches rows
	// still in the default 'pending' status.
	if _, err := s.db.ExecContext(ctx, `UPDATE episodes SET status='completed' WHERE downloaded_at IS NOT NULL AND status='pending'`); err != nil {
		return fmt.Errorf("backfill episode status: %w", err)
	}
	return nil
}

// ensureColumn adds a column to the given table if it does not already exist.
// MySQL 5.7 does not support ADD COLUMN IF NOT EXISTS, so we probe
// INFORMATION_SCHEMA first.
func (s *MySQLStore) ensureColumn(ctx context.Context, table, column, definition string) error {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`,
		table, column).Scan(&n)
	if err != nil {
		return fmt.Errorf("probe %s.%s: %w", table, column, err)
	}
	if n > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)); err != nil {
		return fmt.Errorf("add %s.%s: %w", table, column, err)
	}
	return nil
}

func (s *MySQLStore) ensureIndex(ctx context.Context, table, indexName, columns string) error {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?`,
		table, indexName).Scan(&n)
	if err != nil {
		return fmt.Errorf("probe %s.%s: %w", table, indexName, err)
	}
	if n > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE INDEX %s ON %s (%s)", indexName, table, columns)); err != nil {
		return fmt.Errorf("create %s.%s: %w", table, indexName, err)
	}
	return nil
}

func (s *MySQLStore) UpsertBangumi(ctx context.Context, rec BangumiRecord) error {
	if s == nil {
		return nil
	}
	title := strings.TrimSpace(rec.Title)
	if title == "" {
		return nil
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO bangumi (title, pikpak_folder_id, cover_url, summary, bangumi_subject_id, mikan_bangumi_id, subscribed)
		VALUES (?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			pikpak_folder_id = IF(VALUES(pikpak_folder_id) <> '', VALUES(pikpak_folder_id), pikpak_folder_id),
			cover_url = IF(VALUES(cover_url) IS NOT NULL, VALUES(cover_url), cover_url),
			summary = IF(VALUES(summary) IS NOT NULL, VALUES(summary), summary),
			bangumi_subject_id = IF(VALUES(bangumi_subject_id) <> 0, VALUES(bangumi_subject_id), bangumi_subject_id),
			mikan_bangumi_id = IF(VALUES(mikan_bangumi_id) <> 0, VALUES(mikan_bangumi_id), mikan_bangumi_id),
			subscribed = IF(VALUES(subscribed) = 1, 1, subscribed)`,
		title, rec.PikPakFolderID, rec.CoverURL, rec.Summary, rec.BangumiSubjectID, rec.MikanBangumiID, rec.Subscribed)
	return err
}

func (s *MySQLStore) Metadata(ctx context.Context, title string) (BangumiRecord, bool, error) {
	if s == nil || strings.TrimSpace(title) == "" {
		return BangumiRecord{}, false, nil
	}
	var rec BangumiRecord
	err := s.db.QueryRowContext(ctx, `SELECT title, pikpak_folder_id, COALESCE(cover_url,''), COALESCE(summary,''), bangumi_subject_id, mikan_bangumi_id, subscribed FROM bangumi WHERE title = ?`, strings.TrimSpace(title)).Scan(&rec.Title, &rec.PikPakFolderID, &rec.CoverURL, &rec.Summary, &rec.BangumiSubjectID, &rec.MikanBangumiID, &rec.Subscribed)
	if err == sql.ErrNoRows {
		return BangumiRecord{}, false, nil
	}
	if err != nil {
		return BangumiRecord{}, false, err
	}
	return rec, true, nil
}

// EpisodeProcessed reports whether an episode is already in flight or done.
// 'failed' rows are NOT considered processed — admin must explicitly retry
// from the failed-episodes dashboard.
func (s *MySQLStore) EpisodeProcessed(ctx context.Context, title, label string) (bool, error) {
	if s == nil {
		return false, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM episodes e JOIN bangumi b ON b.id=e.bangumi_id WHERE b.title=? AND e.label=? AND e.status IN ('submitted','downloading','completed')`, strings.TrimSpace(title), strings.TrimSpace(label)).Scan(&n)
	return n > 0, err
}

// MarkEpisodeDownloaded retains the legacy "completed" semantics for the
// no-PikPak no-Poller code paths (e.g. local + aria2 storage providers).
// PikPak callers should use MarkEpisodeSubmitted + UpdateEpisodePhaseByTaskID.
func (s *MySQLStore) MarkEpisodeDownloaded(ctx context.Context, title, label, folderID, torrentURL string) error {
	if s == nil {
		return nil
	}
	if err := s.UpsertBangumi(ctx, BangumiRecord{Title: title}); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO episodes (bangumi_id, label, pikpak_folder_id, torrent_url, status, downloaded_at)
		SELECT id, ?, ?, NULLIF(?, ''), 'completed', NOW() FROM bangumi WHERE title=?
		ON DUPLICATE KEY UPDATE
			episodes.pikpak_folder_id=IF(VALUES(pikpak_folder_id)<>'',VALUES(pikpak_folder_id),episodes.pikpak_folder_id),
			episodes.torrent_url=IF(VALUES(torrent_url) IS NOT NULL,VALUES(torrent_url),episodes.torrent_url),
			episodes.status='completed',
			episodes.downloaded_at=NOW()`, strings.TrimSpace(label), strings.TrimSpace(folderID), strings.TrimSpace(torrentURL), strings.TrimSpace(title))
	return err
}

// MarkEpisodeSubmitted is the new submit-time write. Sets status='submitted'
// and stores the PikPak task ID. The Poller is responsible for transitioning
// this row to 'completed' or 'failed'.
func (s *MySQLStore) MarkEpisodeSubmitted(ctx context.Context, title, label, folderID, torrentURL, taskID string) error {
	if s == nil {
		return nil
	}
	if err := s.UpsertBangumi(ctx, BangumiRecord{Title: title}); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO episodes (bangumi_id, label, pikpak_folder_id, torrent_url, status, pikpak_task_id, submitted_at, retry_count, failed_at, failed_reason, health_checked_at, health_status)
		SELECT id, ?, ?, NULLIF(?, ''), 'submitted', ?, NOW(), 0, NULL, '', NULL, '' FROM bangumi WHERE title=?
		ON DUPLICATE KEY UPDATE
			episodes.pikpak_folder_id=IF(VALUES(pikpak_folder_id)<>'',VALUES(pikpak_folder_id),episodes.pikpak_folder_id),
			episodes.torrent_url=IF(VALUES(torrent_url) IS NOT NULL,VALUES(torrent_url),episodes.torrent_url),
			episodes.status='submitted',
			episodes.pikpak_task_id=VALUES(pikpak_task_id),
			episodes.submitted_at=NOW(),
			episodes.failed_at=NULL,
			episodes.failed_reason='',
			episodes.health_checked_at=NULL,
			episodes.health_status=''`,
		strings.TrimSpace(label), strings.TrimSpace(folderID), strings.TrimSpace(torrentURL), strings.TrimSpace(taskID), strings.TrimSpace(title))
	return err
}

// UpdateEpisodePhaseByTaskID is the Poller's write path. Idempotent.
//   newStatus=='completed' → also sets downloaded_at=NOW(), pikpak_file_id=fileID,
//                            health_checked_at=NOW(), health_status='ok'.
//   newStatus=='failed'    → also sets failed_at=NOW(), failed_reason=reason.
//   newStatus=='downloading' → no-op if already 'downloading'.
func (s *MySQLStore) UpdateEpisodePhaseByTaskID(ctx context.Context, taskID, newStatus, fileID, reason string) error {
	if s == nil {
		return nil
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return fmt.Errorf("UpdateEpisodePhaseByTaskID: empty task id")
	}
	switch newStatus {
	case "completed":
		_, err := s.db.ExecContext(ctx,
			`UPDATE episodes SET status='completed', downloaded_at=NOW(), pikpak_file_id=NULLIF(?, ''), health_checked_at=NOW(), health_status='ok' WHERE pikpak_task_id=?`,
			strings.TrimSpace(fileID), taskID)
		return err
	case "failed":
		_, err := s.db.ExecContext(ctx,
			`UPDATE episodes SET status='failed', failed_at=NOW(), failed_reason=? WHERE pikpak_task_id=?`,
			truncate(reason, 500), taskID)
		return err
	case "downloading":
		_, err := s.db.ExecContext(ctx,
			`UPDATE episodes SET status='downloading' WHERE pikpak_task_id=? AND status<>'downloading' AND status<>'completed'`,
			taskID)
		return err
	default:
		return fmt.Errorf("UpdateEpisodePhaseByTaskID: unknown status %q", newStatus)
	}
}

// IncrementEpisodeRetry atomically bumps retry_count and returns the new value.
func (s *MySQLStore) IncrementEpisodeRetry(ctx context.Context, taskID string) (int, error) {
	if s == nil {
		return 0, nil
	}
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return 0, fmt.Errorf("IncrementEpisodeRetry: empty task id")
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE episodes SET retry_count=retry_count+1 WHERE pikpak_task_id=?`, taskID); err != nil {
		return 0, err
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT retry_count FROM episodes WHERE pikpak_task_id=?`, taskID).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// OpenEpisode is what the Poller works with.
type OpenEpisode struct {
	BangumiID      int64
	Title          string
	Label          string
	PikPakFolderID string
	TorrentURL     string
	PikPakTaskID   string
	RetryCount     int
	SubmittedAt    time.Time
}

// OpenEpisodes returns all rows that the Poller should refresh.
func (s *MySQLStore) OpenEpisodes(ctx context.Context) ([]OpenEpisode, error) {
	if s == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT b.id, b.title, e.label, COALESCE(e.pikpak_folder_id,''), COALESCE(e.torrent_url,''), COALESCE(e.pikpak_task_id,''), e.retry_count, COALESCE(e.submitted_at, NOW())
		FROM episodes e JOIN bangumi b ON b.id=e.bangumi_id
		WHERE e.status IN ('submitted','downloading')`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OpenEpisode
	for rows.Next() {
		var ep OpenEpisode
		if err := rows.Scan(&ep.BangumiID, &ep.Title, &ep.Label, &ep.PikPakFolderID, &ep.TorrentURL, &ep.PikPakTaskID, &ep.RetryCount, &ep.SubmittedAt); err != nil {
			return nil, err
		}
		out = append(out, ep)
	}
	return out, rows.Err()
}

// UncheckedEpisode is returned for completed episodes that have not yet had
// a one-time health check.
type UncheckedEpisode struct {
	Title          string
	Label          string
	PikPakFolderID string
	PikPakFileID   string
}

// UncheckedCompletedEpisode returns the row IFF status='completed' AND
// health_checked_at IS NULL. Used by Runner.RunOnce when it's about to
// skip a folder that already has children.
func (s *MySQLStore) UncheckedCompletedEpisode(ctx context.Context, title, label string) (UncheckedEpisode, bool, error) {
	if s == nil {
		return UncheckedEpisode{}, false, nil
	}
	var ep UncheckedEpisode
	err := s.db.QueryRowContext(ctx,
		`SELECT b.title, e.label, COALESCE(e.pikpak_folder_id,''), COALESCE(e.pikpak_file_id,'')
		 FROM episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 WHERE b.title=? AND e.label=? AND e.status='completed' AND e.health_checked_at IS NULL`,
		strings.TrimSpace(title), strings.TrimSpace(label)).Scan(&ep.Title, &ep.Label, &ep.PikPakFolderID, &ep.PikPakFileID)
	if err == sql.ErrNoRows {
		return UncheckedEpisode{}, false, nil
	}
	if err != nil {
		return UncheckedEpisode{}, false, err
	}
	return ep, true, nil
}

// UncheckedCompletedEpisodeRaw is the flat-field variant used by the
// app.Runner.RunOnce existing-folder branch (avoids importing this
// package's struct types into internal/app).
func (s *MySQLStore) UncheckedCompletedEpisodeRaw(ctx context.Context, title, label string) (string, string, bool, error) {
	ep, ok, err := s.UncheckedCompletedEpisode(ctx, title, label)
	if err != nil || !ok {
		return "", "", ok, err
	}
	return ep.PikPakFolderID, ep.PikPakFileID, true, nil
}

// MarkEpisodeHealthOK records that we've verified the episode's files look
// healthy. Sets pikpak_file_id if non-empty.
func (s *MySQLStore) MarkEpisodeHealthOK(ctx context.Context, title, label, fileID string) error {
	if s == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 SET e.health_checked_at=NOW(), e.health_status='ok',
		     e.pikpak_file_id=IF(?<>'', ?, e.pikpak_file_id)
		 WHERE b.title=? AND e.label=?`,
		strings.TrimSpace(fileID), strings.TrimSpace(fileID), strings.TrimSpace(title), strings.TrimSpace(label))
	return err
}

// MarkEpisodeBrokenAndFailed flips a completed episode to 'failed' after a
// post-hoc health check finds it broken. Used by the cycle's existing-folder
// branch (we don't auto-resubmit from there because legacy torrent_url may
// be stale).
func (s *MySQLStore) MarkEpisodeBrokenAndFailed(ctx context.Context, title, label, reason string) error {
	if s == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 SET e.status='failed', e.failed_at=NOW(), e.failed_reason=?,
		     e.health_checked_at=NOW(), e.health_status='broken'
		 WHERE b.title=? AND e.label=?`,
		truncate(reason, 500), strings.TrimSpace(title), strings.TrimSpace(label))
	return err
}

// MarkEpisodeResubmittedFromBroken is called by the Poller after it renamed
// a broken file and submitted a fresh PikPak task for the same episode.
func (s *MySQLStore) MarkEpisodeResubmittedFromBroken(ctx context.Context, title, label, newTaskID, oldFileID, reason string) error {
	if s == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 SET e.status='submitted', e.pikpak_task_id=?, e.submitted_at=NOW(),
		     e.retry_count=e.retry_count+1,
		     e.failed_at=NULL, e.failed_reason=?,
		     e.health_checked_at=NULL, e.health_status='broken',
		     e.pikpak_file_id=IF(?<>'', '', e.pikpak_file_id)
		 WHERE b.title=? AND e.label=?`,
		strings.TrimSpace(newTaskID), truncate(reason, 500), strings.TrimSpace(oldFileID), strings.TrimSpace(title), strings.TrimSpace(label))
	return err
}

// FailedEpisode is returned to the admin dashboard.
type FailedEpisode struct {
	BangumiID    int64     `json:"bangumi_id"`
	BangumiTitle string    `json:"bangumi_title"`
	Label        string    `json:"label"`
	TorrentURL   string    `json:"torrent_url"`
	FailedReason string    `json:"failed_reason"`
	FailedAt     time.Time `json:"failed_at"`
	RetryCount   int       `json:"retry_count"`
}

// ListFailedEpisodes returns all rows currently in 'failed' status.
func (s *MySQLStore) ListFailedEpisodes(ctx context.Context) ([]FailedEpisode, error) {
	if s == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT b.id, b.title, e.label, COALESCE(e.torrent_url,''), COALESCE(e.failed_reason,''), COALESCE(e.failed_at, NOW()), e.retry_count
		 FROM episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 WHERE e.status='failed'
		 ORDER BY e.failed_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FailedEpisode
	for rows.Next() {
		var fe FailedEpisode
		if err := rows.Scan(&fe.BangumiID, &fe.BangumiTitle, &fe.Label, &fe.TorrentURL, &fe.FailedReason, &fe.FailedAt, &fe.RetryCount); err != nil {
			return nil, err
		}
		out = append(out, fe)
	}
	return out, rows.Err()
}

// FindFailedEpisode looks up one failed row by (title, label) and returns it.
func (s *MySQLStore) FindFailedEpisode(ctx context.Context, title, label string) (FailedEpisode, bool, error) {
	if s == nil {
		return FailedEpisode{}, false, nil
	}
	var fe FailedEpisode
	err := s.db.QueryRowContext(ctx,
		`SELECT b.id, b.title, e.label, COALESCE(e.torrent_url,''), COALESCE(e.failed_reason,''), COALESCE(e.failed_at, NOW()), e.retry_count
		 FROM episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 WHERE b.title=? AND e.label=? AND e.status='failed'`,
		strings.TrimSpace(title), strings.TrimSpace(label)).Scan(&fe.BangumiID, &fe.BangumiTitle, &fe.Label, &fe.TorrentURL, &fe.FailedReason, &fe.FailedAt, &fe.RetryCount)
	if err == sql.ErrNoRows {
		return FailedEpisode{}, false, nil
	}
	if err != nil {
		return FailedEpisode{}, false, err
	}
	return fe, true, nil
}

// ResetEpisodeForRetry clears 'failed' state so the cycle / one-shot RunOnce
// will re-process this episode. retry_count is preserved (acts as a lifetime
// counter, not a per-attempt budget).
func (s *MySQLStore) ResetEpisodeForRetry(ctx context.Context, title, label string) error {
	if s == nil {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE episodes e JOIN bangumi b ON b.id=e.bangumi_id
		 SET e.status='pending', e.failed_at=NULL, e.failed_reason='',
		     e.pikpak_task_id='', e.health_checked_at=NULL, e.health_status=''
		 WHERE b.title=? AND e.label=? AND e.status='failed'`,
		strings.TrimSpace(title), strings.TrimSpace(label))
	return err
}

func truncate(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	return s[:max]
}

func (s *MySQLStore) SaveBangumiMetadata(ctx context.Context, title, coverURL, summary string) error {
	return s.UpsertBangumi(ctx, BangumiRecord{Title: title, CoverURL: coverURL, Summary: summary})
}

func (s *MySQLStore) DeleteBangumi(ctx context.Context, titles []string) (int64, error) {
	if s == nil || len(titles) == 0 {
		return 0, nil
	}
	cleaned := make([]string, 0, len(titles))
	for _, title := range titles {
		if title = strings.TrimSpace(title); title != "" {
			cleaned = append(cleaned, title)
		}
	}
	if len(cleaned) == 0 {
		return 0, nil
	}
	placeholders := strings.TrimRight(strings.Repeat("?,", len(cleaned)), ",")
	args := make([]any, 0, len(cleaned))
	for _, title := range cleaned {
		args = append(args, title)
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM bangumi WHERE title IN (`+placeholders+`)`, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *MySQLStore) DeleteEpisode(ctx context.Context, title, label string) (int64, error) {
	if s == nil {
		return 0, nil
	}
	title = strings.TrimSpace(title)
	label = strings.TrimSpace(label)
	if title == "" || label == "" {
		return 0, nil
	}
	res, err := s.db.ExecContext(ctx, `DELETE e FROM episodes e JOIN bangumi b ON b.id=e.bangumi_id WHERE b.title=? AND e.label=?`, title, label)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *MySQLStore) SaveSubscription(ctx context.Context, rec BangumiRecord, language int) error {
	if s == nil {
		return nil
	}
	rec.Subscribed = true
	if err := s.UpsertBangumi(ctx, rec); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO subscriptions (bangumi_id, language, source)
		SELECT id, ?, 'mikan' FROM bangumi WHERE title=?
		ON DUPLICATE KEY UPDATE language=VALUES(language)`, language, strings.TrimSpace(rec.Title))
	return err
}

// ListSubscribedTitles returns the titles of every bangumi flagged
// subscribed. The mobile client calls this on login to hydrate its local
// SubscribedStore so the 已订阅 button is stable across devices.
func (s *MySQLStore) ListSubscribedTitles(ctx context.Context) ([]string, error) {
	if s == nil {
		return nil, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT title FROM bangumi WHERE subscribed = 1 ORDER BY title`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *MySQLStore) SaveSnapshot(ctx context.Context, key string, value any) error {
	if s == nil || strings.TrimSpace(key) == "" {
		return nil
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal mysql snapshot: %w", err)
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO cache_snapshots (cache_key, payload, updated_at) VALUES (?, ?, NOW())
		ON DUPLICATE KEY UPDATE payload=VALUES(payload), updated_at=NOW()`, strings.TrimSpace(key), string(payload))
	return err
}

func (s *MySQLStore) LoadSnapshot(ctx context.Context, key string, target any) (time.Time, bool, error) {
	if s == nil || strings.TrimSpace(key) == "" {
		return time.Time{}, false, nil
	}
	var payload string
	var updatedAt time.Time
	err := s.db.QueryRowContext(ctx, `SELECT payload, updated_at FROM cache_snapshots WHERE cache_key=?`, strings.TrimSpace(key)).Scan(&payload, &updatedAt)
	if err == sql.ErrNoRows {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	if err := json.Unmarshal([]byte(payload), target); err != nil {
		return time.Time{}, false, fmt.Errorf("parse mysql snapshot: %w", err)
	}
	return updatedAt, true, nil
}
