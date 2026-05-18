package mikan

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// rssURLPattern matches Mikan's personal RSS URL. Mikan renders this href
// as a RELATIVE path ("/RSS/MyBangumi?token=...") on /Home/MyBangumi and on
// the site root, so the pattern accepts either form. Absolute form is kept
// for forward compatibility.
//
// The token segment must include URL-encoded characters; in particular the
// trailing %3d / %3D (base64 '=' padding) is load-bearing — strip it and
// Mikan returns a wrong/empty feed. The character class includes the base64
// alphabet plus the URL-encoded forms.
var rssURLPattern = regexp.MustCompile(`^(?:https?://[^/]+)?/RSS/MyBangumi\?token=[A-Za-z0-9%+/=._\-]+`)

// FetchPersonalRSS returns the personal MyBangumi RSS URL for the
// currently-logged-in session. Caller MUST have already called
// Login(client, ...).
//
// The returned URL is verbatim — never round-trip through net/url, never
// strip the trailing %3d, never normalize. Treat it as opaque.
func FetchPersonalRSS(client *http.Client) (string, error) {
	if client == nil {
		return "", fmt.Errorf("mikan client is nil")
	}
	candidates := []string{
		defaultBaseURL + "/Home/MyBangumi",
		defaultBaseURL + "/",
	}
	var lastErr error
	for _, page := range candidates {
		url, err := scrapeRSSURL(client, page)
		if err != nil {
			lastErr = err
			continue
		}
		if url != "" {
			return url, nil
		}
	}
	if lastErr != nil {
		return "", fmt.Errorf("mikan personal RSS link not found: %w", lastErr)
	}
	return "", fmt.Errorf("mikan personal RSS link not found on known pages")
}

func scrapeRSSURL(client *http.Client, pageURL string) (string, error) {
	resp, err := client.Get(pageURL)
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", pageURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("fetch %s: status %s", pageURL, resp.Status)
	}
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", pageURL, err)
	}

	found := ""
	doc.Find("input").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		val, ok := s.Attr("value")
		if !ok {
			return true
		}
		val = strings.TrimSpace(val)
		if rssURLPattern.MatchString(val) {
			found = val
			return false
		}
		return true
	})
	if found == "" {
		doc.Find("a").EachWithBreak(func(_ int, s *goquery.Selection) bool {
			href, ok := s.Attr("href")
			if !ok {
				return true
			}
			href = strings.TrimSpace(href)
			if rssURLPattern.MatchString(href) {
				found = href
				return false
			}
			return true
		})
	}
	return absolutizeRSSURL(found), nil
}

// absolutizeRSSURL promotes a relative "/RSS/MyBangumi?token=..." match to a
// full https://mikanani.me/... URL. Already-absolute matches are returned as-is.
// String concatenation only — never round-trip through net/url, which would
// re-encode the load-bearing %3d padding.
func absolutizeRSSURL(raw string) string {
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		return defaultBaseURL + raw
	}
	return defaultBaseURL + "/" + raw
}
