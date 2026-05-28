package update

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	repoOwner  = "MarchSnow-1"
	repoName   = "PortRelay"
	releasesURL = "https://api.github.com/repos/" + repoOwner + "/" + repoName + "/releases/latest"
	repoURL     = "https://github.com/" + repoOwner + "/" + repoName
)

type githubRelease struct {
	TagName string `json:"tag_name"`
}

// CheckForUpdate fetches the latest release tag from GitHub and compares it
// to the current version. If a newer version is available, it logs a message.
func CheckForUpdate(currentVersion string) {
	if currentVersion == "" || currentVersion == "dev" {
		return
	}

	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequest("GET", releasesURL, nil)
	if err != nil {
		return
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", repoOwner+"/"+repoName)

	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return
	}

	latest := strings.TrimPrefix(release.TagName, "v")
	current := strings.TrimPrefix(currentVersion, "v")

	if latest != current {
		log.Printf("[Update] New version available: %s (current: %s)", latest, currentVersion)
		log.Printf("[Update] Download: %s/releases", repoURL)
	}
}
