package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const cacheSaveLastBackupDateAndContinueFromCache = "2006-01-02 15:04:05"

func getGithubSaveLastBackupDateAndContinueFromPath(c *appConfig) string {
	return filepath.Join(c.cacheDir, "github_save_last_backup_date_and_continue_from")
}

func handleGitRepositoryClone(client interface{}, c *appConfig) error {

	// Used for waiting for all the goroutines to finish before exiting
	startTime := time.Now()
	githubSaveLastBackupDateAndContinueFromCacheFilePath := getGithubSaveLastBackupDateAndContinueFromPath(c)

	var wg sync.WaitGroup
	defer wg.Wait()

	tokens := make(chan bool, c.maxConcurrentClones)
	gitHostUsername = getUsername(client, c.service)

	if len(gitHostUsername) == 0 && !*ignorePrivate && *useHTTPSClone {
		log.Fatal("Your Git host's username is needed for backing up private repositories via HTTPS")
	}

	if c.githubSaveLastBackupDateAndContinueFrom {
		exists, err := fileExists(githubSaveLastBackupDateAndContinueFromCacheFilePath)
		if err != nil {
			return err
		}
		if exists {
			// load
			var tmpContent string
			tmpContent, err = getFileContents(githubSaveLastBackupDateAndContinueFromCacheFilePath)
			if tmpContent != "" {
				tmpTime, timeErr := time.Parse(cacheSaveLastBackupDateAndContinueFromCache, tmpContent)
				if timeErr != nil {
					// reset?! or report error...
					return timeErr
				}
				c.githubStartFromLastPushAt = tmpTime.Format(cacheSaveLastBackupDateAndContinueFromCache)
				log.Println("filtering by last push at date -> ", c.githubStartFromLastPushAt)
			}
		}
	}

	repositories, err := getRepositories(
		client,
		c,
		c.service,
		c.githubRepoType,
		c.githubNamespaceWhitelist,
		c.gitlabProjectVisibility,
		c.gitlabProjectMembershipType,
		c.ignoreFork,
	)

	if err != nil {
		return err
	}
	if len(repositories) == 0 {
		return fmt.Errorf("no repositories retrieved")
	}

	isAnyErrorOccurred := false

	if c.githubSaveLastBackupDateAndContinueFrom {
		defer func() {
			// try saving the date...
			if isAnyErrorOccurred {
				log.Println("failed to cache the time of this current clone")
			} else {
				// Save the time
				cacheDirErr := os.MkdirAll(c.cacheDir, 0751)
				if cacheDirErr != nil {
					log.Println(fmt.Sprintf("failed to create cache dir -> %v", c.cacheDir))
					return
				}
				err = writeFileContents(githubSaveLastBackupDateAndContinueFromCacheFilePath, startTime.Format(cacheSaveLastBackupDateAndContinueFromCache))
				if err != nil {
					log.Println(fmt.Sprintf("failed to save cache file -> %s", githubSaveLastBackupDateAndContinueFromCacheFilePath))
				} else {
					log.Println(fmt.Sprintf("cache file saved successfully -> %s", githubSaveLastBackupDateAndContinueFromCacheFilePath))
				}
			}
		}()
	}

	log.Printf("Backing up %v repositories now..\n", len(repositories))
	for _, repo := range repositories {
		tokens <- true
		wg.Add(1)
		go func(repo *Repository) {
			// Backup
			stdoutStderr, err := backUp(c.backupDir, repo, c.bare, &wg)
			if err != nil {
				isAnyErrorOccurred = true
				log.Printf("Error backing up %s: %s\n", repo.Name, stdoutStderr)
			}
			<-tokens
		}(repo)
	}
	return nil
}
