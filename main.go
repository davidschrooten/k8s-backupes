package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

type Backup struct {
	url          string
	repoName     string
	repoPath     string
	snapshotName string
}

func (b Backup) healthCheck() error {

	resp, err := http.Get(fmt.Sprintf("%v/_cluster/health", b.url))

	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		log.Printf("K8S | Info: Connected to cluster %s", b.url)

		bodyBytes, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return err
		}

		var bodyJSON map[string]interface{}

		if err := json.Unmarshal(bodyBytes, &bodyJSON); err != nil {
			return err
		}

		if bodyJSON["status"] != "green" {
			log.Printf("K8S | Warning: Continues with health concern: %s", bodyJSON["status"])
		}

		return nil
	}

	return errors.New("Health endpoint returns status other then OK")
}

func (b Backup) createRepo() error {
	resp, err := http.Get(fmt.Sprintf("%v/_snapshot/%v", b.url, b.repoName))

	if err != nil {
		return errors.New("K8S | Error: Cannot connect to elasticsearch cluster")
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {

		client := &http.Client{}
		req, err := http.NewRequest("PUT", fmt.Sprintf("%v/_snapshot/%v", b.url, b.repoName), strings.NewReader(fmt.Sprintf(`{ "type": "fs", "settings": { "location": "/%v/%v" } }`, b.repoPath, b.repoName)))
		resp, err := client.Do(req)

		if err != nil {
			return err
		}

		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			bodyBytes, err := ioutil.ReadAll(resp.Body)

			if err != nil {
				return err
			}

			var bodyJSON map[string]interface{}

			if err := json.Unmarshal(bodyBytes, &bodyJSON); err != nil {
				return err
			}

			if bodyJSON["acknowledged"] != true {
				return errors.New("K8S | Error: Repo creation not acknowledged")
			}

			return nil
		}

		return errors.New("Cannot create new repo endpoint")
	}

	if resp.StatusCode == http.StatusOK {
		log.Print("K8S | Info: Using existing repo")
		return nil
	}

	return errors.New("Repo endpoint returns status other then OK")
}

func (b Backup) createSnapshot() error {
	client := &http.Client{}
	req, err := http.NewRequest("PUT", fmt.Sprintf("%v/_snapshot/%v/%v?wait_for_completion=true", b.url, b.repoName, b.snapshotName), strings.NewReader(""))
	resp, err := client.Do(req)

	if err != nil {
		return errors.New("K8S | Error: Cannot connect to elasticsearch cluster")
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)

		if err != nil {
			return err
		}

		var bodyJSON map[string]interface{}

		if err := json.Unmarshal(bodyBytes, &bodyJSON); err != nil {
			return err
		}

		snapshotJSON := bodyJSON["snapshot"].(map[string]interface{})

		if snapshotJSON["state"] != "SUCCESS" {
			return errors.New("Snapshot creation not acknowledged")
		}

		return nil
	}

	return errors.New("Snapshot endpoint returns status other then OK")
}

func main() {
	args := os.Args[1:]

	if len(args) != 1 {
		log.Fatalf("K8S | Error: Missing elasticsearch address as argument")
		return
	}

	currentTime := time.Now()
	hr, min, _ := currentTime.Clock()
	repoName := fmt.Sprintf("production-%v", currentTime.Format("02-01-2006"))
	snapshotName := fmt.Sprintf("%d-%02d", hr, min)

	b := Backup{
		url:          args[0],
		repoName:     repoName,
		repoPath:     "snapshots",
		snapshotName: snapshotName,
	}

	if err := b.healthCheck(); err != nil {
		log.Fatalf("K8S | Error: %v", err)
		return
	}

	if err := b.createRepo(); err != nil {
		log.Fatalf("K8S | Error: %v", err)
		return
	}

	if err := b.createSnapshot(); err != nil {
		log.Fatalf("K8S | Error: %v", err)
		return
	}

	log.Print("K8S | Success: snapshot completed")
}
