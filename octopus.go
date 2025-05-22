package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Contact struct {
	EmailAddress string `json:"email_address"`
	Status       string `json:"status"`
	ID           string `json:"id"`
}

type BatchContactsPayload struct {
	Contacts []Contact `json:"contacts"`
	ListID   string    `json:"list_id"`
}

type UpsertContactPayload struct {
	EmailAddress string `json:"email_address"`
	Status       string `json:"status"`
}

type GetContactsResult struct {
	Data   []Contact `json:"data"`
	Paging struct {
		Next struct {
			Url           string `json:"url"`
			StartingAfter string `json:"starting_after"`
		} `json:"paging"`
	} `json:"next"`
}

func SuscribeEmailsFromDB(authKey string, listID string, rows pgx.Rows) {
	const chunkSize = 50
	var chunk []UpsertContactPayload

	for rows.Next() {
		var email string
		if err := rows.Scan(&email); err != nil {
			log.Printf("Failed to scan row: %v", err)
			continue
		}

		chunk = append(chunk, UpsertContactPayload{EmailAddress: email, Status: "subscribed"})

		// When chunk is full, process it
		if len(chunk) == chunkSize {
			processChunk(chunk, authKey, listID)
			chunk = chunk[:0] // clear for next chunk
			time.Sleep(2 * time.Second)
		}
	}

	// Process remaining chunk
	if len(chunk) > 0 {
		processChunk(chunk, authKey, listID)
	}
}

func processChunk(chunk []UpsertContactPayload, authKey, listID string) {
	var wg sync.WaitGroup

	for _, contact := range chunk {
		wg.Add(1)
		go func(c UpsertContactPayload) {
			defer wg.Done()
			err := retryRequest(3, func() error {
				return upsertEmail(authKey, c, listID)
			}, time.Second*10)
			if err != nil {
				log.Printf("Error processing contact: %v", c)
			}
		}(contact)
	}

	wg.Wait()
	log.Printf("Processed chunk of %d contacts", len(chunk))
}

type ListInfo struct {
	Counts []struct {
		Pending      int `json:"pending"`
		Subscribed   int `json:"subscribed"`
		Unsubscribed int `json:"unsubscribed"`
	} `json:"counts"`
}

func getListInfo(authKey string, listID string) (ListInfo, error) {

	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s", listID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return ListInfo{}, fmt.Errorf("unable to create request: %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return ListInfo{}, fmt.Errorf("unable to make request: %v", err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return ListInfo{}, fmt.Errorf("unable to read request: %v", err)
	}
	var ret ListInfo
	if res.StatusCode != http.StatusOK {
		return ListInfo{}, fmt.Errorf("request for list info returned status code: %d, request body: %s", res.StatusCode, string(body))
	}
	err = json.Unmarshal(body, &ret)
	if err != nil {
		return ListInfo{}, fmt.Errorf("unable to unmarshal JSON from %s, error: %v", body, err)
	}
	res.Body.Close()
	return ret, nil
}

func getChunk(authKey string, listID string) (GetContactsResult, error) {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts", listID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return GetContactsResult{}, fmt.Errorf("unable to create request: %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return GetContactsResult{}, fmt.Errorf("unable to make request: %v", err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return GetContactsResult{}, fmt.Errorf("unable to read request: %v", err)
	}
	var ret GetContactsResult
	if res.StatusCode != http.StatusOK {
		return GetContactsResult{}, fmt.Errorf("request for chunk returned status code: %d, request body: %s", res.StatusCode, string(body))
	}
	err = json.Unmarshal(body, &ret)
	if err != nil {
		return GetContactsResult{}, fmt.Errorf("unable to unmarshal JSON from %s, error: %v", body, err)
	}
	res.Body.Close()
	return ret, nil
}

func UnsubscribeEmails(authKey string, listID string) error {
	info, err := getListInfo(authKey, listID)
	if err != nil {
		return fmt.Errorf("unable to get chunk: %v", err)
	}
	subscribed := info.Counts[0].Subscribed

	chunkSize := 100

	for i := 0; i < subscribed; i += chunkSize {
		chunk, err := getChunk(authKey, listID)
		if err != nil {
			return fmt.Errorf("unable to get chunk: %v", err)
		}
		err = retryRequest(3, func() error {
			return unsubscribeChunk(authKey, listID, chunk.Data)
		}, time.Second*8)
		if err != nil {
			return fmt.Errorf("unable to unsubscribe chunk: %v", err)
		}
		time.Sleep(time.Second * 3)
	}
	return nil
}

func retryRequest(numRetries int, f func() error, waitTime time.Duration) error {
	var err error = nil
	for range numRetries {
		err = f()
		if err == nil {
			return nil
		}
		time.Sleep(waitTime)
	}
	return fmt.Errorf("retried %d times, got error: %v", numRetries, err)
}

func unsubscribeChunk(authKey string, listID string, arr []Contact) error {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts/batch", listID)

	for i := range arr {
		arr[i].Status = "unsubscribed"
	}
	payload := BatchContactsPayload{
		ListID:   listID,
		Contacts: arr,
	}

	payloadString, _ := json.Marshal(payload)
	payloadReader := strings.NewReader(string(payloadString))

	req, err := http.NewRequest("PUT", url, payloadReader)
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))
	req.Header.Add("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error in request response: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("error reading request response: %v", err)
		}
		return fmt.Errorf("error in request batch update response: %s, error code: %d", string(body), res.StatusCode)
	}

	return res.Body.Close()
}

func upsertEmail(authKey string, payload UpsertContactPayload, listID string) error {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts", listID)
	payload.Status = "subscribed"

	payloadString, _ := json.Marshal(payload)
	payloadReader := strings.NewReader(string(payloadString))

	req, err := http.NewRequest("PUT", url, payloadReader)
	if err != nil {
		return fmt.Errorf("error creating upsert request: %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))
	req.Header.Add("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error in upsert request response: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("error reading request response: %v", err)
		}
		return fmt.Errorf("error in upsert request response, returncode: %d, response body: %s", res.StatusCode, string(body))
	}

	return res.Body.Close()
}

func getAuthReq(key string) string {
	return fmt.Sprintf("Bearer %s", key)
}
