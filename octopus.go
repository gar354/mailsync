package main

import (
	"crypto/md5"
	"encoding/hex"
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

func queryEmailOctopus(authKey string, listID string, rows pgx.Rows) {
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
			upsertEmail(authKey, c, listID)
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

func getListInfo(authKey string, listID string) ListInfo {

	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s", listID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("Error: unable to create request %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Error: recieving request: %v", err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Error reading request response: %v", err)
	}
	var ret ListInfo
	if res.StatusCode != http.StatusOK {
		log.Println(string(body))
		log.Fatalf("Error in request response: %v", err)
	}
	err = json.Unmarshal(body, &ret)
	if err != nil {
		log.Fatalf("Error unmarshaling JSON %v", err)
	}
	res.Body.Close()
	return ret
}

func getChunk(authKey string, listID string) GetContactsResult {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts", listID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("Error: unable to create request %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("Error: recieving request: %v", err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatalf("Error reading request response: %v", err)
	}
	var ret GetContactsResult
	if res.StatusCode != http.StatusOK {
		log.Println(string(body))
		log.Fatalf("Error in getChunck request response: %v", err)
	}
	err = json.Unmarshal(body, &ret)
	if err != nil {
		log.Fatalf("Error unmarshaling JSON %v", err)
	}
	res.Body.Close()
	return ret
}

func unsubscribeEmails(authKey string, listID string) {
	info := getListInfo(authKey, listID)
	subscribed := info.Counts[0].Subscribed

	chunkSize := 100

	for i := 0; i < subscribed; i += chunkSize {
		chunk := getChunk(authKey, listID)
		unsubscribeChunk(authKey, listID, chunk.Data)

		time.Sleep(time.Second * 3)
	}
}

func unsubscribeChunk(authKey string, listID string, arr []Contact) {
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
		log.Fatalf("Error in request response: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			log.Fatalf("Error reading request response: %v", err)
		}
		log.Println(string(body))
		log.Fatalf("Error in request batch update response: %v", err)
	}

	res.Body.Close()
}

func upsertEmail(authKey string, payload UpsertContactPayload, listID string) {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts", listID)
	payload.Status = "subscribed"

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
		log.Fatalf("Error in request response: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			log.Fatalf("Error reading request response: %v", err)
		}
		log.Println(string(body))
		log.Fatalf("Error in request response: %v", err)
	}

	res.Body.Close()
}

func getAuthReq(key string) string {
	return fmt.Sprintf("Bearer %s", key)
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
