package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type Contact struct {
	EmailAddress string         `json:"email_address"`
	Status       string         `json:"status"`
	ID           string         `json:"id,omitempty"`
	Fields       map[string]any `json:"fields"`
}

type BatchContactsPayload struct {
	Contacts []Contact `json:"contacts"`
	ListID   string    `json:"list_id"`
}

type UpsertContactPayload struct {
	EmailAddress string         `json:"email_address"`
	Status       string         `json:"status"`
	Fields       map[string]any `json:"fields"`
}

type GetContactsResult struct {
	Data   []Contact `json:"data"`
	Paging struct {
		Next struct {
			Url           string `json:"url"`
			StartingAfter string `json:"starting_after,omitempty"`
		} `json:"next"`
	} `json:"paging"`
}

type ListInfo struct {
	Counts []struct {
		Pending      int `json:"pending"`
		Subscribed   int `json:"subscribed"`
		Unsubscribed int `json:"unsubscribed"`
	} `json:"counts"`
}

func SubscribeEmails(authKey string, listID string, emails []UpsertContactPayload) {
	const chunkSize = 50

	for i := 0; i < len(emails); i += chunkSize {
		end := min(len(emails), i+chunkSize)
		chunk := emails[i:end]
		subscribeChunk(chunk, authKey, listID)
		time.Sleep(2 * time.Second)
	}
}

func DeleteEmails(authKey string, listID string, emails []Contact) {
	const chunkSize = 50

	for i := 0; i < len(emails); i += chunkSize {
		end := min(len(emails), i+chunkSize)
		chunk := emails[i:end]
		deleteChunk(chunk, authKey, listID)
		time.Sleep(2 * time.Second)
	}
}

func GetEmails(authKey string, listID string, info ListInfo) ([]Contact, error) {
	var list []Contact
	startingAfter := ""
	for i := 0; i < info.Counts[0].Subscribed; {
		chunk, err := getChunk(authKey, listID, "subscribed", 100, startingAfter)
		if err != nil {
			return list, err
		}
		list = append(list, chunk.Data...)
		startingAfter = chunk.Paging.Next.StartingAfter
		i += len(chunk.Data)
	}
	return list, nil
}

func subscribeChunk(chunk []UpsertContactPayload, authKey, listID string) {
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

func deleteChunk(chunk []Contact, authKey, listID string) {
	var wg sync.WaitGroup

	for _, c := range chunk {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			err := retryRequest(3, func() error {
				return deleteEmail(authKey, id, listID)
			}, time.Second*10)
			if err != nil {
				log.Printf("Error deleting contact: %v, %s", err, c.EmailAddress)
			}
		}(c.ID)
	}

	wg.Wait()
	log.Printf("Deleted chunk of %d contacts", len(chunk))
}

func GetListInfo(authKey string, listID string) (ListInfo, error) {

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

func getChunk(authKey string, listID string, status string, size int, startingAfter string) (GetContactsResult, error) {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts", listID)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return GetContactsResult{}, fmt.Errorf("unable to create request: %v", err)
	}

	params := req.URL.Query()
	params.Add("status", status)
	params.Add("limit", strconv.Itoa(size))
	if startingAfter != "" {
		params.Add("starting_after", startingAfter)
	}
	req.URL.RawQuery = params.Encode()

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

func GetLists(emails []Contact, rows pgx.Rows) ([]UpsertContactPayload, []Contact, error) {
	var upsertList []UpsertContactPayload
	var deleteList []Contact

	emailMap := make(map[string]Contact)
	for _, e := range emails {
		emailMap[e.EmailAddress] = e
	}

	for rows.Next() {
		var email, firstName, lastName string
		if err := rows.Scan(&email, &firstName, &lastName); err != nil {
			log.Printf("Failed to scan row: %v", err)
		}

		if _, found := emailMap[email]; found {
			continue
		}

		// add to the upsert list, does not exist in the list or is updated
		upsertList = append(upsertList, UpsertContactPayload{
			EmailAddress: email,
			Fields:       map[string]any{"FirstName": firstName, "LastName": lastName},
			Status:       "subscribed",
		})
	}
	for _, e := range emailMap {
		deleteList = append(deleteList, e)
	}
	return upsertList, deleteList, nil
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

func upsertEmail(authKey string, payload UpsertContactPayload, listID string) error {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts", listID)
	payload.Status = "subscribed"

	payloadString, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error unmarshalling payload: %v, struct: %v", err, payload)
	}
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

func deleteEmail(authKey string, ID string, listID string) error {
	url := fmt.Sprintf("https://api.emailoctopus.com/lists/%s/contacts/%s", listID, ID)

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("error creating upsert request: %v", err)
	}

	req.Header.Add("Authorization", getAuthReq(authKey))
	req.Header.Add("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error in delete request response: %v", err)
	}
	if res.StatusCode != http.StatusOK {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("error reading request response: %v", err)
		}
		return fmt.Errorf("error in delete request response, returncode: %d, response body: %s", res.StatusCode, string(body))
	}

	return res.Body.Close()
}

func getAuthReq(key string) string {
	return fmt.Sprintf("Bearer %s", key)
}
