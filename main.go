package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"crypto/md5"
	"encoding/hex"
	"encoding/json"

	"github.com/joho/godotenv"
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

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dbAddr := os.Getenv("DB_ADDR")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_SER")
	dbName := os.Getenv("DB_PASSWORD")

	emailOctopusAPIKey := os.Getenv("EMAIL_OCTOPUS_API_KEY")
	f, err := os.Open("sample.json")

	if err != nil {
		log.Fatal("Can't load file")
	}
	data, err := io.ReadAll(f)
	if err != nil {
		log.Fatal("Can't read file:", err)
	}

	if err != nil {
		log.Fatal("Can't load file")
	}
	log.Println(string(data))

	contact, err := loadData(data)
	if err != nil {
		log.Fatalf("can't load data %v", err)
	}

	// queryEmailOctopus(emailOctopusAPIKey, contact)
	unsubscribeEmails(emailOctopusAPIKey, "7dadb030-8fb5-11ee-9a34-59d0c2d4a70b")
	queryEmailOctopus(emailOctopusAPIKey, "7dadb030-8fb5-11ee-9a34-59d0c2d4a70b", contact)
}

func queryEmailOctopus(authKey string, listID string, arr []UpsertContactPayload) {
	chunkSize := 50
	totalContacts := len(arr)

	for i := 0; i < totalContacts; i += chunkSize {
		end := min(i+chunkSize, totalContacts)

		var wg sync.WaitGroup

		for j := i; j < end; j++ {
			wg.Add(1)
			go func(contact UpsertContactPayload) {
				defer wg.Done()
				upsertEmail(authKey, contact, listID)
			}(arr[j])
		}
		wg.Wait()
		log.Printf("%d Done", end)

		time.Sleep(time.Second * 2)
	}
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

func loadData(data []byte) ([]UpsertContactPayload, error) {
	var arr []UpsertContactPayload

	err := json.Unmarshal(data, &arr)
	if err != nil {
		return arr, err
	}

	// for i, con := range arr {
	// 	arr[i].ID = GetMD5Hash(con.EmailAddress)
	// 	arr[i].Status = "subscribed"
	// }

	return arr, nil

}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}
