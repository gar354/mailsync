package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type MailInfo struct {
	Name   string  `json:"name"`
	ID     string  `json:"id"`
	Grades []int32 `json:"grades"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbAddr := os.Getenv("DB_ADDR")
	dbPort := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	emailOctopusAPIKey := os.Getenv("EMAIL_OCTOPUS_API_KEY")
	connUrl := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", dbUser, dbPassword, dbAddr, dbPort, dbName)
	connection, err := DBConnect(connUrl)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Open The Config and Get the Things
	f, err := os.Open("mailconfig.json")
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

	mailconfig, err := loadData(data)
	if err != nil {
		log.Fatalf("can't load data %v", err)
	}

	for _, info := range mailconfig {
		log.Printf("Unsubscribing Emails: %s", info.Name)
		err = UnsubscribeEmails(emailOctopusAPIKey, info.ID)
		if err != nil {
			log.Printf("unable to unsubscribe emails: %v", err)
			continue
		}
		rows, err := connection.QueryGrades(info.Grades)
		if err != nil {
			log.Printf("Unable to query grades: %v", err)
			continue
		}
		log.Printf("Subscribing Emails: %s", info.Name)
		SuscribeEmailsFromDB(emailOctopusAPIKey, info.ID, rows)
		log.Printf("Cleaning Unsubscribed Emails: %s", info.Name)
		err = CleanUnsubscribedEmails(emailOctopusAPIKey, info.ID)
		if err != nil {
			log.Printf("Unable to clean unsubcribed emails: %v", err)
			continue
		}
		rows.Close()
	}
	err = connection.DBClose()
	if err != nil {
		log.Fatalf("Error closing database connection: %v", err)
	}

}

func loadData(data []byte) ([]MailInfo, error) {
	var arr []MailInfo

	err := json.Unmarshal(data, &arr)
	if err != nil {
		return arr, err
	}

	return arr, nil
}
