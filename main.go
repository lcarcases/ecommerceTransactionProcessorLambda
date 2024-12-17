package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"net/smtp"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type MyEvent struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type MonthTransactions struct {
	total int
	sum   float64
}

func HandleRequest(ctx context.Context, event MyEvent) (string, error) {

	transactionsByMonth := make(map[string]MonthTransactions)
	totalRevenue := 0.0
	report := ""

	// Load SDK config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", fmt.Errorf("unable to load SDK config, %v", err)
	}

	// Create an Amazon S3 service client to interact with the S3
	svc := s3.NewFromConfig(cfg)

	// Set up the necessary parameters to specify which object to retrieve from the S3
	input := &s3.GetObjectInput{
		Bucket: aws.String(event.Bucket),
		Key:    aws.String(event.Key),
	}

	// Get the object (csv) from the Amazon S3 bucket
	result, err := svc.GetObject(ctx, input)
	if err != nil {
		return "", fmt.Errorf("unable to get object, %v", err)
	}

	// Ensure the response body is always closed
	defer result.Body.Close()

	// Read the CSV file
	reader := csv.NewReader(result.Body)
	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("unable to read CSV, %v", err)
	}

	for i, record := range records {
		// Avoid read header of the CSV
		if i == 0 {
			continue
		}

		// Parse the date
		date, err := time.Parse("01/02/06", record[0])
		if err != nil {
			log.Fatal(err)
		}

		month := date.Month().String()

		// Initialize the month if it does not exist
		if _, exists := transactionsByMonth[month]; !exists {
			transactionsByMonth[month] = MonthTransactions{0, 0}
		}

		// Parse the product quantity and price
		productQuantity, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			log.Fatal(err)
		}

		// Parse the product price
		productPrice, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			log.Fatal(err)
		}

		totalRevenue += productQuantity * productPrice

		// Update the total transactions and revenue for the month
		currentTransactionsMonth := transactionsByMonth[month]
		currentTransactionsMonth.sum += productQuantity * productPrice
		currentTransactionsMonth.total++
		transactionsByMonth[month] = currentTransactionsMonth

		// Generate the report
		report = fmt.Sprintf("Total Revenue: $%.2f\n", totalRevenue)
		for month, transactions := range transactionsByMonth {
			avgTransactionValue := transactions.sum / float64(transactions.total)
			report += fmt.Sprintf("Number of transactions in %s: %d\n", month, transactions.total)
			report += fmt.Sprintf("Average transaction value in %s: $%.2f\n", month, avgTransactionValue)
		}

	}

	//Mail sending

	// Set up authentication iformation
	fmt.Println("Sending email...")
	fmt.Println(os.Getenv("gmailPassword"))
	auth := smtp.PlainAuth("", "lcarcases@gmail.com", os.Getenv("gmailPassword"), "smtp.gmail.com")

	// Define the message to be sent and the recipient
	to := []string{"lcarcases@gmail.com"}
	msg := []byte("To: lcarcases@gmail.com\r\n" +
		"Subject: Monthly Report\r\n" +
		"\r\n" +
		report + "\r\n")

	// Send the email
	err = smtp.SendMail("smtp.gmail.com:587", auth, "lcarcases@gmail.com", to, msg)

	if err != nil {
		log.Fatal(err)
	}

	return "", nil

}

func main() {
	lambda.Start(HandleRequest)
}
