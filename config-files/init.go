package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	Admindatabase "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/cloudspannerecosystem/dynamodb-adapter/models"
	"google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"gopkg.in/yaml.v3"
)

var readFile = os.ReadFile

var (
	adapterTableDDL = `
	CREATE TABLE dynamodb_adapter_table_ddl (
		column STRING(MAX) NOT NULL,
		tableName STRING(MAX) NOT NULL,
		dataType STRING(MAX) NOT NULL,
		originalColumn STRING(MAX) NOT NULL,
		partitionKey STRING(MAX),
		sortKey STRING(MAX),
		spannerIndexName STRING(MAX),
		actualTable STRING(MAX)
	) PRIMARY KEY (tableName, column)`
)

func main() {
	dryRun := flag.Bool("dry_run", false, "Run the program in dry-run mode to output DDL and queries without making changes")
	flag.Parse()

	config, err := loadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}
	// Construct database name
	databaseName := fmt.Sprintf(
		"projects/%s/instances/%s/databases/%s",
		config.Spanner.ProjectID, config.Spanner.InstanceID, config.Spanner.DatabaseName,
	)
	if *dryRun {
		fmt.Println("-- Dry Run Mode: Generating Spanner DDL and Insert Queries Only --")
		runDryRun(databaseName)
	} else {
		fmt.Println("-- Executing Setup on Spanner --")
		executeSetup(databaseName)
	}
}

func loadConfig(filename string) (*models.Config, error) {
	// Read the file
	data, err := readFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Unmarshal YAML data into config struct
	var config models.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

func runDryRun(databaseName string) {
	fmt.Println("-- Spanner DDL to create the adapter table --")
	fmt.Println(adapterTableDDL)

	client := createDynamoClient()
	tables, err := listDynamoTables(client)
	if err != nil {
		log.Fatalf("Failed to list DynamoDB tables: %v", err)
	}

	for _, tableName := range tables {
		fmt.Printf("Processing table: %s\n", tableName)
		generateInsertQueries(tableName, client)
	}
}

func executeSetup(databaseName string) {
	ctx := context.Background()

	// Create Spanner database and adapter table
	if err := createDatabase(ctx, databaseName); err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}
	if err := createTable(ctx, databaseName, adapterTableDDL); err != nil {
		log.Fatalf("Failed to create adapter table: %v", err)
	}

	// Fetch and migrate data
	client := createDynamoClient()
	tables, err := listDynamoTables(client)
	if err != nil {
		log.Fatalf("Failed to list DynamoDB tables: %v", err)
	}

	for _, tableName := range tables {
		if err := migrateDynamoTableToSpanner(ctx, databaseName, tableName, client); err != nil {
			log.Printf("Failed to migrate table %s: %v", tableName, err)
		}
	}
	fmt.Println("Migration complete.")
}

func createDatabase(ctx context.Context, db string) error {
	matches := regexp.MustCompile("^(.*)/databases/(.*)$").FindStringSubmatch(db)
	if matches == nil || len(matches) != 3 {
		return fmt.Errorf("invalid database ID: %s", db)
	}

	adminClient, err := Admindatabase.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Spanner Admin client: %v", err)
	}
	defer adminClient.Close()

	op, err := adminClient.CreateDatabase(ctx, &database.CreateDatabaseRequest{
		Parent:          matches[1],
		CreateStatement: "CREATE DATABASE `" + matches[2] + "`",
	})

	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") {
			log.Printf("Database `%s` already exists. Skipping creation.", matches[2])
			return nil
		}
		return fmt.Errorf("failed to initiate database creation: %v", err)
	}

	if op == nil {
		return fmt.Errorf("received nil operation for database creation")
	}

	_, err = op.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error while waiting for database creation to complete: %v", err)
	}

	log.Printf("Database `%s` created successfully.", matches[2])
	return nil
}

func createTable(ctx context.Context, db, ddl string) error {
	adminClient, err := Admindatabase.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create Spanner Admin client: %v", err)
	}
	defer adminClient.Close()

	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to create Spanner client: %v", err)
	}
	defer client.Close()

	stmt := spanner.Statement{
		SQL: `SELECT COUNT(*) 
              FROM INFORMATION_SCHEMA.TABLES 
              WHERE TABLE_NAME = @tableName`,
		Params: map[string]interface{}{
			"tableName": "dynamodb_adapter_table_ddl",
		},
	}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var tableCount int64
	err = iter.Do(func(row *spanner.Row) error {
		return row.Columns(&tableCount)
	})
	if err != nil {
		return fmt.Errorf("failed to query table existence: %w", err)
	}

	if tableCount > 0 {
		fmt.Println("Table `dynamodb_adapter_table_ddl` already exists. Skipping creation.")
		return nil
	}

	op, err := adminClient.UpdateDatabaseDdl(ctx, &database.UpdateDatabaseDdlRequest{
		Database:   db,
		Statements: []string{ddl},
	})
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return op.Wait(ctx)
}

func listDynamoTables(client *dynamodb.Client) ([]string, error) {
	output, err := client.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}
	return output.TableNames, nil
}

func migrateDynamoTableToSpanner(ctx context.Context, db, tableName string, client *dynamodb.Client) error {
	// Fetch attributes, partition key, and sort key from DynamoDB table
	config, err := loadConfig("../config.yaml")
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}
	models.SpannerTableMap[tableName] = config.Spanner.InstanceID
	attributes, partitionKey, sortKey, err := fetchTableAttributes(client, tableName)
	if err != nil {
		return fmt.Errorf("failed to fetch attributes for table %s: %v", tableName, err)
	}

	// Generate Spanner index name and actual table name
	//	spannerIndexName := fmt.Sprintf("index_%s", tableName)
	actualTable := tableName

	// Prepare mutations to insert data into the adapter table
	var mutations []*spanner.Mutation
	for column, dataType := range attributes {
		mutations = append(mutations, spanner.InsertOrUpdate(
			"dynamodb_adapter_table_ddl",
			[]string{
				"column", "tableName", "dataType", "originalColumn",
				"partitionKey", "sortKey", "spannerIndexName", "actualTable",
			},
			[]interface{}{
				column, tableName, dataType, column,
				partitionKey, sortKey, column, actualTable,
			},
		))
	}

	// Perform batch insert into Spanner
	if err := spannerBatchInsert(ctx, db, mutations); err != nil {
		return fmt.Errorf("failed to insert metadata for table %s into Spanner: %v", tableName, err)
	}

	log.Printf("Successfully migrated metadata for DynamoDB table %s to Spanner.", tableName)
	return nil
}

func fetchTableAttributes(client *dynamodb.Client, tableName string) (map[string]string, string, string, error) {
	// Fetch table description
	output, err := client.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to describe table %s: %w", tableName, err)
	}

	// Extract partition key and sort key
	var partitionKey, sortKey string
	for _, keyElement := range output.Table.KeySchema {
		switch keyElement.KeyType {
		case dynamodbtypes.KeyTypeHash:
			partitionKey = aws.ToString(keyElement.AttributeName)
		case dynamodbtypes.KeyTypeRange:
			sortKey = aws.ToString(keyElement.AttributeName)
		}
	}

	// Extract attributes from the table
	attributes := make(map[string]string)
	scanOutput, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, "", "", fmt.Errorf("failed to scan table %s: %w", tableName, err)
	}

	for _, item := range scanOutput.Items {
		for attr, value := range item {
			attributes[attr] = inferDynamoDBType(value)
		}
	}

	return attributes, partitionKey, sortKey, nil
}

func inferDynamoDBType(attr dynamodbtypes.AttributeValue) string {
	switch attr.(type) {
	case *dynamodbtypes.AttributeValueMemberS:
		return "S"
	case *dynamodbtypes.AttributeValueMemberN:
		return "N"
	case *dynamodbtypes.AttributeValueMemberB:
		return "B"
	case *dynamodbtypes.AttributeValueMemberBOOL:
		return "BOOL"
	case *dynamodbtypes.AttributeValueMemberSS:
		return "SS"
	case *dynamodbtypes.AttributeValueMemberNS:
		return "NS"
	case *dynamodbtypes.AttributeValueMemberBS:
		return "BS"
	case *dynamodbtypes.AttributeValueMemberNULL:
		return "NULL"
	case *dynamodbtypes.AttributeValueMemberM:
		return "M"
	case *dynamodbtypes.AttributeValueMemberL:
		return "L"
	default:
		log.Printf("Unknown DynamoDB attribute type: %T\n", attr)
		return "Unknown"
	}
}

func spannerBatchInsert(ctx context.Context, db string, mutations []*spanner.Mutation) error {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return err
	}
	defer client.Close()

	_, err = client.Apply(ctx, mutations)
	return err
}

func createDynamoClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Failed to load AWS configuration: %v", err)
	}
	return dynamodb.NewFromConfig(cfg)
}

func generateInsertQueries(tableName string, client *dynamodb.Client) {
	attributes, partitionKey, sortKey, err := fetchTableAttributes(client, tableName)
	if err != nil {
		log.Printf("Failed to fetch attributes for table %s: %v", tableName, err)
		return
	}

	spannerIndexName := fmt.Sprintf("index_%s", tableName)
	actualTable := tableName

	for column, dataType := range attributes {
		query := fmt.Sprintf(
			`INSERT INTO dynamodb_adapter_table_ddl 
			(column, tableName, dataType, originalColumn, partitionKey, sortKey, spannerIndexName, actualTable) 
			VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');`,
			column, tableName, dataType, column, partitionKey, sortKey, spannerIndexName, actualTable,
		)
		fmt.Println(query)
	}
}
