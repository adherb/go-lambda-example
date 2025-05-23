package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"github.com/sirupsen/logrus"
)

var (
	ErrEmptyQuery    = fmt.Errorf("search query cannot be empty")
	ErrQueryTooLong  = fmt.Errorf("search query too long (max 100 characters)")
	ErrQueryTooShort = fmt.Errorf("search query too short (min 2 characters)")
	ErrInvalidLimit  = fmt.Errorf("invalid limit parameter")
)

type FoodItem struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	Brand             string    `json:"brand,omitempty"`
	ServingSize       float64   `json:"servingSize"`
	ServingUnit       string    `json:"servingUnit"`
	Calories          float64   `json:"calories"`
	Protein           float64   `json:"protein"`
	Carbs             float64   `json:"carbs"`
	Fat               float64   `json:"fat"`
	Fiber             float64   `json:"fiber"`
	Source            string    `json:"source"`
	VerificationStatus string    `json:"verificationStatus"`
	CreatedAt         time.Time `json:"createdAt"`
	ExternalID        string    `json:"externalID"`
}

type SearchResponse struct {
	Items []FoodItem `json:"items"`
	Total int        `json:"total"`
}

type EdamamSearchResponse struct {
	Hints []struct {
		Food struct {
			FoodId    string `json:"foodId"`
			Label     string `json:"label"`
			Brand     string `json:"brand"`
			Nutrients struct {
				ENERC_KCAL float64 `json:"ENERC_KCAL"`
				PROCNT     float64 `json:"PROCNT"`
				CHOCDF     float64 `json:"CHOCDF"`
				FAT        float64 `json:"FAT"`
				FIBTG      float64 `json:"FIBTG"`
			} `json:"nutrients"`
			ServingSizes []struct {
				Quantity float64 `json:"quantity"`
				Unit     string  `json:"unit"`
			} `json:"servingSizes"`
		} `json:"food"`
	} `json:"hints"`
	Parsed []struct {
		Food struct {
			FoodId    string `json:"foodId"`
			Label     string `json:"label"`
			Brand     string `json:"brand"`
			Nutrients struct {
				ENERC_KCAL float64 `json:"ENERC_KCAL"`
				PROCNT     float64 `json:"PROCNT"`
				CHOCDF     float64 `json:"CHOCDF"`
				FAT        float64 `json:"FAT"`
				FIBTG      float64 `json:"FIBTG"`
			} `json:"nutrients"`
			ServingSizes []struct {
				Quantity float64 `json:"quantity"`
				Unit     string  `json:"unit"`
			} `json:"servingSizes"`
		} `json:"food"`
	} `json:"parsed"`
}

var rdsClient *rdsdata.Client
var rateLimiter = make(chan struct{}, 1)

func init() {
	// Configure structured logging
	logrus.SetFormatter(&logrus.JSONFormatter{})
	logrus.SetLevel(logrus.InfoLevel)
	if os.Getenv("LOG_LEVEL") == "debug" {
		logrus.SetLevel(logrus.DebugLevel)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		logrus.WithError(err).Fatal("Unable to load SDK config")
	}
	rdsClient = rdsdata.NewFromConfig(cfg)

	// Validate required environment variables at startup
	validateEnvironmentVariables()

	// Start the rate limiter goroutine
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case rateLimiter <- struct{}{}:
			default:
			}
		}
	}()
}

// Environment variable validation
func validateEnvironmentVariables() {
	required := []string{"EDAMAM_APP_ID", "EDAMAM_APP_KEY", "DB_NAME", "DB_CLUSTER_ARN", "DB_SECRET_ARN"}
	for _, env := range required {
		if os.Getenv(env) == "" {
			logrus.WithField("env_var", env).Fatal("Required environment variable is not set")
		}
	}
}

// Input validation
func validateSearchQuery(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return ErrEmptyQuery
	}
	if len(query) > 100 {
		return ErrQueryTooLong
	}
	if len(query) < 2 {
		return ErrQueryTooShort
	}
	return nil
}

func validateLimit(limitStr string) (int, error) {
	if limitStr == "" {
		return 20, nil // default
	}
	
	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return 0, ErrInvalidLimit
	}
	
	if limit <= 0 || limit > 100 {
		return 0, fmt.Errorf("limit must be between 1 and 100")
	}
	
	return limit, nil
}

func searchEdamam(ctx context.Context, query string) ([]FoodItem, error) {
	logrus.WithField("query", query).Info("Searching Edamam API")
	
	// Context timeout for rate limiter
	select {
	case <-rateLimiter:
	case <-ctx.Done():
		return nil, fmt.Errorf("rate limiter timeout: %w", ctx.Err())
	}

	appID := os.Getenv("EDAMAM_APP_ID")
	appKey := os.Getenv("EDAMAM_APP_KEY")

	url := fmt.Sprintf("https://api.edamam.com/api/food-database/v2/parser?app_id=%s&app_key=%s&ingr=%s&nutrition-type=logging",
		url.QueryEscape(appID),
		url.QueryEscape(appKey),
		url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		logrus.WithError(err).WithField("query", query).Error("Edamam API request failed")
		return nil, fmt.Errorf("edamam API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		logrus.WithFields(logrus.Fields{
			"status_code": resp.StatusCode,
			"response_body": string(body),
			"query": query,
		}).Error("Edamam API error response")
		return nil, fmt.Errorf("edamam API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"query": query,
		"response_size": len(body),
	}).Debug("Raw Edamam Response received")
	
	var searchResult EdamamSearchResponse
	if err := json.Unmarshal(body, &searchResult); err != nil {
		logrus.WithError(err).WithField("query", query).Error("Failed to decode Edamam response")
		return nil, fmt.Errorf("failed to decode edamam response: %w", err)
	}

	logrus.WithFields(logrus.Fields{
		"query": query,
		"parsed_items": len(searchResult.Parsed),
		"hint_items": len(searchResult.Hints),
	}).Info("Processing Edamam response")

	var items []FoodItem
	for _, hint := range searchResult.Hints {
		item := convertToFoodItem(hint.Food)
		items = append(items, item)
	}

	logrus.WithFields(logrus.Fields{
		"query": query,
		"converted_items": len(items),
	}).Info("Converted items from Edamam response")
	
	return items, nil
}

func convertToFoodItem(food struct {
	FoodId    string `json:"foodId"`
	Label     string `json:"label"`
	Brand     string `json:"brand"`
	Nutrients struct {
		ENERC_KCAL float64 `json:"ENERC_KCAL"`
		PROCNT     float64 `json:"PROCNT"`
		CHOCDF     float64 `json:"CHOCDF"`
		FAT        float64 `json:"FAT"`
		FIBTG      float64 `json:"FIBTG"`
	} `json:"nutrients"`
	ServingSizes []struct {
		Quantity float64 `json:"quantity"`
		Unit     string  `json:"unit"`
	} `json:"servingSizes"`
}) FoodItem {
	servingSize := 100.0
	servingUnit := "g"
	
	if len(food.ServingSizes) > 0 {
		servingSize = food.ServingSizes[0].Quantity
		servingUnit = food.ServingSizes[0].Unit
	}

	return FoodItem{
		Name:              food.Label,
		Brand:             food.Brand,
		ServingSize:       servingSize,
		ServingUnit:       servingUnit,
		Calories:          food.Nutrients.ENERC_KCAL,
		Protein:           food.Nutrients.PROCNT,
		Carbs:             food.Nutrients.CHOCDF,
		Fat:               food.Nutrients.FAT,
		Fiber:             food.Nutrients.FIBTG,
		ExternalID:        food.FoodId,
		Source:            "EDAMAM",
		VerificationStatus: "VERIFIED",
		CreatedAt:         time.Now(),
	}
}

// Batch save with better error handling
func saveItemsBatch(ctx context.Context, items []FoodItem) error {
	if len(items) == 0 {
		return nil
	}

	// Calculate reasonable batch size based on parameter count (12 params per item)
	const maxBatchSize = 80
	
	// Process in batches only if we have a very large number of items
	for i := 0; i < len(items); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(items) {
			end = len(items)
		}
		
		batch := items[i:end]
		if err := saveBatch(ctx, batch); err != nil {
			return fmt.Errorf("failed to save batch %d-%d: %w", i, end-1, err)
		}
	}
	
	return nil
}

func saveBatch(ctx context.Context, items []FoodItem) error {
	const sqlTemplate = `
		INSERT INTO food_items (
			name, brand, serving_size, serving_unit,
			calories, protein, carbs, fat, fiber,
			source, verification_status, external_id
		) VALUES %s
		ON CONFLICT (source, external_id) DO UPDATE SET
			name = EXCLUDED.name,
			brand = EXCLUDED.brand,
			serving_size = EXCLUDED.serving_size,
			serving_unit = EXCLUDED.serving_unit,
			calories = EXCLUDED.calories,
			protein = EXCLUDED.protein,
			carbs = EXCLUDED.carbs,
			fat = EXCLUDED.fat,
			fiber = EXCLUDED.fiber,
			updated_at = CURRENT_TIMESTAMP
	`
	
	valueStrings := make([]string, 0, len(items))
	valueParams := make([]rdstypes.SqlParameter, 0, len(items)*12)
	
	for i, item := range items {
		paramBase := i * 12
		valueStrings = append(valueStrings, fmt.Sprintf(
			"(:name%d, :brand%d, :serving_size%d, :serving_unit%d, "+
			":calories%d, :protein%d, :carbs%d, :fat%d, :fiber%d, "+
			":source%d, :verification_status%d, :external_id%d)",
			paramBase, paramBase, paramBase, paramBase,
			paramBase, paramBase, paramBase, paramBase, paramBase,
			paramBase, paramBase, paramBase))
			
		valueParams = append(valueParams,
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("name%d", paramBase)), Value: &rdstypes.FieldMemberStringValue{Value: item.Name}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("brand%d", paramBase)), Value: &rdstypes.FieldMemberStringValue{Value: item.Brand}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("serving_size%d", paramBase)), Value: &rdstypes.FieldMemberDoubleValue{Value: item.ServingSize}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("serving_unit%d", paramBase)), Value: &rdstypes.FieldMemberStringValue{Value: item.ServingUnit}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("calories%d", paramBase)), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Calories}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("protein%d", paramBase)), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Protein}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("carbs%d", paramBase)), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Carbs}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("fat%d", paramBase)), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Fat}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("fiber%d", paramBase)), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Fiber}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("source%d", paramBase)), Value: &rdstypes.FieldMemberStringValue{Value: item.Source}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("verification_status%d", paramBase)), Value: &rdstypes.FieldMemberStringValue{Value: item.VerificationStatus}},
			rdstypes.SqlParameter{Name: aws.String(fmt.Sprintf("external_id%d", paramBase)), Value: &rdstypes.FieldMemberStringValue{Value: item.ExternalID}},
		)
	}
	
	sql := fmt.Sprintf(sqlTemplate, strings.Join(valueStrings, ","))
	
	_, err := rdsClient.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		Database:    aws.String(os.Getenv("DB_NAME")),
		ResourceArn: aws.String(os.Getenv("DB_CLUSTER_ARN")),
		SecretArn:   aws.String(os.Getenv("DB_SECRET_ARN")),
		Sql:         aws.String(sql),
		Parameters:  valueParams,
	})
	
	if err != nil {
		return fmt.Errorf("database batch insert failed: %w", err)
	}
	
	return nil
}

func saveToDatabase(ctx context.Context, item *FoodItem) error {
	log.Printf("INFO: Saving item to database: %+v\n", item)
	
	sql := `
		INSERT INTO food_items (
			name, brand, serving_size, serving_unit,
			calories, protein, carbs, fat, fiber,
			source, verification_status, external_id
		) VALUES (
			:name, :brand, :serving_size, :serving_unit,
			:calories, :protein, :carbs, :fat, :fiber,
			:source, :verification_status, :external_id
		)
		ON CONFLICT (source, external_id) DO UPDATE SET
			name = EXCLUDED.name,
			brand = EXCLUDED.brand,
			serving_size = EXCLUDED.serving_size,
			serving_unit = EXCLUDED.serving_unit,
			calories = EXCLUDED.calories,
			protein = EXCLUDED.protein,
			carbs = EXCLUDED.carbs,
			fat = EXCLUDED.fat,
			fiber = EXCLUDED.fiber,
			updated_at = CURRENT_TIMESTAMP
		RETURNING id;
	`

	result, err := rdsClient.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		Database:    aws.String(os.Getenv("DB_NAME")),
		ResourceArn: aws.String(os.Getenv("DB_CLUSTER_ARN")),
		SecretArn:   aws.String(os.Getenv("DB_SECRET_ARN")),
		Sql:         aws.String(sql),
		Parameters: []rdstypes.SqlParameter{
			{Name: aws.String("name"), Value: &rdstypes.FieldMemberStringValue{Value: item.Name}},
			{Name: aws.String("brand"), Value: &rdstypes.FieldMemberStringValue{Value: item.Brand}},
			{Name: aws.String("serving_size"), Value: &rdstypes.FieldMemberDoubleValue{Value: item.ServingSize}},
			{Name: aws.String("serving_unit"), Value: &rdstypes.FieldMemberStringValue{Value: item.ServingUnit}},
			{Name: aws.String("calories"), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Calories}},
			{Name: aws.String("protein"), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Protein}},
			{Name: aws.String("carbs"), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Carbs}},
			{Name: aws.String("fat"), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Fat}},
			{Name: aws.String("fiber"), Value: &rdstypes.FieldMemberDoubleValue{Value: item.Fiber}},
			{Name: aws.String("source"), Value: &rdstypes.FieldMemberStringValue{Value: item.Source}},
			{Name: aws.String("verification_status"), Value: &rdstypes.FieldMemberStringValue{Value: item.VerificationStatus}},
			{Name: aws.String("external_id"), Value: &rdstypes.FieldMemberStringValue{Value: item.ExternalID}},
		},
	})
	if err != nil {
		return fmt.Errorf("database insert failed: %w", err)
	}

	if len(result.Records) > 0 && len(result.Records[0]) > 0 {
		if idField, ok := result.Records[0][0].(*rdstypes.FieldMemberStringValue); ok {
			item.ID = idField.Value
			log.Printf("INFO: Successfully saved item with ID: %s\n", item.ID)
		}
	}
	return nil
}

func searchLocalDatabase(ctx context.Context, query string, limit int) ([]FoodItem, bool, error) {
	log.Printf("INFO: Searching database for query: %s with limit: %d", query, limit)
	
	sql := `
		SELECT id, name, brand, serving_size, serving_unit,
			calories, protein, carbs, fat, fiber,
			source, verification_status, created_at, external_id
		FROM food_items 
		WHERE (LOWER(name) LIKE LOWER(:query) OR LOWER(brand) LIKE LOWER(:query))
		ORDER BY 
			source = 'EDAMAM' DESC,
			created_at DESC 
		LIMIT :limit
	`
	
	result, err := rdsClient.ExecuteStatement(ctx, &rdsdata.ExecuteStatementInput{
		Database:    aws.String(os.Getenv("DB_NAME")),
		ResourceArn: aws.String(os.Getenv("DB_CLUSTER_ARN")),
		SecretArn:   aws.String(os.Getenv("DB_SECRET_ARN")),
		Sql:         aws.String(sql),
		Parameters: []rdstypes.SqlParameter{
			{Name: aws.String("query"), Value: &rdstypes.FieldMemberStringValue{Value: "%" + query + "%"}},
			{Name: aws.String("limit"), Value: &rdstypes.FieldMemberLongValue{Value: int64(limit)}},
		},
	})
	if err != nil {
		log.Printf("ERROR: Database search failed: %v", err)
		return nil, false, fmt.Errorf("database search failed: %w", err)
	}

	log.Printf("INFO: Found %d records in database", len(result.Records))

	var items []FoodItem
	hasEdamamResults := false
	
	for i, record := range result.Records {
		if len(record) < 14 {
			log.Printf("WARN: Record %d has insufficient fields, skipping", i)
			continue
		}
		
		item := FoodItem{
			ID:                getStringValue(record[0]),
			Name:              getStringValue(record[1]),
			Brand:             getStringValue(record[2]),
			ServingSize:       getNumericValue(record[3]),
			ServingUnit:       getStringValue(record[4]),
			Calories:          getNumericValue(record[5]),
			Protein:           getNumericValue(record[6]),
			Carbs:             getNumericValue(record[7]),
			Fat:               getNumericValue(record[8]),
			Fiber:             getNumericValue(record[9]),
			Source:            getStringValue(record[10]),
			VerificationStatus: getStringValue(record[11]),
			ExternalID:        getStringValue(record[13]),
		}
		
		items = append(items, item)
		if item.Source == "EDAMAM" {
			hasEdamamResults = true
		}
	}
	
	log.Printf("INFO: Returning %d items, hasEdamamResults: %v", len(items), hasEdamamResults)
	return items, hasEdamamResults, nil
}

func getStringValue(field rdstypes.Field) string {
	if field == nil {
		return ""
	}
	if v, ok := field.(*rdstypes.FieldMemberStringValue); ok {
		return v.Value
	}
	return ""
}

func getNumericValue(field rdstypes.Field) float64 {
	if field == nil {
		return 0
	}
	
	switch v := field.(type) {
	case *rdstypes.FieldMemberDoubleValue:
		return v.Value
	case *rdstypes.FieldMemberLongValue:
		return float64(v.Value)
	case *rdstypes.FieldMemberStringValue:
		if f, err := strconv.ParseFloat(v.Value, 64); err == nil {
			return f
		}
		return 0
	default:
		log.Printf("WARN: Unknown numeric field type: %T", field)
		return 0
	}
}

func errorResponse(statusCode int, message string) events.APIGatewayProxyResponse {
	return events.APIGatewayProxyResponse{
		StatusCode: statusCode,
		Headers: map[string]string{
			"Content-Type":                "application/json",
			"Access-Control-Allow-Origin": "*",
		},
		Body: fmt.Sprintf(`{"error": "%s"}`, message),
	}
}

func handler(ctx context.Context, request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	// Add timeout to context
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	query := request.QueryStringParameters["q"]
	limitStr := request.QueryStringParameters["limit"]

	// Validate input
	if err := validateSearchQuery(query); err != nil {
		log.Printf("ERROR: Invalid query: %v", err)
		return errorResponse(http.StatusBadRequest, err.Error()), nil
	}

	limit, err := validateLimit(limitStr)
	if err != nil {
		log.Printf("ERROR: Invalid limit: %v", err)
		return errorResponse(http.StatusBadRequest, err.Error()), nil
	}
	
	// First search local database
	localResults, hasEdamamResults, err := searchLocalDatabase(ctx, query, limit)
	if err != nil {
		log.Printf("ERROR: Database search failed: %v", err)
		return errorResponse(http.StatusInternalServerError, "Database search failed"), nil
	}
	
	// Call Edamam if we don't have any Edamam results for this search
	if !hasEdamamResults {
		log.Printf("INFO: No Edamam results found, searching Edamam API...")
		
		edamamResults, err := searchEdamam(ctx, query)
		if err != nil {
			log.Printf("WARN: Edamam search failed: %v", err)
			// Continue with local results only
		} else if len(edamamResults) > 0 {
			// Use improved batch save
			if err := saveItemsBatch(ctx, edamamResults); err != nil {
				log.Printf("WARN: Failed to save Edamam results: %v", err)
			} else {
				// Search database again to get items with IDs
				localResults, _, err = searchLocalDatabase(ctx, query, limit)
				if err != nil {
					log.Printf("WARN: Failed to fetch saved items: %v", err)
				}
			}
		}
	}

	response := SearchResponse{
		Items: localResults,
		Total: len(localResults),
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Printf("ERROR: Failed to marshal response: %v", err)
		return errorResponse(http.StatusInternalServerError, "Failed to encode response"), nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Headers: map[string]string{
			"Content-Type":                "application/json",
			"Access-Control-Allow-Origin": "*",
		},
		Body: string(jsonResponse),
	}, nil
}

func main() {
	lambda.Start(handler)
}