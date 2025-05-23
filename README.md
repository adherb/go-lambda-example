# Food Search API Lambda

A production-ready AWS Lambda function that provides intelligent food search with database caching and external API integration.

## 🚀 Overview

Smart caching strategy:

1. **Database-first**: Search local PostgreSQL for existing results
2. **API fallback**: Query Edamam Food Database API when needed
3. **Auto-cache**: Store API results to minimize external calls

## 🏗 Architecture

```
API Gateway → Lambda Handler → PostgreSQL (RDS Aurora)
                    ↓
              Edamam Food API
```

**Key Features:**

- Serverless auto-scaling with AWS Lambda
- Intelligent caching reduces API costs
- Rate limiting respects external API limits
- Structured logging for production monitoring
- Comprehensive input validation

## 📋 API

### Endpoint

```
GET /search?q={query}&limit={limit}
```

### Parameters

- `q` (required): Search query, 2-100 characters
- `limit` (optional): Max results 1-100, default 20

### Response

```json
{
  "items": [
    {
      "name": "Chicken Breast",
      "brand": "Generic",
      "calories": 165,
      "protein": 31,
      "servingSize": 100,
      "servingUnit": "g"
    }
  ],
  "total": 1
}
```

## 🔧 Technical Highlights

### Smart Caching

```go
// Only calls external API if no cached results exist
localResults, hasEdamamResults, err := searchLocalDatabase(ctx, query, limit)
if !hasEdamamResults {
    edamamResults, err := searchEdamam(ctx, query)
    saveItemsBatch(ctx, edamamResults) // Cache for future
}
```

### Rate Limiting

```go
// Channel-based rate limiter prevents API abuse
var rateLimiter = make(chan struct{}, 1)
// Allows one request per 500ms
```

### Structured Logging

```go
logrus.WithFields(logrus.Fields{
    "query": query,
    "status_code": resp.StatusCode,
}).Error("API error")
```

### Batch Operations

- Efficient batch database inserts (80 items/batch)
- `ON CONFLICT` for upsert behavior
- Parameterized queries prevent SQL injection
