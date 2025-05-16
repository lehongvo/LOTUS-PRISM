# LOTUS-PRISM REST API

This REST API provides access to retail price intelligence data from the LOTUS-PRISM system. The data is sourced from the Gold layer in the data lake and made available through standardized endpoints with authentication and rate limiting.

## Getting Started

### Prerequisites

- Python 3.7+
- Flask
- PyJWT
- Flask-CORS
- Pandas
- PyODBC (for database connections)
- Python-dotenv

### Installation

1. Clone the repository:
```bash
git clone https://github.com/your-org/lotus-prism-api.git
cd lotus-prism-api
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with the following variables:
```
DB_CONNECTION_STRING=your_connection_string
API_SECRET_KEY=your_secret_key
RATE_LIMIT=100
DEBUG=False
PORT=5000
```

5. Run the API:
```bash
python app.py
```

## Authentication

The API uses JWT (JSON Web Tokens) for authentication. To obtain a token:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"username":"your_user","password":"your_password"}' http://localhost:5000/api/auth/token
```

The response will include a token that should be included in the Authorization header of subsequent requests:

```bash
curl -X GET -H "Authorization: Bearer your_token" http://localhost:5000/api/price-comparison
```

## Rate Limiting

The API includes rate limiting to prevent abuse. By default, this is set to 100 requests per hour per user. The rate limit can be configured in the `.env` file.

## Endpoints

### Health Check

```
GET /api/health
```

Returns the status of the API.

**Response:**
```json
{
    "status": "ok",
    "timestamp": "2023-07-21T15:30:45.123456",
    "version": "1.0.0"
}
```

### Authentication

```
POST /api/auth/token
```

Generates a JWT token for authentication.

**Request Body:**
```json
{
    "username": "your_user",
    "password": "your_password"
}
```

**Response:**
```json
{
    "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "expires_in": 86400
}
```

### Price Comparison

```
GET /api/price-comparison
```

Returns price comparison data across retailers.

**Query Parameters:**
- `category` (optional): Filter by category
- `retailer` (optional): Filter by retailer
- `date_from` (optional): Start date (YYYY-MM-DD)
- `date_to` (optional): End date (YYYY-MM-DD)

**Response:**
```json
{
    "timestamp": "2023-07-21T15:30:45.123456",
    "data": [
        {
            "category": "Vegetables",
            "retailer": "aeon",
            "avg_price": 45000,
            "min_price": 15000,
            "max_price": 80000,
            "price_competitiveness": "Competitive"
        },
        ...
    ],
    "record_count": 5
}
```

### Category Analysis

```
GET /api/category-analysis
```

Returns category analysis data.

**Query Parameters:**
- `category` (optional): Filter by category
- `retailer` (optional): Filter by retailer

**Response:**
```json
{
    "timestamp": "2023-07-21T15:30:45.123456",
    "data": [
        {
            "category": "Vegetables",
            "count": 150,
            "avg_price": 45000,
            "price_trend": "decreasing",
            "price_sensitivity": "high"
        },
        ...
    ],
    "record_count": 5,
    "categories": ["Vegetables", "Fruits", "Dairy", "Meat", "Bakery"]
}
```

### Retailer Performance

```
GET /api/retailer-performance
```

Returns retailer performance metrics.

**Query Parameters:**
- `retailer` (optional): Filter by retailer

**Response:**
```json
{
    "timestamp": "2023-07-21T15:30:45.123456",
    "data": [
        {
            "retailer": "aeon",
            "category_count": 25,
            "avg_price_index": 105,
            "market_position": "mid-market",
            "price_competitiveness_score": 85
        },
        ...
    ],
    "record_count": 5,
    "retailers": ["aeon", "lotte", "winmart", "mm_mega", "lotus"]
}
```

### Price Sensitivity Analysis

```
GET /api/price-sensitivity
```

Returns price sensitivity analysis by category.

**Query Parameters:**
- `category` (optional): Filter by category

**Response:**
```json
{
    "timestamp": "2023-07-21T15:30:45.123456",
    "data": [
        {
            "category": "Vegetables",
            "price_sensitivity_index": 0.85,
            "price_elasticity": -1.2,
            "sensitivity_level": "high"
        },
        ...
    ],
    "record_count": 5
}
```

## Documentation

The API includes a documentation endpoint:

```
GET /api/docs
```

This returns a JSON representation of the API endpoints and their parameters.

## Deployment

### Azure API Management

To deploy this API to Azure API Management:

1. Create an Azure API Management instance
2. Deploy the API to Azure App Service
3. Import the API definition into API Management
4. Configure the API Management policies for authentication, rate limiting, and caching
5. Set up monitoring and analytics

### Docker Deployment

A Dockerfile is included for containerized deployment:

```bash
docker build -t lotus-prism-api .
docker run -p 5000:5000 -e DB_CONNECTION_STRING=your_connection_string -e API_SECRET_KEY=your_secret_key lotus-prism-api
```

## Security Considerations

- The API uses JWT for authentication
- Rate limiting is implemented to prevent abuse
- Input validation is performed on all parameters
- SQL injection prevention is implemented in the database queries
- SSL/TLS is recommended for production deployments
- The API secret key should be stored securely and rotated regularly

## Error Handling

The API returns appropriate HTTP status codes and error messages:

- 200: Success
- 400: Bad Request
- 401: Unauthorized
- 404: Not Found
- 429: Too Many Requests
- 500: Internal Server Error

Error responses include a message describing the error.

## License

Copyright (c) 2023 LOTUS-PRISM. All rights reserved. 