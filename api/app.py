"""
LOTUS-PRISM REST API

API này cung cấp truy cập đến dữ liệu phân tích giá bán lẻ từ hệ thống LOTUS-PRISM.
Dữ liệu được lấy từ Gold layer trong Data Lake.
"""

import os
import json
import datetime
import logging
from functools import wraps

# Try to import required packages
try:
    from flask import Flask, jsonify, request, abort
    from flask_cors import CORS
    import jwt
    import pandas as pd
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Error: Required package not installed - {str(e)}")
    print("Please install required packages with: pip install -r requirements.txt")
    exit(1)

# Load environment variables
try:
    load_dotenv()
except Exception as e:
    print(f"Warning: Could not load .env file - {str(e)}")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for all domains on all routes

# Configuration
DB_CONNECTION_STRING = os.environ.get('DB_CONNECTION_STRING', '')
API_SECRET_KEY = os.environ.get('API_SECRET_KEY', 'lotus-prism-secret-key')
RATE_LIMIT = int(os.environ.get('RATE_LIMIT', '100'))  # Requests per hour

# In-memory cache (for demo purposes)
cache = {}
rate_limit_counter = {}

# Authentication decorator
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        
        # Check if token is in headers
        if 'Authorization' in request.headers:
            auth_header = request.headers['Authorization']
            try:
                token = auth_header.split(" ")[1]
            except IndexError:
                return jsonify({'message': 'Token is missing or invalid'}), 401
        
        if not token:
            return jsonify({'message': 'Token is missing!'}), 401
        
        try:
            # Verify token
            data = jwt.decode(token, API_SECRET_KEY, algorithms=["HS256"])
            current_user = data['user']
            
            # Check rate limit
            user_key = str(current_user)
            current_hour = datetime.datetime.now().strftime('%Y-%m-%d-%H')
            rate_key = f"{user_key}:{current_hour}"
            
            if rate_key in rate_limit_counter:
                rate_limit_counter[rate_key] += 1
                if rate_limit_counter[rate_key] > RATE_LIMIT:
                    return jsonify({'message': 'Rate limit exceeded'}), 429
            else:
                rate_limit_counter[rate_key] = 1
                
        except Exception as e:
            logger.error(f"Token validation error: {str(e)}")
            return jsonify({'message': 'Token is invalid!'}), 401
        
        return f(current_user, *args, **kwargs)
    
    return decorated

# Helper function to get mock data
def get_data_from_db(query):
    """Generate mock data based on the query"""
    logger.info(f"Executing query: {query}")
    return get_mock_data(query)

# Helper function to generate mock data for demo
def get_mock_data(query):
    """Generate mock data based on the query for demo purposes"""
    if "price_comparison" in query.lower():
        # Mock price comparison data
        return pd.DataFrame({
            'category': ['Vegetables', 'Fruits', 'Dairy', 'Meat', 'Bakery'],
            'retailer': ['aeon', 'lotte', 'winmart', 'mm_mega', 'lotus'],
            'avg_price': [45000, 65000, 75000, 120000, 35000],
            'min_price': [15000, 25000, 35000, 85000, 15000],
            'max_price': [80000, 120000, 150000, 250000, 60000],
            'price_competitiveness': ['Competitive', 'Premium', 'Standard', 'Premium', 'Very Competitive']
        })
    
    elif "category_analysis" in query.lower():
        # Mock category analysis data
        return pd.DataFrame({
            'category': ['Vegetables', 'Fruits', 'Dairy', 'Meat', 'Bakery'],
            'count': [150, 120, 80, 95, 110],
            'avg_price': [45000, 65000, 75000, 120000, 35000],
            'price_trend': ['decreasing', 'stable', 'increasing', 'stable', 'decreasing'],
            'price_sensitivity': ['high', 'medium', 'low', 'low', 'high']
        })
    
    elif "retailer_performance" in query.lower():
        # Mock retailer performance data
        return pd.DataFrame({
            'retailer': ['aeon', 'lotte', 'winmart', 'mm_mega', 'lotus'],
            'category_count': [25, 30, 22, 18, 27],
            'avg_price_index': [105, 115, 98, 120, 95],
            'market_position': ['mid-market', 'premium', 'value', 'premium', 'value'],
            'price_competitiveness_score': [85, 75, 90, 70, 92]
        })
    
    elif "price_sensitivity" in query.lower():
        # Mock price sensitivity data
        return pd.DataFrame({
            'category': ['Vegetables', 'Fruits', 'Dairy', 'Meat', 'Bakery'],
            'price_sensitivity_index': [0.85, 0.65, 0.45, 0.35, 0.75],
            'price_elasticity': [-1.2, -0.8, -0.5, -0.3, -1.0],
            'sensitivity_level': ['high', 'medium', 'low', 'low', 'high']
        })
    
    # Default mock data
    return pd.DataFrame({
        'category': ['Uncategorized'],
        'count': [0],
        'message': ['No data available']
    })

# Health check endpoint
@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.datetime.now().isoformat(),
        'version': '1.0.0'
    })

# Documentation endpoint
@app.route('/api/docs', methods=['GET'])
def docs():
    """API documentation"""
    return jsonify({
        'api_name': 'LOTUS-PRISM API',
        'version': '1.0.0',
        'description': 'API for retail price intelligence and competitor analysis',
        'base_url': request.url_root + 'api/',
        'endpoints': [
            {
                'path': '/health',
                'method': 'GET',
                'description': 'Health check endpoint',
                'auth_required': False
            },
            {
                'path': '/auth/token',
                'method': 'POST',
                'description': 'Get authentication token',
                'auth_required': False,
                'body': {
                    'username': 'string',
                    'password': 'string'
                }
            },
            {
                'path': '/price-comparison',
                'method': 'GET',
                'description': 'Get price comparison data across retailers',
                'auth_required': True,
                'parameters': {
                    'category': 'string (optional)',
                    'retailer': 'string (optional)',
                    'date_from': 'string (YYYY-MM-DD, optional)',
                    'date_to': 'string (YYYY-MM-DD, optional)'
                }
            },
            {
                'path': '/category-analysis',
                'method': 'GET',
                'description': 'Get category analysis data',
                'auth_required': True,
                'parameters': {
                    'category': 'string (optional)',
                    'retailer': 'string (optional)'
                }
            },
            {
                'path': '/retailer-performance',
                'method': 'GET',
                'description': 'Get retailer performance metrics',
                'auth_required': True,
                'parameters': {
                    'retailer': 'string (optional)'
                }
            }
        ]
    })

# Authentication endpoint - generate token
@app.route('/api/auth/token', methods=['POST'])
def generate_token():
    """Generate JWT token for authentication"""
    data = request.get_json()
    
    if not data or 'username' not in data or 'password' not in data:
        return jsonify({'message': 'Missing username or password'}), 400
    
    # In a real implementation, you would validate credentials against a database
    # For demo purposes, accept any username/password
    username = data['username']
    password = data['password']
    
    # Generate token with 24 hour expiration
    token = jwt.encode({
        'user': username,
        'exp': datetime.datetime.now() + datetime.timedelta(hours=24)
    }, API_SECRET_KEY, algorithm="HS256")
    
    return jsonify({
        'token': token,
        'expires_in': 86400  # 24 hours in seconds
    })

# Price comparison endpoint
@app.route('/api/price-comparison', methods=['GET'])
@token_required
def price_comparison(current_user):
    """Get price comparison data across retailers"""
    # Get query parameters
    category = request.args.get('category')
    retailer = request.args.get('retailer')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    # Check cache first
    cache_key = f"price_comparison:{category}:{retailer}:{date_from}:{date_to}"
    if cache_key in cache:
        logger.info(f"Cache hit for {cache_key}")
        return jsonify(cache[cache_key])
    
    # Build SQL query
    query = "SELECT * FROM gold_retailer_price_comparison WHERE 1=1"
    
    if category:
        query += f" AND category = '{category}'"
    if retailer:
        query += f" AND retailer = '{retailer}'"
    if date_from:
        query += f" AND batch_date >= '{date_from}'"
    if date_to:
        query += f" AND batch_date <= '{date_to}'"
    
    # Get data
    df = get_data_from_db(query)
    
    # Convert to dictionary
    result = {
        'timestamp': datetime.datetime.now().isoformat(),
        'data': df.to_dict('records'),
        'record_count': len(df)
    }
    
    # Cache the result
    cache[cache_key] = result
    
    return jsonify(result)

# Category analysis endpoint
@app.route('/api/category-analysis', methods=['GET'])
@token_required
def category_analysis(current_user):
    """Get category analysis data"""
    # Get query parameters
    category = request.args.get('category')
    retailer = request.args.get('retailer')
    
    # Check cache first
    cache_key = f"category_analysis:{category}:{retailer}"
    if cache_key in cache:
        logger.info(f"Cache hit for {cache_key}")
        return jsonify(cache[cache_key])
    
    # Build SQL query
    query = "SELECT * FROM gold_category_price_comparison WHERE 1=1"
    
    if category:
        query += f" AND category = '{category}'"
    if retailer:
        query += f" AND retailer = '{retailer}'"
    
    # Get data
    df = get_data_from_db(query)
    
    # Convert to dictionary
    result = {
        'timestamp': datetime.datetime.now().isoformat(),
        'data': df.to_dict('records'),
        'record_count': len(df),
        'categories': df['category'].unique().tolist() if 'category' in df.columns else []
    }
    
    # Cache the result
    cache[cache_key] = result
    
    return jsonify(result)

# Retailer performance endpoint
@app.route('/api/retailer-performance', methods=['GET'])
@token_required
def retailer_performance(current_user):
    """Get retailer performance metrics"""
    # Get query parameters
    retailer = request.args.get('retailer')
    
    # Check cache first
    cache_key = f"retailer_performance:{retailer}"
    if cache_key in cache:
        logger.info(f"Cache hit for {cache_key}")
        return jsonify(cache[cache_key])
    
    # Build SQL query
    query = "SELECT * FROM gold_retailer_performance WHERE 1=1"
    
    if retailer:
        query += f" AND retailer = '{retailer}'"
    
    # Get data
    df = get_data_from_db(query)
    
    # Convert to dictionary
    result = {
        'timestamp': datetime.datetime.now().isoformat(),
        'data': df.to_dict('records'),
        'record_count': len(df),
        'retailers': df['retailer'].unique().tolist() if 'retailer' in df.columns else []
    }
    
    # Cache the result
    cache[cache_key] = result
    
    return jsonify(result)

# Price sensitivity analysis endpoint 
@app.route('/api/price-sensitivity', methods=['GET'])
@token_required
def price_sensitivity(current_user):
    """Get price sensitivity analysis by category"""
    # Get query parameters
    category = request.args.get('category')
    
    # Check cache first
    cache_key = f"price_sensitivity:{category}"
    if cache_key in cache:
        logger.info(f"Cache hit for {cache_key}")
        return jsonify(cache[cache_key])
    
    # Build SQL query - this would be derived from historical price/volume data
    query = "SELECT * FROM gold_price_sensitivity WHERE 1=1"
    
    if category:
        query += f" AND category = '{category}'"
    
    # Get data
    df = get_data_from_db(query)
    
    # Convert to dictionary
    result = {
        'timestamp': datetime.datetime.now().isoformat(),
        'data': df.to_dict('records'),
        'record_count': len(df)
    }
    
    # Cache the result
    cache[cache_key] = result
    
    return jsonify(result)

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'message': 'Resource not found'}), 404

@app.errorhandler(500)
def server_error(error):
    logger.error(f"Server error: {str(error)}")
    return jsonify({'message': 'Internal server error'}), 500

if __name__ == '__main__':
    try:
        port = int(os.environ.get('PORT', 8080))
        debug = os.environ.get('DEBUG', 'False').lower() == 'true'
        print(f"Starting API server on port {port}, debug mode: {debug}")
        app.run(host='0.0.0.0', port=port, debug=debug)
    except Exception as e:
        logger.error(f"Failed to start server: {str(e)}")
        print(f"Error: Failed to start server - {str(e)}") 