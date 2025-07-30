from flask import Flask, jsonify, request
import os
import psycopg2
import psycopg2.extras
import json
from dotenv import load_dotenv
from functools import wraps
import requests
from email_service import send_course_email

# Load environment variables from .env file
load_dotenv()

# Get API secret from environment
API_SECRET = os.getenv('API_SECRET')
if not API_SECRET:
    raise ValueError("API_SECRET environment variable must be set")

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            return jsonify({'error': 'Missing or invalid authorization header'}), 401
        
        token = auth_header.split('Bearer ')[1]
        if token != API_SECRET:
            return jsonify({'error': 'Invalid authorization token'}), 401
            
        return f(*args, **kwargs)
    return decorated

app = Flask(__name__)

# Connection pool
conn_pool = None

def get_db_connection():
    """Get a database connection from the pool or create a new one"""
    global conn_pool
    try:
        if conn_pool is None or conn_pool.closed:
            # Format the connection string with your actual password
            # Format connection string with password from environment variables
            connection_string = f"postgresql://{os.getenv('SUPABASE_USER')}:{os.getenv('SUPABASE_PASSWORD')}@{os.getenv('SUPABASE_POOLER_HOST', 'aws-0-us-west-1.pooler.supabase.com')}:{os.getenv('SUPABASE_POOLER_PORT', '6543')}/postgres"
            conn_pool = psycopg2.connect(connection_string)
            conn_pool.autocommit = True
        return conn_pool
    except Exception as e:
        app.logger.error(f"Database connection error: {e}")
        raise

def db_error_handler(f):
    """Decorator to handle database errors"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except psycopg2.Error as e:
            app.logger.error(f"Database error: {e}")
            return jsonify({'error': 'Database error', 'details': str(e)}), 500
        except Exception as e:
            app.logger.error(f"Unexpected error: {e}")
            return jsonify({'error': 'Server error', 'details': str(e)}), 500
    return decorated_function

# Load courses from the simplified JSON file
try:
    with open('simplified_courses.json', 'r') as f:
        COURSES = json.load(f)['courses']
except Exception as e:
    app.logger.error(f"Error loading courses: {e}")
    COURSES = []

@app.route("/courses", methods=["GET"])
@require_auth
def get_courses():
    return jsonify(COURSES)

# Add a root route to verify the app is running
@app.route("/", methods=["GET"])
def index():
    return jsonify({"message": "Welcome to the Golf Course API", "status": "operational"})

@app.route('/cache/<key>', methods=['GET'])
@require_auth
@db_error_handler
def get_cached_course_poi(key):
    """Get a cached course POI by key"""
    conn = get_db_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT value FROM course_poi_cache WHERE key = %s", (key,))
        row = cur.fetchone()
        if row:
            # Parse the JSON if it's stored as a string
            try:
                if isinstance(row['value'], str):
                    value = json.loads(row['value'])
                else:
                    value = row['value']
                return jsonify({'key': key, 'value': value})
            except json.JSONDecodeError:
                # Return as is if not valid JSON
                return jsonify({'key': key, 'value': row['value']})
        else:
            return jsonify({'error': 'Not found'}), 404

@app.route('/cache', methods=['POST'])
@require_auth
@db_error_handler
def store_cached_course_poi():
    """Store a cached course POI"""
    data = request.get_json()
    if not data or 'key' not in data or 'value' not in data:
        return jsonify({'error': 'Missing key or value'}), 400
    
    key = data['key']
    value = data['value']
    
    # Convert to JSON string if it's a dict
    if isinstance(value, (dict, list)):
        value_json = json.dumps(value)
    else:
        value_json = value
    
    conn = get_db_connection()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO course_poi_cache (key, value) 
            VALUES (%s, %s)
            ON CONFLICT (key) 
            DO UPDATE SET value = %s, inserted_at = NOW()
            """, 
            (key, value_json, value_json)
        )
    
    return jsonify({'success': True, 'key': key}), 201


@app.route('/coordinates/<course_id>', methods=['GET'])
@require_auth
@db_error_handler
def get_course_coordinates(course_id):
    """
    Get coordinates for a golf course by ID.
    Checks database cache first, then fetches from Golf API if not found.
    """
    # Check if we have this course ID cached in the database
    cache_key = f"coordinates_{course_id}"
    
    conn = get_db_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT value FROM course_poi_cache WHERE key = %s", (cache_key,))
        row = cur.fetchone()
        
        if row:
            # We found cached data, return it
            app.logger.info(f"Returning cached data for {course_id}")
            try:
                if isinstance(row['value'], str):
                    coordinates_data = json.loads(row['value'])
                else:
                    coordinates_data = row['value']
                processed_data = extract_green_centers(coordinates_data) 
                return jsonify(processed_data)  
            except json.JSONDecodeError:
                # Return as is if not valid JSON
                raise Exception("Invalid JSON data in cache")
        
        # No cached data found, fetch from Golf API
        app.logger.info(f"No cached data found, fetching from Golf API for course {course_id}")
        golf_api_url = f"https://www.golfapi.io/api/v2.3/coordinates/{course_id}"
        golf_api_token = os.getenv('GOLF_API_TOKEN')

        if not golf_api_token:
            return jsonify({'error': 'Golf API token not configured'}), 500
        
        try:
            headers = {"Authorization": f"Bearer {golf_api_token}"}
            response = requests.get(golf_api_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                coordinates_data = response.json()
                
                # Extract API requests left
                api_requests_left = coordinates_data.get('apiRequestsLeft')
                
                # Store in cache for future requests
                value_json = json.dumps(coordinates_data)
                cur.execute(
                    """
                    INSERT INTO course_poi_cache (key, value) 
                    VALUES (%s, %s)
                    ON CONFLICT (key) 
                    DO UPDATE SET value = %s
                    """, 
                    (cache_key, value_json, value_json)
                )
            
                # Get course name for the email
                course_name = "Unknown Course"
                for course in COURSES:
                    if course.get('courseId') == course_id:
                        course_name = course.get('courseName', "Unknown Course")
                        break
                
                # Send email with API requests left information
                app.logger.info(f"Sending email for new course POI data added for {course_name} (ID: {course_id})")
                send_course_email(course_id, course_name, coordinates_data, "POI", api_requests_left)
                
                processed_data = extract_green_centers(coordinates_data) 
                return jsonify(processed_data)  
            else:
                return jsonify({
                    'error': 'Golf API request failed', 
                    'status_code': response.status_code,
                    'details': response.text
                }), response.status_code
                
        except requests.exceptions.RequestException as e:
            return jsonify({'error': 'Failed to fetch from Golf API', 'details': str(e)}), 500

@app.route('/info/<course_id>', methods=['GET'])
@require_auth
@db_error_handler
def get_course_info(course_id):
    """
    Get course info for a golf course by ID.
    Checks database cache first, then fetches from Golf API if not found.
    """
    # Check if we have this course ID cached in the database
    cache_key = f"info_{course_id}"
    
    conn = get_db_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT value FROM course_info_cache WHERE key = %s", (cache_key,))
        row = cur.fetchone()
        
        if row:
            # We found cached data, return it
            app.logger.info(f"Returning cached course info data for {course_id}")
            try:
                if isinstance(row['value'], str):
                    info_data = json.loads(row['value'])
                else:
                    info_data = row['value']
                processed_data = extract_pars(info_data)
                return jsonify(processed_data)  
            except json.JSONDecodeError:
                # Return as is if not valid JSON
                raise Exception("Invalid JSON data in cache")
        
        # No cached data found, fetch from Golf API
        app.logger.info(f"No cached course info data found, fetching from Golf API for course {course_id}")
        golf_api_url = f"https://www.golfapi.io/api/v2.3/courses/{course_id}"
        golf_api_token = os.getenv('GOLF_API_TOKEN')
        
        try:
            headers = {"Authorization": f"Bearer {golf_api_token}"}
            response = requests.get(golf_api_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                info_data = response.json()
                
                # Extract API requests left
                api_requests_left = info_data.get('apiRequestsLeft')
                
                # Store in cache for future requests
                value_json = json.dumps(info_data)
                cur.execute(
                    """
                    INSERT INTO course_info_cache (key, value) 
                    VALUES (%s, %s)
                    ON CONFLICT (key) 
                    DO UPDATE SET value = %s
                    """, 
                    (cache_key, value_json, value_json)
                )
            
                # Get course name for the email
                course_name = "Unknown Course"
                for course in COURSES:
                    if course.get('courseId') == course_id:
                        course_name = course.get('courseName', "Unknown Course")
                        break
                
                # Send email with API requests left information
                app.logger.info(f"Sending email for new course info data added for {course_name} (ID: {course_id})")
                send_course_email(course_id, course_name, info_data, "Info", api_requests_left)
                
                processed_data = extract_pars(info_data)
                return jsonify(processed_data)  
            else:
                return jsonify({
                    'error': 'Golf API request failed', 
                    'status_code': response.status_code,
                    'details': response.text
                }), response.status_code
                
        except requests.exceptions.RequestException as e:
            return jsonify({'error': 'Failed to fetch from Golf API', 'details': str(e)}), 500

def extract_green_centers(coordinates_data):
    """
    Extract hole numbers and green coordinates (center, front, back) from course data.
    
    Args:
        coordinates_data (dict): The full course coordinates data from Golf API
        
    Returns:
        dict: A simplified JSON with hole numbers and green coordinates
    """
    holes_data = {}
    
    # Check if the required data structure exists
    if not coordinates_data or 'coordinates' not in coordinates_data:
        return {'error': 'Invalid course data format', 'holes': []}
    
    # Process each coordinate entry
    for coord in coordinates_data.get('coordinates', []):
        # Filter for green (poi=1) and different locations (1=front, 2=center, 3=back)
        if coord.get('poi') == 1 and coord.get('location') in [1, 2, 3]:
            hole_number = coord.get('hole')
            location = coord.get('location')
            
            # Create hole entry if it doesn't exist
            if hole_number not in holes_data:
                holes_data[hole_number] = {
                    'holeNumber': hole_number
                }
            
            # Add coordinates based on location
            if location == 1:  # Front of green
                holes_data[hole_number]['frontOfGreen'] = {
                    'latitude': coord.get('latitude'),
                    'longitude': coord.get('longitude')
                }
            elif location == 2:  # Center of green
                holes_data[hole_number]['centerOfGreen'] = {
                    'latitude': coord.get('latitude'),
                    'longitude': coord.get('longitude')
                }
            elif location == 3:  # Back of green
                holes_data[hole_number]['backOfGreen'] = {
                    'latitude': coord.get('latitude'),
                    'longitude': coord.get('longitude')
                }
    
    # Convert dictionary to sorted list
    holes_list = [holes_data[hole] for hole in sorted(holes_data.keys())]
    
    return {
        'courseID': coordinates_data.get('courseID', ''),
        'holes': holes_list,
        'count': len(holes_list)
    }

def extract_pars(info_data):
    """
    Extract par information for men and women from course info data.
    
    Args:
        info_data (dict): The full course info data from Golf API
        
    Returns:
        dict: A simplified JSON with course ID and par information
    """
    # Check if the required data structure exists
    if not info_data:
        return {'error': 'Invalid course data format'}
    
    # Extract par data
    pars_men = info_data.get('parsMen', [])
    pars_women = info_data.get('parsWomen', [])
    
    return {
        'courseID': info_data.get('courseID', ''),
        'parsMen': pars_men,
        'parsWomen': pars_women
    }

@app.teardown_appcontext
def close_db_connection(exception):
    """Close database connection when the application context ends"""
    global conn_pool
    if conn_pool is not None and not conn_pool.closed:
        conn_pool.close()
        conn_pool = None

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv('PORT', '5000')))
