import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import os
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_course_email(course_id, course_name, course_data, data_type, api_requests_left=None):
    """
    Email course data as a JSON file attachment.
    
    Args:
        course_id (str): The ID of the course
        course_name (str): The name of the course for the email subject
        course_data (dict): The course data to send (POI or info)
        data_type (str): Type of data ('POI' or 'Info') for subject and filename
        api_requests_left (str, optional): The number of API requests left
    
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    # Get email configuration from environment variables
    gmail_user = os.getenv('GMAIL_USER')
    gmail_password = os.getenv('GMAIL_APP_PASSWORD')
    
    # Clean the password to remove any problematic spaces
    if gmail_password:
        # Remove all whitespace from password
        gmail_password = ''.join(gmail_password.split())
    
    recipient_email = os.getenv('RECIPIENT_EMAIL')
    
    # Validate email configuration
    if not gmail_user or not gmail_password or not recipient_email:
        logger.error("Email configuration missing. Set GMAIL_USER, GMAIL_APP_PASSWORD, and RECIPIENT_EMAIL")
        return False
    
    try:
        # Create the email
        msg = MIMEMultipart()
        msg['From'] = gmail_user
        msg['To'] = recipient_email
        
        # Use ASCII-only subject line
        subject = f"Golf Course {data_type}: {course_name}"
        msg['Subject'] = subject.encode('ascii', 'ignore').decode('ascii')
        
        # Email body
        body = f"Attached is the {data_type.lower()} data for {course_name} (ID: {course_id})."
        
        # Add API requests left information if available
        if api_requests_left:
            body += f"\n\nYou have {api_requests_left} API requests remaining."
        
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        
        # Convert course_data to a formatted JSON string
        json_data = json.dumps(course_data, indent=2)
        
        # Create JSON file attachment
        attachment = MIMEApplication(json_data.encode('utf-8'), _subtype='json')
        
        # Clean filename
        filename = f"{course_id}_{course_name.replace(' ', '_')}"
        filename = filename.encode('ascii', 'ignore').decode('ascii')  # Ensure ASCII-only
        
        attachment.add_header('Content-Disposition', 'attachment', 
                             filename=f"{filename}_{data_type.lower()}.json")
        msg.attach(attachment)
        
        # Connect to Gmail SMTP server
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(gmail_user, gmail_password)
            server.send_message(msg)
        
        logger.info(f"Email sent successfully for {course_name} (ID: {course_id}) - {data_type}")
        if api_requests_left:
            logger.info(f"API requests remaining: {api_requests_left}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to send email: {e}")
        return False 