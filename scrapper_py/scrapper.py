import time
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup
from datetime import datetime
import os
import sys
import re
import random
from urllib.parse import urljoin, urlparse, parse_qs

# Custom exception for handling errors
class ScraperError(Exception):
    """Exception raised when the scraper encounters an error."""
    pass

#############################################
# CONFIGURATION VARIABLES - EDIT AS NEEDED
#############################################

# Base URL
BASE_URL = "https://firmenregister.de"

# Location configuration - German states with proper URL encoding
STATES = [
    "Baden-W%FCrttemberg",  # Baden-Württemberg properly encoded
    "Bayern",
    "Berlin",
    "Brandenburg",
    "Bremen",
    "Hamburg",
    "Hessen",
    "Mecklenburg-Vorpommern",
    "Niedersachsen",
    "Nordrhein-Westfalen",
    "Rheinland-Pfalz",
    "Saarland",
    "Sachsen",
    "Sachsen-Anhalt",
    "Schleswig-Holstein",
    "Th%FCringen"  # Thüringen properly encoded
]

# Display names for the states (for output files and display)
STATE_DISPLAY_NAMES = {
    "Baden-W%FCrttemberg": "baden-württemberg",
    "Bayern": "bayern",
    "Berlin": "berlin",
    "Brandenburg": "brandenburg",
    "Bremen": "bremen",
    "Hamburg": "hamburg",
    "Hessen": "hessen",
    "Mecklenburg-Vorpommern": "mecklenburg-vorpommern",
    "Niedersachsen": "niedersachsen",
    "Nordrhein-Westfalen": "nordrhein-westfalen", 
    "Rheinland-Pfalz": "rheinland-pfalz",
    "Saarland": "saarland",
    "Sachsen": "sachsen",
    "Sachsen-Anhalt": "sachsen-anhalt",
    "Schleswig-Holstein": "schleswig-holstein",
    "Th%FCringen": "thüringen"
}

# Starting point configuration (can be adjusted to resume from a particular state)
START_STATE_INDEX = 0  # 0 is Baden-Württemberg (first in STATES list)

# Output configuration
ONE_FILE_PER_STATE = True  # True to create one file per state, False for one big file

# Delay settings (seconds) - slightly reduced to be faster
MIN_PAGE_DELAY = 0.8
MAX_PAGE_DELAY = 2.5
MIN_COMPANY_DELAY = 0.3
MAX_COMPANY_DELAY = 1.0

# Files
PROGRESS_FILE = 'scraping_progress.json'
PROGRESS_BACKUP_FILE = 'scraping_progress.backup.json'
PROCESSED_COMPANIES_FILE = 'processed_companies.json'
BLOCKED_PAGES_DIR = 'blocked_pages'  # Directory to save blocked page responses

# Debug mode - prints more information
DEBUG = True

# User agent list to rotate and avoid blocking
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0',
]

#############################################
# SELECTORS - HTML parsing patterns - UPDATED TO FIX DEPRECATION WARNING
#############################################

SELECTORS = {
    # Company rows on list page
    'company_rows': 'tr[valign="top"][bgcolor="#FFE8A9"]',
    
    # Inside a company row
    'company_link': 'td a[href^="register.php?cmd=anzeige"]',
    'company_email': 'a[href^="mailto:"]',
    'company_website': 'a[href^="click.php"]',
    'company_products': 'img[src="pic/prod.gif"]',
    
    # Pagination
    'pagination': 'tr[bgcolor="#FFCC33"] a',
    'pages_info': 'tr td.blue',
    
    # Company details page - updated to use :-soup-contains instead of :contains
    'company_details': 'tbody',  # Main table body containing company details
    'company_name': 'td:-soup-contains("Firmenname") + td',
    'company_street': 'td:-soup-contains("Adresse") + td',
    'company_zipcode': 'td:-soup-contains("PLZ / Ort") + td',
    'company_phone': 'td:-soup-contains("Telefon") + td',
    'company_fax': 'td:-soup-contains("Fax") + td',
    'company_mobile': 'td:-soup-contains("Mobil") + td',
    'company_email_detail': 'td:-soup-contains("E-Mail") + td',
    'company_website_detail': 'td:-soup-contains("Homepage") + td',
    'company_contact': 'td:-soup-contains("Kontakt") + td',
    'company_products_info': 'td:-soup-contains("Produkte / Infos") + td',
    'company_industry': 'td:-soup-contains("Branchen") + td',
}

#############################################
# HELPER FUNCTIONS
#############################################

# Function to get a random user agent
def get_headers():
    user_agent = random.choice(USER_AGENTS)
    return {
        'User-Agent': user_agent,
        'Accept-Language': 'de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml',
        'Referer': 'https://firmenregister.de/',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
    }

# Debug print function
def debug_print(message):
    if DEBUG:
        print(f"[DEBUG] {message}")

# Simplified function to save blocked page responses - only save important errors
def save_blocked_page(url, content=None, status_code=None, headers=None, error_message=None, force_save=False):
    """Save the HTML content of a blocked page for debugging - optimized to save only important errors."""
    # Create the directory if it doesn't exist
    if not os.path.exists(BLOCKED_PAGES_DIR):
        os.makedirs(BLOCKED_PAGES_DIR)
    
    # Determine if we should save this response based on criteria
    should_save = force_save or status_code == 403 or status_code == 429 or status_code == 404
    
    # For page failures, always save
    if "Failed to fetch page" in str(error_message):
        should_save = True
    
    # For successful responses, only save pagination debugging
    if status_code == 200 and not force_save:
        should_save = False
    
    if not should_save:
        # Just log but don't save the file
        if DEBUG:
            print(f"Skipping saving response for URL: {url} (status: {status_code})")
        return None
        
    # Create a filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    parsed_url = urlparse(url)
    page_num = "unknown"
    query_params = parse_qs(parsed_url.query)
    
    # Try to extract page number for better filenames
    if "ap" in query_params:
        page_num = f"page{int(query_params['ap'][0])+1}"
        
    safe_url = f"{parsed_url.path.replace('/', '_')}_{page_num}"
    
    if status_code:
        filename = f"{BLOCKED_PAGES_DIR}/blocked_{status_code}_{timestamp}_{safe_url}.html"
    else:
        filename = f"{BLOCKED_PAGES_DIR}/error_{timestamp}_{safe_url}.html"
    
    try:
        # Save the raw HTML content if available
        if content:
            # Always use binary mode for content
            with open(filename, 'wb') as f:
                # Ensure content is bytes
                if isinstance(content, str):
                    f.write(content.encode('utf-8'))
                else:
                    f.write(content)
                print(f"Saved content to {filename} ({len(content)} bytes)")
        else:
            # If no content, create a file with error info
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"No content received from {url}\n")
                if error_message:
                    f.write(f"Error: {error_message}\n")
                print(f"Saved error note to {filename}")
        
        # Save additional request information
        info_filename = f"{filename}.info.txt"
        with open(info_filename, 'w', encoding='utf-8') as f:
            f.write(f"URL: {url}\n")
            f.write(f"Status Code: {status_code or 'Unknown'}\n")
            f.write(f"Timestamp: {datetime.now().isoformat()}\n")
            f.write(f"Error Message: {error_message or 'None'}\n")
            f.write(f"Page Number: {page_num}\n")
            
            if headers:
                f.write(f"Request Headers:\n")
                for header, value in headers.items():
                    f.write(f"  {header}: {value}\n")
            
        return filename
    except Exception as e:
        print(f"Error saving blocked page: {str(e)}")
        return None

# More efficient fetch_page function that uses requests.Session for connection pooling
def fetch_page(url, max_retries=3, session=None):
    """Fetch a page with proper error handling and logging."""
    headers = get_headers()
    
    # Use an existing session or create a new one for connection pooling
    if session is None:
        session = requests.Session()
    
    for attempt in range(max_retries):
        try:
            print(f"Fetching: {url} (attempt {attempt+1}/{max_retries})")
            
            response = session.get(url, headers=headers, timeout=15)
            
            # Check for blocking responses
            if response.status_code == 403:
                print(f"CRITICAL: Received 403 Forbidden response from {url}")
                print("The scraper appears to be banned or rate-limited.")
                # Save the blocked page content
                save_blocked_page(url, response.content, response.status_code, headers, force_save=True)
                raise ScraperError("Received 403 Forbidden error. Progress saved for resuming in a new session.")
            
            # Check for rate limiting
            if response.status_code == 429:
                print(f"CRITICAL: Rate limited on {url}")
                save_blocked_page(url, response.content, response.status_code, headers, force_save=True)
                # Wait longer before retrying
                wait_time = 30 * (attempt + 1)
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
                continue
                
            # Also check for other non-200 responses
            if response.status_code != 200:
                print(f"WARNING: Received non-200 status code: {response.status_code} from {url}")
                save_blocked_page(url, response.content, response.status_code, headers)
                
                # For severe errors, treat as blocking
                if response.status_code >= 400:  # Client or Server errors
                    raise ScraperError(f"Received error status code: {response.status_code}. Progress saved.")
            
            # Check for CAPTCHA or other blocking indicators in content
            lower_content = response.content.lower()
            if b'captcha' in lower_content or b'blocked' in lower_content or b'rate limit' in lower_content:
                print(f"WARNING: Possible CAPTCHA or blocking detected in response content")
                save_blocked_page(url, response.content, response.status_code, headers, force_save=True)
            
            # Success path - save HTML for debugging pagination only when there's a failure
            if "ap=5" in url:
                # This is page 6 which has been failing - save for debugging
                save_blocked_page(url, response.content, response.status_code, headers, 
                              "Page 6 debug content (successful response)", force_save=True)
            
            return response.content
                
        except requests.exceptions.RequestException as e:
            print(f"Request error (attempt {attempt+1}/{max_retries}): {str(e)}")
            
            # For connection errors, retry after waiting
            wait_time = 5 * (attempt + 1)
            
            if isinstance(e, requests.exceptions.Timeout):
                error_type = "TIMEOUT"
            elif isinstance(e, requests.exceptions.ConnectionError):
                error_type = "CONNECTION_ERROR"  
            elif isinstance(e, requests.exceptions.TooManyRedirects):
                error_type = "TOO_MANY_REDIRECTS"
            else:
                error_type = "REQUEST_EXCEPTION"
                
            # Save error info for the last attempt
            if attempt == max_retries - 1:
                error_message = f"{error_type}: {str(e)}"
                # Try to save response content if available
                if hasattr(e, 'response') and e.response is not None:
                    save_blocked_page(url, e.response.content, 
                                    getattr(e.response, 'status_code', None), 
                                    headers, error_message, force_save=True)
                else:
                    # No response available, just save error info
                    save_blocked_page(url, None, None, headers, error_message, force_save=True)
            
            # Sleep before retrying
            print(f"Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Unexpected error fetching URL: {str(e)}")
            # On the last attempt, save the error
            if attempt == max_retries - 1:
                save_blocked_page(url, None, None, headers, f"UNEXPECTED: {str(e)}", force_save=True)
            
    # If we get here, all retries failed
    print(f"Failed to fetch {url} after {max_retries} attempts")
    return None

# Load progress data
def load_progress():
    """Load progress data with improved error handling and backup restoration."""
    progress_data = {
        'current_state_index': START_STATE_INDEX,
        'current_page': 0,
        'timestamp': datetime.now().isoformat(),
        'processed_companies': {}
    }
    
    # Try to load the main progress file
    try:
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, 'r') as f:
                progress_data = json.load(f)
                debug_print(f"Loaded progress data from {PROGRESS_FILE}")
                return progress_data
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        print(f"Error loading progress file: {str(e)}")
        # If main file failed, try to load from backup
        if os.path.exists(PROGRESS_BACKUP_FILE):
            try:
                with open(PROGRESS_BACKUP_FILE, 'r') as f:
                    progress_data = json.load(f)
                    print(f"Restored progress from backup file {PROGRESS_BACKUP_FILE}")
                    return progress_data
            except Exception as e2:
                print(f"Error loading backup progress file: {str(e2)}")
    
    return progress_data

# Save progress data
def save_progress(progress_data, atomic=True):
    """Save progress data with atomic write pattern to prevent corruption."""
    # Update timestamp
    progress_data['timestamp'] = datetime.now().isoformat()
    
    if atomic:
        # Atomic write pattern: write to temporary file, then rename
        temp_file = f"{PROGRESS_FILE}.temp"
        try:
            # Write to temp file first
            with open(temp_file, 'w') as f:
                json.dump(progress_data, f, indent=2)
            
            # Make a backup of the current file if it exists
            if os.path.exists(PROGRESS_FILE):
                if os.path.exists(PROGRESS_BACKUP_FILE):
                    os.remove(PROGRESS_BACKUP_FILE)
                os.rename(PROGRESS_FILE, PROGRESS_BACKUP_FILE)
            
            # Rename temp file to the actual progress file
            os.rename(temp_file, PROGRESS_FILE)
            debug_print(f"Progress saved successfully to {PROGRESS_FILE}")
            
        except Exception as e:
            print(f"Error during atomic progress save: {str(e)}")
            # Fall back to direct save if atomic save fails
            try:
                with open(PROGRESS_FILE, 'w') as f:
                    json.dump(progress_data, f, indent=2)
            except Exception as e2:
                print(f"Critical error: Could not save progress: {str(e2)}")
    else:
        # Direct save without atomic pattern
        try:
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            print(f"Error saving progress: {str(e)}")

# Load processed companies
def load_processed_companies():
    """Load the set of already processed company IDs."""
    processed = set()
    
    if os.path.exists(PROCESSED_COMPANIES_FILE):
        try:
            with open(PROCESSED_COMPANIES_FILE, 'r') as f:
                company_data = json.load(f)
                processed = set(company_data.get('ids', []))
                debug_print(f"Loaded {len(processed)} processed companies from cache file")
        except Exception as e:
            print(f"Error loading company cache: {str(e)}")
    
    # Also check the main progress file for company IDs
    progress = load_progress()
    if 'processed_companies' in progress:
        if isinstance(progress['processed_companies'], dict):
            # New format where we store timestamps
            for company_id in progress['processed_companies']:
                processed.add(company_id)
        elif isinstance(progress['processed_companies'], list):
            # Old format was just a list
            processed.update(set(progress['processed_companies']))
    
    return processed

# Save processed companies
def save_processed_companies(processed_companies, progress_data=None):
    """Save the set of processed company IDs with timestamps."""
    try:
        # Ensure we have a JSON-serializable format
        if isinstance(processed_companies, set):
            # Convert set to dict with timestamps
            now = datetime.now().isoformat()
            companies_dict = {company_id: now for company_id in processed_companies}
        elif isinstance(processed_companies, list):
            # Convert list to dict with timestamps
            now = datetime.now().isoformat()
            companies_dict = {company_id: now for company_id in processed_companies}
        else:
            # Assume it's already a dict
            companies_dict = processed_companies
        
        # Update the main progress data
        if progress_data is not None:
            progress_data['processed_companies'] = companies_dict
            save_progress(progress_data)
        
        # Also save to dedicated cache file for faster loading
        with open(PROCESSED_COMPANIES_FILE, 'w') as f:
            json.dump({'ids': list(companies_dict.keys()), 
                      'timestamp': datetime.now().isoformat()}, 
                     f, indent=2)
    except Exception as e:
        print(f"Error saving company cache: {str(e)}")

# Function to perform a clean shutdown, saving all progress
def save_and_exit(progress_data, processed_companies, exit_code=0, message="Scraper stopped"):
    """Save all progress and exit cleanly."""
    print(f"\n{message}")
    
    try:
        # Convert processed_companies to a serializable format if it's a set
        if isinstance(processed_companies, set):
            now = datetime.now().isoformat()
            processed_companies_dict = {company_id: now for company_id in processed_companies}
            processed_companies = processed_companies_dict
        
        # Update processed companies in the progress data
        progress_data['processed_companies'] = processed_companies
        
        # Save progress one last time
        save_progress(progress_data)
        
        # Save processed companies separately
        save_processed_companies(processed_companies)
    except Exception as e:
        print(f"Error during shutdown: {e}")
        try:
            # Try a simpler save with minimal data
            minimal_progress = {
                'timestamp': datetime.now().isoformat(),
                'error_during_shutdown': str(e),
                'current_state_index': progress_data.get('current_state_index'),
                'current_page': progress_data.get('current_page')
            }
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(minimal_progress, f)
        except:
            print("Critical failure: Could not save even minimal progress")
    
    print(f"Progress saved. Exiting with code {exit_code}.")
    sys.exit(exit_code)

# Extract company ID from URL
def extract_company_id(url):
    """Extract the company ID from a company detail URL."""
    if not url:
        return None
    
    # Try to extract eid parameter from URL
    match = re.search(r'eid=(\d+)', url)
    if match:
        return match.group(1)
    return None

# Parse company details from the details page
def parse_company_details(soup, company_id, state):
    """Extract company details from the company details page."""
    company_data = {
        'company_id': company_id,
        'state': state,
        'name': '',
        'street': '',
        'zipcode': '',
        'city': '',
        'phone': '',
        'fax': '',
        'mobile': '',
        'email': '',
        'website': '',
        'contact_person': '',
        'products_info': '',
        'industry': '',
        'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    try:
        # Find the company details section (tbody containing all company info)
        details_section = soup.select_one(SELECTORS['company_details'])
        
        if not details_section:
            debug_print(f"Company details section not found for company ID {company_id}")
            return None
        
        # Extract company name
        name_field = details_section.select_one(SELECTORS['company_name'])
        if name_field:
            h2_tag = name_field.select_one('h2')
            if h2_tag:
                company_data['name'] = h2_tag.text.strip()
            else:
                company_data['name'] = name_field.text.strip()
        
        # Extract address details
        street_field = details_section.select_one(SELECTORS['company_street'])
        if street_field:
            a_tag = street_field.select_one('a')
            if a_tag:
                company_data['street'] = a_tag.text.strip()
            else:
                company_data['street'] = street_field.text.strip()
        
        # Extract zipcode and city
        zipcode_field = details_section.select_one(SELECTORS['company_zipcode'])
        if zipcode_field:
            # The HTML structure has two links: one for zipcode and one for city
            plz_link = zipcode_field.select_one('a')
            city_link = zipcode_field.select_one('a:nth-of-type(2)')
            
            if plz_link:
                company_data['zipcode'] = plz_link.text.strip()
            
            if city_link:
                company_data['city'] = city_link.text.strip()
            
            # If structured extraction fails, try basic regex
            if not company_data['zipcode'] or not company_data['city']:
                zipcode_text = zipcode_field.text.strip()
                match = re.match(r'(\d{5})\s+(.*)', zipcode_text)
                if match:
                    company_data['zipcode'] = match.group(1)
                    company_data['city'] = match.group(2)
        
        # Extract phone
        phone_field = details_section.select_one(SELECTORS['company_phone'])
        if phone_field:
            company_data['phone'] = phone_field.text.strip()
        
        # Extract fax
        fax_field = details_section.select_one(SELECTORS['company_fax'])
        if fax_field:
            company_data['fax'] = fax_field.text.strip()
        
        # Extract mobile
        mobile_field = details_section.select_one(SELECTORS['company_mobile'])
        if mobile_field:
            company_data['mobile'] = mobile_field.text.strip()
        
        # Extract email
        email_field = details_section.select_one(SELECTORS['company_email_detail'])
        if email_field:
            email_link = email_field.select_one('a')
            if email_link:
                company_data['email'] = email_link.text.strip()
            else:
                company_data['email'] = email_field.text.strip()
        
        # Extract website
        website_field = details_section.select_one(SELECTORS['company_website_detail'])
        if website_field:
            website_link = website_field.select_one('a')
            if website_link:
                company_data['website'] = website_link.text.strip()
            else:
                company_data['website'] = website_field.text.strip()
        
        # Extract contact person
        contact_field = details_section.select_one(SELECTORS['company_contact'])
        if contact_field:
            company_data['contact_person'] = contact_field.text.strip()
        
        # Extract products/info
        products_field = details_section.select_one(SELECTORS['company_products_info'])
        if products_field:
            company_data['products_info'] = products_field.text.strip()
        
        # Extract industry
        industry_field = details_section.select_one(SELECTORS['company_industry'])
        if industry_field:
            h2_tag = industry_field.select_one('h2')
            if h2_tag:
                # Replace <br> with newlines for better formatting
                for br in h2_tag.find_all('br'):
                    br.replace_with('\n')
                company_data['industry'] = h2_tag.text.strip()
            else:
                company_data['industry'] = industry_field.text.strip()
        
        return company_data
    
    except Exception as e:
        print(f"Error parsing company details for {company_id}: {str(e)}")
        return None

#############################################
# SCRAPING FUNCTIONS
#############################################

def get_companies_from_page(html_content, state):
    """Extract company links and basic info from a search results page."""
    soup = BeautifulSoup(html_content, 'html.parser')
    companies = []
    
    # Find all company rows
    company_rows = soup.select(SELECTORS['company_rows'])
    
    for row in company_rows:
        # Extract company link which contains the ID
        company_link = row.select_one(SELECTORS['company_link'])
        
        if not company_link:
            continue
            
        company_url = company_link.get('href', '')
        company_name = company_link.text.strip()
        company_id = extract_company_id(company_url)
        
        if not company_id:
            continue
            
        # Extract email if available in the list view
        email = ''
        email_element = row.select_one(SELECTORS['company_email'])
        if email_element:
            email_address = email_element.get('onmouseover')
            if email_address:
                # Extract email from onmouseover attribute
                email_match = re.search(r"return escape\('([^']+)'\)", email_address)
                if email_match:
                    email = email_match.group(1)
        
        # Extract website if available in the list view
        website = ''
        website_element = row.select_one(SELECTORS['company_website'])
        if website_element:
            website_url = website_element.get('onmouseover')
            if website_url:
                # Extract URL from onmouseover attribute
                website_match = re.search(r"return escape\('([^']+)'\)", website_url)
                if website_match:
                    website = website_match.group(1)
        
        # Extract product/industry description if available
        products = ''
        products_element = row.select_one(SELECTORS['company_products'])
        if products_element:
            products_desc = products_element.get('onmouseover')
            if products_desc:
                # Extract description from onmouseover attribute
                products_match = re.search(r"return escape\('([^']+)'\)", products_desc)
                if products_match:
                    products = products_match.group(1)
        
        # Store basic company info
        companies.append({
            'id': company_id,
            'name': company_name,
            'url': company_url,
            'email': email,
            'website': website,
            'products': products,
            'state': state
        })
    
    return companies

# Completely rewritten pagination detection function that focuses on solving page 6 issue
def get_pagination_info(html_content, page_num, url):
    """Extract pagination information with improved detection of next page links."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Always save page 5 HTML for debugging
    if page_num == 5:
        debug_file = f"{BLOCKED_PAGES_DIR}/page6_pagination_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(debug_file, 'wb') as f:
            f.write(html_content)
        print(f"Saved page 6 HTML for pagination debugging to {debug_file}")
    
    # Check for total entries information
    total_entries = 0
    entries_info = soup.select_one(SELECTORS['pages_info'])
    if entries_info:
        entries_match = re.search(r'(\d+)\s+Einträge gefunden', entries_info.text)
        if entries_match:
            total_entries = int(entries_match.group(1))
            print(f"Found {total_entries} total entries")
    
    # Get next page link - FIXED APPROACH checking all pagination links
    next_page_url = None
    current_page = page_num
    
    # Extract pagination links but with more debugging
    pagination_links = soup.select(SELECTORS['pagination'])
    
    if not pagination_links and page_num > 0:
        print(f"WARNING: No pagination links found on page {page_num+1}. Saving HTML for inspection.")
        save_blocked_page(url, html_content, 200, None, f"No pagination links on page {page_num+1}", force_save=True)
        
    # Get all page numbers from pagination
    page_numbers = []
    for link in pagination_links:
        try:
            # Extract page number from the URL parameter 'ap'
            href = link.get('href', '')
            ap_match = re.search(r'ap=(\d+)', href)
            if ap_match:
                page_numbers.append(int(ap_match.group(1)))
            # Also check the link text which might contain the page number
            if link.text.strip():
                text_match = re.search(r'\[?(\d+)\]?', link.text.strip())
                if text_match:
                    page_num_from_text = int(text_match.group(1))
                    # Adjust for 0-indexing in URL vs 1-indexing in display
                    if page_num_from_text > 1:  # Displayed as 2+
                        page_numbers.append(page_num_from_text - 1)  # Convert to 0-indexed for the URL
        except ValueError:
            continue
    
    # Find the next page number relative to current page
    if page_numbers:
        # Sort page numbers and find the next one after current page
        page_numbers = sorted(set(page_numbers))
        print(f"Found page numbers in pagination: {page_numbers}")
        
        # Find the next page number after current_page
        for p in page_numbers:
            if p > current_page:  # We found a higher page number
                next_page = p
                # Construct the next page URL
                next_page_url = re.sub(r'ap=\d+', f'ap={next_page}', url)
                print(f"Found next page: {next_page+1}, URL: {next_page_url}")
                break
    
    # If we couldn't find the next page but know there should be more pages
    if not next_page_url and current_page < total_entries / 10:
        # Try constructing the next page URL directly
        next_page = current_page + 1
        next_page_url = re.sub(r'ap=\d+', f'ap={next_page}', url)
        print(f"Constructed next page URL: {next_page_url}")
        
    return {
        'total_entries': total_entries,
        'current_page': current_page,
        'next_page_url': next_page_url
    }

def scrape_company_details(company_id, state, processed_companies):
    """Fetch and parse details for a single company."""
    if company_id in processed_companies:
        debug_print(f"Company {company_id} already processed, skipping")
        return None
        
    # Construct URL to company details page
    detail_url = f"{BASE_URL}/register.php?cmd=anzeige&eid={company_id}"
    
    try:
        # Fetch company details page
        content = fetch_page(detail_url)
        if not content:
            return None
            
        # Parse company details
        soup = BeautifulSoup(content, 'html.parser')
        company_data = parse_company_details(soup, company_id, state)
        
        # Mark as processed
        processed_companies.add(company_id)
        
        return company_data
    
    except Exception as e:
        print(f"Error scraping company {company_id}: {str(e)}")
        return None

# Function to get the state output filename
def get_state_filename(state):
    """Get the output filename for a state, using the display name mapping."""
    if state in STATE_DISPLAY_NAMES:
        return f"{STATE_DISPLAY_NAMES[state]}.csv"
    else:
        # Clean up the URL encoded state name as fallback
        clean_state = state.replace('%FC', 'ü').replace('%C3%BC', 'ü')
        return f"{clean_state.replace(' ', '_').lower()}.csv"

# Get state-specific fr_param based on the state code
def get_fr_param_for_state(state):
    """Get the correct fr_param for each state to fix pagination issues."""
    # Default value that seems to work for Baden-Württemberg
    if state == "Baden-W%FCrttemberg":
        return "Ojo6Ojo6Ojo6Ojo6Ojo6OkJhZGVuLVf8cnR0ZW1iZXJnOjo6Ojo6Ojo%3D"
    elif state == "Bayern":
        return "Ojo6Ojo6Ojo6Ojo6Ojo6OkJheWVybjo6Ojo6Ojo6"
    elif state == "Berlin":
        return "Ojo6Ojo6Ojo6Ojo6Ojo6OkJlcmxpbjo6Ojo6Ojo6"
    # Add more state mappings if needed
    
    # For other states, try to generate a parameter or use a generic one
    # This is a simplification - you may need to extract this from the first page
    return "Ojo6Ojo6Ojo6Ojo6Ojo6OjoOjo6Ojo6Ojo6" 

def scrape_state(state, start_page=0):
    """Scrape all companies for a given state."""
    print(f"\n{'='*50}")
    state_display = STATE_DISPLAY_NAMES.get(state, state)
    print(f"Starting scraper for state: {state_display}")
    print(f"{'='*50}")
    
    # Load progress and processed companies
    progress = load_progress()
    processed_companies = load_processed_companies()
    
    # Prepare output filename
    state_filename = get_state_filename(state)
    
    # Load existing data if available
    companies_data = []
    if os.path.exists(state_filename):
        try:
            existing_df = pd.read_csv(state_filename)
            companies_data = existing_df.to_dict('records')
            print(f"Loaded {len(companies_data)} existing companies from {state_filename}")
        except Exception as e:
            print(f"Error loading existing data: {str(e)}")
    
    # Initialize variables
    page = start_page
    has_next_page = True
    fr_param = None  # Will be set after first page
    session = requests.Session()  # Create a session for connection pooling
    
    try:
        while has_next_page:
            # Construct URL for the current page
            if page == 0:
                # First page URL
                url = f"{BASE_URL}/register.php?cmd=search&stichwort=&firma=&branche=&vonplz=&ort=&strasse=&vorwahl=&bundesland={state}&Suchen=Suchen"
            else:
                # Use the extracted fr_param from the previous page or get it based on state
                if fr_param is None:
                    fr_param = get_fr_param_for_state(state)
                url = f"{BASE_URL}/register.php?cmd=mysearch&fr={fr_param}&auswahl=alle&ap={page}"
            
            print(f"Fetching page {page+1} for state {state_display}: {url}")
            
            # Update progress
            progress['current_state_index'] = STATES.index(state)
            progress['current_page'] = page
            save_progress(progress)
            
            # Fetch the page
            content = fetch_page(url, session=session)
            if not content:
                print(f"Failed to fetch page {page+1} for state {state_display}")
                
                # Special handling for page 6 (when page=5)
                if page == 5:
                    print("This is the troublesome page 6. Trying with a modified URL...")
                    # Try an alternative URL construction for page 6
                    alt_url = f"{BASE_URL}/register.php?cmd=mysearch&auswahl=alle&ap=5"
                    print(f"Trying alternative URL: {alt_url}")
                    content = fetch_page(alt_url, session=session)
                    if not content:
                        print("Alternative URL also failed. Saving debug info.")
                        save_blocked_page(alt_url, b"", None, get_headers(), 
                                     "Alternative URL for page 6 failed", force_save=True)
                        break
                else:
                    break
                
            # Extract companies from this page
            page_companies = get_companies_from_page(content, state_display)
            print(f"Found {len(page_companies)} companies on page {page+1}")
            
            # Check if we need to extract the fr_param from the page
            if page == 0 and fr_param is None:
                # Try to extract fr_param from the pagination links
                soup = BeautifulSoup(content, 'html.parser')
                pagination_links = soup.select(SELECTORS['pagination'])
                for link in pagination_links:
                    href = link.get('href', '')
                    fr_match = re.search(r'fr=([^&]+)', href)
                    if fr_match:
                        fr_param = fr_match.group(1)
                        print(f"Extracted fr_param: {fr_param}")
                        break
                
                # If still no fr_param, use the default
                if fr_param is None:
                    fr_param = get_fr_param_for_state(state)
                    print(f"Using default fr_param: {fr_param}")
            
            # Get pagination information with the modified function that knows the current page
            pagination = get_pagination_info(content, page, url)
            
            # Process companies
            for company in page_companies:
                company_id = company['id']
                
                # Skip already processed companies
                if company_id in processed_companies:
                    debug_print(f"Company {company_id} already processed, skipping")
                    continue
                
                # Get complete company details
                company_data = scrape_company_details(company_id, state_display, processed_companies)
                
                if company_data:
                    companies_data.append(company_data)
                    
                    # Save progress after each company
                    if len(companies_data) % 10 == 0:
                        # Save to CSV
                        df = pd.DataFrame(companies_data)
                        df.to_csv(state_filename, index=False)
                        print(f"Saved {len(companies_data)} companies to {state_filename}")
                        
                        # Update processed companies
                        save_processed_companies(processed_companies, progress)
                
                # Random delay between companies
                time.sleep(random.uniform(MIN_COMPANY_DELAY, MAX_COMPANY_DELAY))
            
            # Check if there's a next page
            if pagination['next_page_url']:
                has_next_page = True
                page += 1
            else:
                # Double-check if we should have more pages based on entry count
                if pagination['total_entries'] > (page + 1) * 10:
                    print(f"WARNING: Pagination suggests there are no more pages, but total entries ({pagination['total_entries']}) suggests there should be more.")
                    
                    # Special handling for page 6 onwards
                    if page >= 5:
                        print("Attempting to continue with direct URL construction...")
                        page += 1
                        has_next_page = True
                        continue
                
                has_next_page = False
                print(f"No more pages found for state {state_display}")
            
            # Save progress at the end of each page
            progress['current_page'] = page
            save_progress(progress)
            
            # Save data at the end of each page
            df = pd.DataFrame(companies_data)
            df.to_csv(state_filename, index=False)
            print(f"Saved {len(companies_data)} companies to {state_filename}")
            
            # Random delay between pages
            if has_next_page:
                delay = random.uniform(MIN_PAGE_DELAY, MAX_PAGE_DELAY)
                print(f"Waiting {delay:.2f} seconds before fetching next page...")
                time.sleep(delay)
    
    except KeyboardInterrupt:
        print("\nScraper interrupted by user")
        save_and_exit(progress, processed_companies, 0, "Scraper manually interrupted")
    except ScraperError as e:
        print(f"\nScraper error: {str(e)}")
        save_and_exit(progress, processed_companies, 1, str(e))
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        save_and_exit(progress, processed_companies, 1, f"Scraper crashed with error: {str(e)}")
    
    # Final save
    df = pd.DataFrame(companies_data)
    df.to_csv(state_filename, index=False)
    print(f"Completed scraping for state {state_display}. Saved {len(companies_data)} companies.")
    
    return companies_data

def main():
    """Main function to run the scraper."""
    start_time = time.time()
    
    print(f"Starting Firmenregister.de scraper")
    print(f"Output mode: {'One file per state' if ONE_FILE_PER_STATE else 'One combined file'}")
    
    # Create blocked pages directory if it doesn't exist
    if not os.path.exists(BLOCKED_PAGES_DIR):
        os.makedirs(BLOCKED_PAGES_DIR)
        print(f"Created directory for blocked pages: {BLOCKED_PAGES_DIR}")
    
    # Load previous progress
    progress = load_progress()
    
    # Get current state to process from progress or start with default
    current_state_index = progress.get('current_state_index', START_STATE_INDEX)
    current_page = progress.get('current_page', 0)
    
    # Validate index
    if current_state_index >= len(STATES):
        print("All states have been processed!")
        return
    
    try:
        # Process current state
        state = STATES[current_state_index]
        state_display = STATE_DISPLAY_NAMES.get(state, state)
        print(f"Processing state {current_state_index + 1}/{len(STATES)}: {state_display}")
        
        # Scrape the state
        companies = scrape_state(state, current_page)
        
        # Move to next state
        progress['current_state_index'] = current_state_index + 1
        progress['current_page'] = 0  # Reset page for next state
        save_progress(progress)
        
        # If using a combined file for all states, append data
        if not ONE_FILE_PER_STATE and companies:
            combined_filename = "all_companies.csv"
            
            if os.path.exists(combined_filename):
                # Append to existing file
                existing_df = pd.read_csv(combined_filename)
                new_df = pd.DataFrame(companies)
                combined_df = pd.concat([existing_df, new_df], ignore_index=True)
                combined_df.to_csv(combined_filename, index=False)
            else:
                # Create new file
                pd.DataFrame(companies).to_csv(combined_filename, index=False)
            
            print(f"Updated combined data file: {combined_filename}")
        
        # Calculate total execution time
        total_time = time.time() - start_time
        hours, remainder = divmod(total_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        print(f"\nTotal execution time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        
        if current_state_index + 1 < len(STATES):
            next_state = STATES[current_state_index + 1]
            next_state_display = STATE_DISPLAY_NAMES.get(next_state, next_state)
            print(f"Next run will process state: {next_state_display}")
        else:
            print("All states have been processed!")
    
    except KeyboardInterrupt:
        print("\nScraper interrupted by user")
        save_and_exit(progress, load_processed_companies(), 0, "Scraper manually interrupted")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        save_and_exit(progress, load_processed_companies(), 1, f"Scraper crashed with error: {str(e)}")

if __name__ == "__main__":
    main()