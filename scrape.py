import requests
import warnings
from bs4 import BeautifulSoup
import datetime
import time
import re
import csv
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

# Base URL patterns
base_url = "https://www.jobbank.gc.ca/jobsearch/jobposting/"
search_url_template = "https://www.jobbank.gc.ca/jobsearch/jobsearch?fage=30&page={}&sort=M&fprov=ON"  # Template for search URL
post_url = "https://www.jobbank.gc.ca/jobsearch/jobpostingtfw/"
results_per_page = 25  # Number of results per page

live_domains = ("outlook.com", "hotmail.com", "live.com", "live.ca")

# Define the headers with the desired User-Agent
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36"
}

warnings.filterwarnings("ignore", category=UserWarning, module='bs4')

# Function to get HTML content from a URL
def fetch_html(url, retries=3, backoff_factor=2):
    """Fetch HTML content from a URL with retry logic for 503 errors."""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                return response.text
            elif response.status_code == 503:
                wait_time = backoff_factor ** attempt  # Exponential backoff
                print(f"503 Service Unavailable: Retrying in {wait_time} seconds...")
                time.sleep(wait_time)  # Wait before retrying
            else:
                print(f"Failed to retrieve {url}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"An error occurred while fetching {url}: {e}")
            return None
    print(f"Failed to fetch {url} after {retries} attempts.")
    return None

# Function to extract job IDs from the search page
def extract_job_ids(search_page_html):
    soup = BeautifulSoup(search_page_html, 'html.parser')
    job_ids = []
    
    # Find all article tags and extract job IDs
    articles = soup.find_all('article', id=re.compile(r'article-\d+'))
    for article in articles:
        job_id = article['id'].split('-')[1]  # Extract ID from 'article-<id>'
        job_ids.append(job_id)

    return job_ids

# Function to extract the total number of job postings
def extract_total_postings(search_page_html):
    soup = BeautifulSoup(search_page_html, 'html.parser')
    results_summary = soup.find('div', class_='results-summary')
    
    if results_summary:
        found_span = results_summary.find('span', class_='found')
        if found_span:
            total_postings = found_span.get_text(strip=True)
            return int(total_postings.replace(',', ''))  # Convert to integer after removing commas
    return 0

def parse_job_posting_details(job_id, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract Job Title
    job_title = soup.find('h1', class_='title').find('span', property='title').get_text(strip=True)
    
    # Extract Posted Date
    posted_date = soup.find('span', property='datePosted').get_text(strip=True).replace("Posted on", "").strip()
    # Convert the date to a datetime object
    try:
        date_obj = datetime.datetime.strptime(posted_date, "%B %d, %Y")
        formatted_date = date_obj.strftime("%Y-%m-%d")  # Format to yyyy-MM-dd
    except ValueError:
        formatted_date = posted_date
    
    # Extract Employer Name and Details
    employer_name_tag = soup.find('span', property='hiringOrganization')
    if employer_name_tag:
        employer_name = employer_name_tag.get_text(strip=True)
        find_employer_link = employer_name_tag.find('a')
        if find_employer_link:
            employer_link = find_employer_link["href"]
        else:
            employer_link = ""
    else:
        employer_name = "Not available"
        employer_link = ""

    # Find the <ul> with the desired class
    job_posting_brief = soup.find('ul', class_="job-posting-brief colcount-lg-2")
    location, region, salary, hours, employment_type, source = ("",) * 6

    if job_posting_brief:
        # Extract the list items
        list_items = job_posting_brief.find_all('li')
        
        # Parse and format the details
        for li in list_items:
            # Check for specific details based on text patterns
            if "Location" in li.get_text():
                location = li.find('span', property="addressLocality").get_text(strip=True)
                region = li.find('span', property="addressRegion").get_text(strip=True)
            elif "Salary" in li.get_text():
                salary = li.find('span', property="minValue").get_text(strip=True)
                hours = li.find('span', property="workHours").get_text(strip=True)
            elif "Terms of employment" in li.get_text():
                employment_type = li.find('span', property="employmentType").get_text(strip=True)
            elif "Source" in li.get_text():
                source = li.get_text(strip=True).replace("Source", "").strip()

    lmia_div = soup.find('div', class_="disclaimer tfw col-md-12")
    lmia = "true" if lmia_div else "false"

    # Construct job URL
    job_url = f"{base_url}{job_id}"

    return {
        "job_id": job_id,  # Add job ID to the result
        "job_url": job_url,  # Add job URL to the result
        "job_title": job_title,
        "posted_date": formatted_date,
        "employer_name": employer_name,
        "employer_link": employer_link,
        "location": location,
        "region": region,
        "salary": salary,
        "hours": hours,
        "employment_type": employment_type,
        "source": source,
        "lmia": lmia
    }

# Function to make a POST request after fetching job posting details
def make_post_request(job_id, retries=3, backoff_factor=2):
    """Make a POST request with retry logic for 503 errors."""
    url = f"{post_url}{job_id}"
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.71 Safari/537.36"
    }
    
    data = {
        "seekeractivity:jobid": str(job_id),
        "seekeractivity_SUBMIT": "1",
        "jakarta.faces.ViewState": "stateless",
        "jakarta.faces.behavior.event": "action",
        "action": "applynowbutton",
        "jakarta.faces.partial.event": "click",
        "jakarta.faces.source": "seekeractivity",
        "jakarta.faces.partial.ajax": "true",
        "jakarta.faces.partial.execute": "jobid",
        "jakarta.faces.partial.render": "applynow markappliedgroup",
        "seekeractivity": "seekeractivity"
    }
    
    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, data=data)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'xml')
                cdata_section = soup.find('update', id="applynow").string
                
                email_soup = BeautifulSoup(cdata_section, 'html.parser')
                email_tag = email_soup.find('a', href=re.compile(r"^mailto:"))
                if email_tag:
                    email_address = email_tag.get_text(strip=True)
                    return email_address
                else:
                    return "Not found"
            elif response.status_code == 503:
                wait_time = backoff_factor ** attempt  # Exponential backoff
                print(f"503 Service Unavailable: Retrying in {wait_time} seconds...")
                time.sleep(wait_time)  # Wait before retrying
            else:
                print(f"Failed to POST to {url}. Status code: {response.status_code}")
                return "Failed"
        except Exception as e:
            print(f"An error occurred while making the POST request to {url}: {e}")
            return "Error"

    print(f"Failed to make POST request to {url} after {retries} attempts.")
    return "Error"

def fetch_job_and_email(job_id):
    url = f"{base_url}{job_id}"
    html_content = fetch_html(url)
    if html_content:
        job_details = parse_job_posting_details(job_id, html_content)  # Pass job_id to the function
        job_details['email'] = make_post_request(job_id)  # Retrieve email address
        job_details['live_email_recovery'] = ""
        if any(domain in job_details['email'] for domain in live_domains):
            job_details['live_email_recovery'] = get_live_recovery_email(job_details['email'])
        print(job_details)
        return job_details
    else:
        print("Couldn't extract job details from ID",job_id)
        return None

def get_live_recovery_email(email):
    # Define the URL and query parameters
    recovery_email = None
    url = "https://account.live.com/ResetPassword.aspx"
    params = {
        "mn": email
    }
    # Define the headers
    headers = {
        "Sec-Ch-Ua": '"Chromium";v="129", "Not=A?Brand";v="8"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Accept-Language": "en-US,en;q=0.9",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.71 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
        "Referer": "https://login.live.com/",
        "Accept-Encoding": "gzip, deflate, br",
        "Priority": "u=0, i"
    }

    # Send the GET request
    response = requests.get(url, headers=headers, params=params)
    response_text = response.text
    soup = BeautifulSoup(response_text, 'html.parser')

    # Find all <script> tags
    scripts = soup.find_all('script')

    # Extract the text within each <script> tag
    for script in scripts:
        script_text = script.get_text()
        if "//<![CDATA[" in script_text and "var ServerData=" in script_text:
            # Remove the beginning and end parts
            json_content = script_text.strip()
            json_content = json_content.replace('//<![CDATA[', '')
            json_content = json_content.replace('//]]>', '')
            json_content = json_content.replace('var ServerData=','')
            json_content = json_content.replace(';window.$Do&&window.$Do.register("ServerData",0,true)','')
            json_content = json_content.strip()
            json_content = json_content[:-1]
            # Parse it as a JSON object
            data_dict = json.loads(json_content)
            for proof in data_dict.get("oProofList", []):
                if proof.get("type") == "Email":
                    recovery_email = proof.get("name")
            if recovery_email is not None:
                print("Recovery email for",email,"is",recovery_email)
                return recovery_email
            else:
                print("No recovery email for",email)
                return None
    if recovery_email is None:
        print("No recovery email for",email)
        return None

def main():
    first_page_html = fetch_html(search_url_template.format(str(1)))
    if first_page_html:
        total_postings = extract_total_postings(first_page_html)
        print(f"Total job postings found: {total_postings}")

        # Calculate total pages needed
        total_pages = (total_postings + results_per_page - 1) // results_per_page

        with open('job_data.csv', 'a', newline='', encoding='utf-8') as output_file:
            dict_writer = None

            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for page_number in range(1, total_pages + 1):
                    print(f"Fetching page {page_number}...")
                    search_page_html = fetch_html(search_url_template.format(page_number))
                    job_ids = extract_job_ids(search_page_html)
                    print(f"Fetched job IDs: {job_ids}")  # Debugging line
                    for job_id in job_ids:
                        futures.append(executor.submit(fetch_job_and_email, job_id))
                    #    fetch_job_and_email(job_id)

                for future in as_completed(futures):
                    job_details = future.result()
                    if job_details:
                        print(job_details)
                        if dict_writer is None:
                            keys = job_details.keys()
                            dict_writer = csv.DictWriter(output_file, fieldnames=keys)
                            dict_writer.writeheader()
                        dict_writer.writerow(job_details)

            print("Job data updated in job_data.csv.")
    else:
        print("Failed to fetch the first page of job postings.")

if __name__ == "__main__":
    main()
