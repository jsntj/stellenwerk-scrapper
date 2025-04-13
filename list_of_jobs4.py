import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
import time
import random
import os

# Constants
BASE_URL = "https://www.stellenwerk.de/hamburg"
DELAY = 3  # Seconds between requests
BATCH_SIZE = 5  # Pages per batch
PAUSE_BETWEEN_BATCHES = 10  # Seconds pause after each batch
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.stellenwerk.de/"
}

def scrape_click_value(url):
    """
    Given a URL (e.g. for "dein job" or "dein profil"),
    scrapes the page and returns a snippet of its content.
    You can adjust the extraction logic based on the page structure.
    """
    try:
        print(f"Scraping click page: {url}")
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try to extract a content area by looking for a div with 'description'
        content_elem = soup.find('div', class_=lambda x: x and 'description' in x.lower())
        if content_elem:
            content = content_elem.get_text(separator=" ", strip=True)
        else:
            # Fallback: return the first part of the full text
            content = soup.get_text(separator=" ", strip=True)
        
        # Return the first 200 characters as a snippet/sample value.
        return content[:200]
    
    except Exception as e:
        print(f"Error scraping click value from {url}: {e}")
        return "N/A"

def scrape_job_details(url):
    """
    Scrapes the main details from a job posting.
    Additionally, if anchors for "dein job" and "dein profil" are found,
    the function "clicks" them (makes a request) and extracts a snippet from each.
    """
    try:
        full_url = f"https://www.stellenwerk.de{url}"
        print(f"\nScraping job details: {full_url}")
        
        response = requests.get(full_url, headers=HEADERS)
        response.raise_for_status()  # Ensure we got a valid response
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Standard extraction with flexible selectors
        title_elem = soup.find('h1') or soup.find(class_=lambda x: x and 'title' in x.lower())
        company_elem = (soup.find(class_='company-name') or 
                        soup.find(class_=lambda x: x and 'employer' in x.lower()) or
                        soup.find('h2'))
        location_elem = (soup.find(class_='job-location') or
                         soup.find(class_=lambda x: x and 'location' in x.lower()))
        salary_elem = (soup.find(class_='salary') or
                       soup.find(class_=lambda x: x and 'gehalt' in x.lower()))
        date_elem = (soup.find(class_='posting-date') or
                     soup.find(class_=lambda x: x and 'date' in x.lower()))
        
        # New: Follow the "dein job" link (if available)
        dein_job_value = "N/A"
        dein_job_anchor = soup.find(lambda tag: tag.name == "a" and "dein job" in tag.get_text(strip=True).lower())
        if dein_job_anchor and 'href' in dein_job_anchor.attrs:
            job_link = dein_job_anchor['href']
            # If relative, build the full URL
            if not job_link.startswith("http"):
                job_link = "https://www.stellenwerk.de" + job_link
            print(f"Found 'dein job' link: {job_link}")
            dein_job_value = scrape_click_value(job_link)
        
        # New: Follow the "dein profil" link (if available)
        dein_profil_value = "N/A"
        dein_profil_anchor = soup.find(lambda tag: tag.name == "a" and "dein profil" in tag.get_text(strip=True).lower())
        if dein_profil_anchor and 'href' in dein_profil_anchor.attrs:
            profil_link = dein_profil_anchor['href']
            if not profil_link.startswith("http"):
                profil_link = "https://www.stellenwerk.de" + profil_link
            print(f"Found 'dein profil' link: {profil_link}")
            dein_profil_value = scrape_click_value(profil_link)
        
        return {
            "Title": title_elem.get_text(strip=True) if title_elem else "N/A",
            "Company": company_elem.get_text(strip=True) if company_elem else "N/A",
            "Location": location_elem.get_text(strip=True) if location_elem else "N/A",
            "Salary": salary_elem.get_text(strip=True) if salary_elem else "N/A",
            "Posted Date": date_elem.get_text(strip=True) if date_elem else "N/A",
            "URL": full_url,
            "Dein Job": dein_job_value,
            "Dein Profil": dein_profil_value
        }
        
    except Exception as e:
        print(f"Error scraping job: {e}")
        return None

def scrape_page(start):
    """
    Scrapes job links from a paginated page.
    """
    try:
        url = f"{BASE_URL}?pagination%5Bstart%5D={start}"
        print(f"\nScraping page {start//10 + 1}: {url}")
        
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all job links with a filter for relevant href strings.
        job_links = []
        for a in soup.select('a[href*="/hamburg/"]'):
            href = a['href']
            if not any(x in href for x in ["account", "login", "register"]):
                job_links.append(href)
        
        print(f"Found {len(job_links)} job links")
        return list(set(job_links))  # Remove duplicates
        
    except Exception as e:
        print(f"Error scraping page: {e}")
        return []

def scrape_in_batches(max_pages=40):
    """
    Scrapes pages in batches, pausing between batches to reduce load on the server.
    """
    all_batches = []
    start = 0
    
    while start < max_pages * 10:
        batch_jobs = []
        for _ in range(BATCH_SIZE):
            job_links = scrape_page(start)
            if not job_links:
                break
                
            for link in job_links:
                job = scrape_job_details(link)
                if job:
                    batch_jobs.append(job)
                time.sleep(DELAY + random.random())
            
            start += 10
            time.sleep(DELAY)
        
        if batch_jobs:
            batch_num = (start // (BATCH_SIZE * 10)) + 1
            save_to_csv(batch_jobs, batch_num)
            all_batches.extend(batch_jobs)
            
            if start < max_pages * 10:
                print(f"\nPausing for {PAUSE_BETWEEN_BATCHES} seconds before next batch...")
                time.sleep(PAUSE_BETWEEN_BATCHES)
        else:
            break
    
    return all_batches

def save_to_csv(jobs, batch_num=None):
    """
    Saves the scraped job data into a CSV file.
    """
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M')
    if batch_num:
        filename = f"stellenwerk_jobs_batch{batch_num}_{timestamp}.csv"
    else:
        filename = f"stellenwerk_jobs_{timestamp}.csv"
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Title", "Company", "Location", "Salary", "Posted Date", "URL",
            "Dein Job", "Dein Profil"
        ])
        writer.writeheader()
        writer.writerows(jobs)
    print(f"\nSaved {len(jobs)} jobs to {filename}")

if __name__ == "__main__":
    print("Starting scraper in batches...")
    jobs = scrape_in_batches(max_pages=40)
    print("\nAll batches completed!")
