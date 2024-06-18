import os
import requests
import json
from pymongo import MongoClient
from bs4 import BeautifulSoup
import time as tm
from itertools import groupby
from datetime import datetime, timedelta, time
from urllib.parse import quote
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
import pandas as pd
import sys
import ast
from dotenv import load_dotenv

def get_with_retry(url, config, retries=3, delay=1):
    # Get the URL with retries and delay
    for i in range(retries):
        try:
            if len(config['proxies']) > 0:
                r = requests.get(url, headers=config['headers'], proxies=config['proxies'], timeout=5)
            else:
                r = requests.get(url, headers=config['headers'], timeout=5)
            return BeautifulSoup(r.content, 'html.parser')
        except requests.exceptions.Timeout:
            print(f"Timeout occurred for URL: {url}, retrying in {delay}s...")
            tm.sleep(delay)
        except Exception as e:
            print(f"An error occurred while retrieving the URL: {url}, error: {e}")
    return None

def transform(soup, config):
    joblist = []
    try:
        divs = soup.find_all('div', class_='base-search-card__info')
    except:
        print("Empty page, no jobs found")
        return joblist
    for item in divs:
        title = item.find('h3').text.strip()
        company = item.find('a', class_='hidden-nested-link')
        location = item.find('span', class_='job-search-card__location')
        parent_div = item.parent
        entity_urn = parent_div['data-entity-urn']
        job_posting_id = entity_urn.split(':')[-1]
        job_url = 'https://www.linkedin.com/jobs/view/'+job_posting_id+'/'

        date_tag_new = item.find('time', class_ = 'job-search-card__listdate--new')
        date_tag = item.find('time', class_='job-search-card__listdate')
        date = date_tag['datetime'] if date_tag else date_tag_new['datetime'] if date_tag_new else ''
        job_description = ''
        job = {
            'title': title,
            'company': company.text.strip().replace('\n', ' ') if company else '',
            'location': location.text.strip() if location else '',
            'date': date,
            'job_url': job_url,
            'job_description': job_description,
            'role': config['role'],
            'stack': []
        }
        joblist.append(job)
    return joblist

def transform_job(soup):
    div = soup.find('div', class_='description__text description__text--rich')
    if div:
        for element in div.find_all(['span', 'a']):
            element.decompose()
        for ul in div.find_all('ul'):
            for li in ul.find_all('li'):
                li.insert(0, '-')
        text = div.get_text(separator='\n').strip()
        text = text.replace('\n\n', '')
        text = text.replace('::marker', '-')
        text = text.replace('-\n', '- ')
        text = text.replace('Show less', '').replace('Show more', '')
        return text
    else:
        return "Could not find Job Description"

def safe_detect(text):
    try:
        return detect(text)
    except LangDetectException:
        return 'en'

def remove_irrelevant_jobs(joblist, config):
    new_joblist = [job for job in joblist if not any(word.lower() in job['job_description'].lower() for word in config['desc_words'])]   
    new_joblist = [job for job in new_joblist if not any(word.lower() in job['title'].lower() for word in config['title_exclude'])] if len(config['title_exclude']) > 0 else new_joblist
    new_joblist = [job for job in new_joblist if any(word.lower() in job['title'].lower() for word in config['title_include'])] if len(config['title_include']) > 0 else new_joblist
    new_joblist = [job for job in new_joblist if safe_detect(job['job_description']) in config['languages']] if len(config['languages']) > 0 else new_joblist
    new_joblist = [job for job in new_joblist if not any(word.lower() in job['company'].lower() for word in config['company_exclude'])] if len(config['company_exclude']) > 0 else new_joblist

    return new_joblist

def remove_duplicates(joblist, config):
    joblist.sort(key=lambda x: (x['title'], x['company']))
    joblist = [next(g) for k, g in groupby(joblist, key=lambda x: (x['title'], x['company']))]
    return joblist

def convert_date_format(date_string):
    date_format = "%Y-%m-%d"
    try:
        job_date = datetime.strptime(date_string, date_format).date()
        return job_date
    except ValueError:
        print(f"Error: The date for job {date_string} - is not in the correct format.")
        return None

def create_connection():
    mongo_uri = os.getenv("MONGO_URI")
    client = MongoClient(mongo_uri)
    db = client.insights
    return db

def get_jobcards(config):
    all_jobs = []
    for k in range(0, config['rounds']):
        for query in config['search_queries']:
            keywords = quote(query['keywords']) # URL encode the keywords
            location = quote(query['location']) # URL encode the location
            for i in range (0, config['pages_to_scrape']):
                url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keywords}&location={location}&f_TPR=&f_WT={query['f_WT']}&geoId=&f_TPR={config['timespan']}&start={25*i}"
                soup = get_with_retry(url, config)
                jobs = transform(soup, config)  # Pass config to transform
                all_jobs = all_jobs + jobs
                print("Finished scraping page: ", url)
                tm.sleep(config['delay_between_requests'])
    print ("Total job cards scraped: ", len(all_jobs))
    all_jobs = remove_duplicates(all_jobs, config)
    print ("Total job cards after removing duplicates: ", len(all_jobs))
    all_jobs = remove_irrelevant_jobs(all_jobs, config)
    print ("Total job cards after removing irrelevant jobs: ", len(all_jobs))
    return all_jobs

def find_new_jobs(all_jobs, db, config):
    jobs_collection = db[config['jobs_tablename']]
    filtered_jobs_collection = db[config['filtered_jobs_tablename']]
    existing_jobs = list(jobs_collection.find({}, {"title": 1, "company": 1, "date": 1, "job_url": 1}))
    existing_jobs_df = pd.DataFrame(existing_jobs)
    new_joblist = [job for job in all_jobs if not job_exists(existing_jobs_df, job)]
    return new_joblist

def job_exists(df, job):
    if df.empty:
        return False
    return ((df['job_url'] == job['job_url']).any() | (((df['title'] == job['title']) & (df['company'] == job['company']) & (df['date'] == job['date'])).any()))

def main():
    start_time = tm.perf_counter()
    job_list = []

    # Load environment variables
    load_dotenv()

    config = {
        'proxies': ast.literal_eval(os.getenv('PROXIES', '{}')),
        'headers': ast.literal_eval(os.getenv('HEADERS', '{}')),
        'search_queries': ast.literal_eval(os.getenv('SEARCH_QUERIES')),
        'desc_words': ast.literal_eval(os.getenv('DESC_WORDS')),
        'title_exclude': ast.literal_eval(os.getenv('TITLE_EXCLUDE')),
        'title_include': ast.literal_eval(os.getenv('TITLE_INCLUDE')),
        'company_exclude': ast.literal_eval(os.getenv('COMPANY_EXCLUDE')),
        'languages': ast.literal_eval(os.getenv('LANGUAGES', '[]')),
        'timespan': os.getenv('TIMESPAN'),
        'jobs_tablename': os.getenv('JOBS_TABLENAME'),
        'filtered_jobs_tablename': os.getenv('FILTERED_JOBS_TABLENAME'),
        'pages_to_scrape': int(os.getenv('PAGES_TO_SCRAPE')),
        'rounds': int(os.getenv('ROUNDS')),
        'days_to_scrape': int(os.getenv('DAYS_TO_SCRAPE')),
        'role': os.getenv('ROLE'),
        'delay_between_requests': int(os.getenv('DELAY_BETWEEN_REQUESTS'))
        
    }

    all_jobs = get_jobcards(config)
    db = create_connection()
    all_jobs = find_new_jobs(all_jobs, db, config)
    print ("Total new jobs found after comparing to the database: ", len(all_jobs))

    if len(all_jobs) > 0:
        for job in all_jobs:
            job_date = convert_date_format(job['date'])
            job_date = datetime.combine(job_date, time())
            if job_date < datetime.now() - timedelta(days=config['days_to_scrape']):
                continue
            print('Found new job: ', job['title'], 'at ', job['company'], job['job_url'])
            desc_soup = get_with_retry(job['job_url'], config)
            job['job_description'] = transform_job(desc_soup)
            language = safe_detect(job['job_description'])
            if language not in config['languages']:
                print('Job description language not supported: ', language)
            job_list.append(job)
            tm.sleep(config['delay_between_requests'])
        
        jobs_to_add = remove_irrelevant_jobs(job_list, config)
        print ("Total jobs to add: ", len(jobs_to_add))
        filtered_list = [job for job in job_list if job not in jobs_to_add]
        
        jobs_collection = db[config['jobs_tablename']]
        filtered_jobs_collection = db[config['filtered_jobs_tablename']]

        if jobs_to_add:
            jobs_collection.insert_many(jobs_to_add)
            print(f"Added {len(jobs_to_add)} new records to the {config['jobs_tablename']} collection")

        if filtered_list:
            filtered_jobs_collection.insert_many(filtered_list)
            print(f"Added {len(filtered_list)} new records to the {config['filtered_jobs_tablename']} collection")
        
    else:
        print("No jobs found")
    
    end_time = tm.perf_counter()
    print(f"Scraping finished in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()