import os
import sys
import requests
import json
import pymongo
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta, time
from urllib.parse import quote
import pandas as pd
import time
import ast
from itertools import groupby
from dotenv import load_dotenv
import logging
import re


logger = logging.getLogger("MyLogger")
logger.setLevel(logging.DEBUG)


file_handler = logging.FileHandler("test.log", mode = "w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter('%(levelname)s - %(message)s')

logger.addHandler(file_handler)



class JobScraper:
    def __init__(self):
        load_dotenv()

        self.config = {
            'proxies': ast.literal_eval(os.getenv('PROXIES', '{}')),
            'headers': ast.literal_eval(os.getenv('HEADERS', '{}')),
            'search_queries': ast.literal_eval(os.getenv('SEARCH_QUERIES')),
            'desc_words': ast.literal_eval(os.getenv('DESC_WORDS')),
            'title_exclude': ast.literal_eval(os.getenv('TITLE_EXCLUDE')),
            'title_include': ast.literal_eval(os.getenv('TITLE_INCLUDE')),
            'company_exclude': ast.literal_eval(os.getenv('COMPANY_EXCLUDE')),
            'timespan': os.getenv('TIMESPAN'),
            'jobs_tablename': os.getenv('JOBS_TABLENAME'),
            'filtered_jobs_tablename': os.getenv('FILTERED_JOBS_TABLENAME'),
            'pages_to_scrape': int(os.getenv('PAGES_TO_SCRAPE')),
            'rounds': int(os.getenv('ROUNDS')),
            'days_to_scrape': int(os.getenv('DAYS_TO_SCRAPE')),
            'role': os.getenv('ROLE'),
            'delay_between_requests': int(os.getenv('DELAY_BETWEEN_REQUESTS'))
        }

        self.job_list = []

        self.mongo_uri = os.getenv("MONGO_URI")
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
            self.db = self.client.insights
            print("Connected to MongoDB")
        except pymongo.uri_parser.InvalidURI:
            logger.warning("Failed to connect to MongoDB - Invalid URI")
        

    def get_with_retry(self, url, retries=3, delay=1):
        # Get the URL with retries and delay
        for i in range(retries):
            try:
                if len(self.config['proxies']) > 0:
                    r = requests.get(url, headers=self.config['headers'], proxies=self.config['proxies'], timeout=5)
                else:
                    r = requests.get(url, headers=self.config['headers'], timeout=5)
                return BeautifulSoup(r.content, 'html.parser')
            except requests.exceptions.Timeout:
                logger.warning(f"Timeout occurred for URL: {url}, retrying in {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.error(f"An error occurred while retrieving the URL: {url}, error: {e}")
        return None

    def transform(self, soup, searchedFrom):
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
                'role': self.config['role'],
                'searchLoc' : searchedFrom,
                'stack': [],
                'applied' : 0
            }
            joblist.append(job)
        return joblist

    def transform_job(self, soup):
        
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
            
    def remove_irrelevant_jobs(self, joblist):
        new_joblist = [job for job in joblist if not any(word.lower() in job['job_description'].lower() for word in self.config['desc_words'])]
        new_joblist = [job for job in new_joblist if not any(word.lower() in job['title'].lower() for word in self.config['title_exclude'])] if len(self.config['title_exclude']) > 0 else new_joblist
        new_joblist = [job for job in new_joblist if any(word.lower() in job['title'].lower() for word in self.config['title_include'])] if len(self.config['title_include']) > 0 else new_joblist
        new_joblist = [job for job in new_joblist if not any(word.lower() in job['company'].lower() for word in self.config['company_exclude'])] if len(self.config['company_exclude']) > 0 else new_joblist

        return new_joblist

    def remove_duplicates(self, joblist):
        joblist.sort(key=lambda x: (x['title'], x['company']))
        joblist = [next(g) for k, g in groupby(joblist, key=lambda x: (x['title'], x['company']))]
        return joblist

    def convert_date_format(self, date_string):
        date_format = "%Y-%m-%d"
        try:
            job_date = datetime.strptime(date_string, date_format).date()
            return job_date
        except ValueError:
            logger.error(f"Error: The date for job {date_string} - is not in the correct format.")
            return None

    def get_jobcards(self):
        all_jobs = []
        for k in range(0, self.config['rounds']):
            for query in self.config['search_queries']:
                keywords = quote(query['keywords']) # URL encode the keywords
                location = quote(query['location']) # URL encode the location
                for i in range(0, self.config['pages_to_scrape']):
                    url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={keywords}&location={location}&f_TPR=&f_WT={query['f_WT']}&geoId=&f_TPR={self.config['timespan']}&start={25*i}"
                    soup = self.get_with_retry(url)
                    jobs = self.transform(soup, location)
                    all_jobs.extend(jobs)
                    print("Finished scraping page: ", url)
                    time.sleep(self.config['delay_between_requests'])
        print(f"Total job cards scraped: {len(all_jobs)}")
        all_jobs = self.remove_duplicates(all_jobs)
        print(f"Total job cards after removing duplicates: {len(all_jobs)}")
        all_jobs = self.remove_irrelevant_jobs(all_jobs)
        print(f"Total job cards after removing irrelevant jobs: {len(all_jobs)}")
        return all_jobs

    def getCount(self, soup):
        tag = soup.find('span', class_= "num-applicants__caption")
        if tag:
            text = tag.get_text(strip=True)
            number = re.search(r'\b(\d+)\b', text)
            if number:
                extracted_number = number.group(1)
                return extracted_number
            else:
                print("Count find count of applicants")
                return 0
            
        else:
            tag = soup.find("figcaption", class_= "num-applicants__caption")
            if tag:
                text = tag.get_text(strip=True)
                number = re.search(r'\b(\d+)\b', text)
                if number:
                    extracted_number = number.group(1)
                    return extracted_number
                else:
                    print("Count find count of applicants")
                    return 0
            else:
                print("Count find count of applicants")
                return 0
                
    
    
    def find_new_jobs(self, all_jobs):
        try:
            jobs_collection = self.db[self.config['jobs_tablename']]
            filtered_jobs_collection = self.db[self.config['filtered_jobs_tablename']]
            existing_jobs = list(jobs_collection.find({}, {"title": 1, "company": 1, "date": 1, "job_url": 1}))
            existing_jobs_df = pd.DataFrame(existing_jobs)
            new_joblist = [job for job in all_jobs if not self.job_exists(existing_jobs_df, job)]
            print(f"Total new jobs found after comparing to the database: {len(all_jobs)}")
            if len(new_joblist) > 0:
                for job in new_joblist:
                    job_date = self.convert_date_format(job['date'])
                    job_date = datetime.combine(job_date, datetime.min.time())
                    if job_date < datetime.now() - timedelta(days=self.config['days_to_scrape']):
                        continue
                    print(f"Found new job: {job['title']} at {job['company']} {job['job_url']}")
                    desc_soup = self.get_with_retry(job['job_url'])
                    job['job_description'] = self.transform_job(desc_soup)
                    job['applied'] = self.getCount(desc_soup)
                    self.job_list.append(job)
                    time.sleep(self.config['delay_between_requests'])

                jobs_to_add = self.remove_irrelevant_jobs(self.job_list)
                print(f"Total jobs to add: {len(jobs_to_add)}", )
                filtered_list = [job for job in self.job_list if job not in jobs_to_add]

                jobs_collection = self.db[self.config['jobs_tablename']]
                filtered_jobs_collection = self.db[self.config['filtered_jobs_tablename']]

                if jobs_to_add:
                    jobs_collection.insert_many(jobs_to_add)
                    print(f"Added {len(jobs_to_add)} new records to the {self.config['jobs_tablename']} collection")

                if filtered_list:
                    filtered_jobs_collection.insert_many(filtered_list)
                    print(f"Added {len(filtered_list)} new records to the {self.config['filtered_jobs_tablename']} collection")

            else:
                logging.warning("There is no new job")

            return new_joblist
        except AttributeError:
            logging.info("New jobs could not be recognized due to a failure while connecting to the database.")
            return all_jobs
        except Exception as e:
            logging.error(f"Error While finding new jobs from database : {e}")
            return all_jobs

    def job_exists(self, df, job):
        if df.empty:
            return False
        return ((df['job_url'] == job['job_url']).any() | (((df['title'] == job['title']) & (df['company'] == job['company']) & (df['date'] == job['date'])).any()))

    def main(self):
        logging.info("Scraping Started...")
        start_time = time.perf_counter()

        all_jobs = self.get_jobcards()
        all_jobs = self.find_new_jobs(all_jobs)

        if len(all_jobs) == 0:
            logging.error("No jobs were found")

        end_time = time.perf_counter()
        logging.info(f"Scraping finished in {end_time - start_time:.2f} seconds")


def CheckErrors():
    with open("test.log", "r") as log:
        if "ERROR" in log.read():
            return 1
    return 0


if __name__ == "__main__":
    job_scraper = JobScraper()
    sys.exit(job_scraper.main() or CheckErrors())
