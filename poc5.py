import boto3
import json
import concurrent.futures
import requests
from datetime import date
import os
from dotenv import load_dotenv
from pathlib import Path
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import random

# set env path
dotEnvPath = Path('./.env')
load_dotenv(dotenv_path=dotEnvPath, override=True)

# lambda client
lambda_client = boto3.client('lambda')

# flaskapp
flaskapp = Flask(__name__)
@flaskapp.route('/')
def index():
    return 'Flask application is running with a cron job.'

# logger configuration
flaskapp.logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('flaskapp.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
flaskapp.logger.addHandler(file_handler)

# scraper implementation
class Invoker:
    def __init__(self):
        self.connection = None
        self.cursor = None

    def main(self):
        flaskapp.logger.info(f'cron job started')
        try:
            response = requests.get(os.environ["FETCH_ENDPOINT"])
            data = response.json()
            if(data):
                self.handle_request(data)
            else:
                flaskapp.logger.info('no data present')

        except Exception as e:
            flaskapp.logger.error(f'error in starting cron job: {e}')


    def storeScrapeInDb(self,row):
        try:
            payload = {
                    "asinId" : row[0],
                    "keyword" : row[1],
                    "rank": [row[2]],
                    "recordedAt": str(date.today())
                    }
            requests.post(os.environ["STORE_ENDPOINT"], json = payload)
        except Exception as e:
            flaskapp.logger.error(f'error when storing scraped data : {e}')

    def handle_request(self, records):
        try:
            payloadSize = int(os.environ["PAYLOAD_SIZE"])
            data = []
            for i in range(0, len(records), payloadSize):
                body = {
                    "maxPages": os.environ["MAX_PAGES"],
                    "pincode": os.environ["PINCODE"],
                    "input": records[i: i+payloadSize]
                }
                data.append(body)
            # print(data)

            with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.environ["MAX_CONCURRENCY"])) as executor:
                futures = [executor.submit(self.invoke_lambda_function, payload) for payload in data]
                while futures:
                    completed, _ = concurrent.futures.wait(futures, timeout=0)
                    for future in completed:
                        result = future.result()
                        rows = self.fetchScrapeData(result)
                        for row in rows:
                            if row:
                                self.storeScrapeInDb(row)

                        futures.remove(future)

        except Exception as e:
            # print(f'error in handling request: {e}')
            flaskapp.logger.error(f'error in handling request: {e}')
        

    def fetchScrapeData(self, result):
        status = result.get('statusCode', 0)
        scrapeResult = []
        if status == 200:
            data = json.loads(result.get('message'))['data']
            for d in data:
                if len(d):
                    scrapeResult.append([d.get('asin'), d.get('keyword'),d.get('rank')])

        elif status == 404:
            data = result.get('data')
            for d in data:
                if len(d):
                    scrapeResult.append([d.get('asin'), d.get('keyword'),d.get('rank')])
            
        elif status == 500:
            data = result.get('data')
            for d in data:
                if len(d):
                    scrapeResult.append([d.get('asin'), d.get('keyword'),d.get('rank')])
        return scrapeResult
            
                


    def invoke_lambda_function(self, payload):
        randomWorker = random.randint(1, int(os.environ["TOTAL_LAMBDA"]))
        maxRetries = 2
        retry = 0
        flaskapp.logger.info(f'invoked worker{randomWorker}')
        try:
            # cases handled if product is found and product is not found.
            while True:
                functionName = f'{os.environ["BASE_LAMBDA_ARN"]}{randomWorker}'
                response = lambda_client.invoke(
                    FunctionName = functionName,
                    InvocationType= 'RequestResponse',
                    Payload =  json.dumps(payload)
                )
                resPayload = response['Payload'].read()
                res = json.loads(resPayload) 

                if(res['statusCode'] and res['statusCode'] == 200):
                    flaskapp.logger.info(f'scraped successfully {res}')
                    return res
                
                if retry > maxRetries:
                    break

                retry = retry + 1
                flaskapp.logger.warn(f'retrying worker{randomWorker}')

            flaskapp.logger.error(f'500 error payload: {payload}')

            result = []
            for data in payload['input']:
                result.append({"asin": data['ASIN'], "keyword": data["keyword"], "rank": 0})
            return {"statusCode": 404, "data": result}
        
            # cases handled after applying retries may be status code 500

        except Exception as e:
            # other exception
            flaskapp.logger.error(f'Lambda service exception for {payload}: error: {e}')
            result = []
            for data in payload['input']:
                result.append({"asin": data['ASIN'], "keyword": data["keyword"], "rank": 0})
            return {"statusCode": 500, "data": result}

# cron job scheduler
scheduler = BackgroundScheduler(job_defaults={'max_instances': 2})
scheduler.start()
app = Invoker()
scheduler.add_job(app.main, 'interval', seconds=int(os.environ['CRON_SCHEDULE_IN_SECONDS']))

if __name__ == "__main__":
    flaskapp.run()
