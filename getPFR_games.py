import requests
from bs4 import BeautifulSoup
import pandas as pd

# FILE 1

class WebpageTableScraper:
    def __init__(self, url):
        self.url = url

    def get_tables_as_dataframes(self):
        response = requests.get(self.url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        tables = soup.find_all('table')
        dataframes = []

        for table in tables:
            df = self._parse_table(table)
            if df is not None:
                dataframes.append(df)

        return dataframes

    def _parse_table(self, table):
        rows = table.find_all('tr')
        if len(rows) == 0:
            return None
        
        header_row = rows[0]
        headers = [header.get_text(strip=True) for header in header_row.find_all('th')]

        data = []
        for row in rows[1:]:
            row_data = [cell.get_text(strip=True) for cell in row.find_all(['td', 'th'])]
            data.append(row_data)

        if not headers:
            return None
        
        return pd.DataFrame(data, columns=headers)

# FILE 2 (Main function)

import json
import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from WebpageTableScraper import WebpageTableScraper
import io

url = 'https://www.pro-football-reference.com/years/2023/games.htm'  # Replace with the actual URL
scraper = WebpageTableScraper(url)
tables_dataframes = scraper.get_tables_as_dataframes()

df = tables_dataframes[0]

df = df[ df['Week'] != 'Week']

# Convert DataFrame to CSV in-memory
csv_buffer = io.BytesIO()
df.to_csv(csv_buffer, index=False, encoding='utf-8')


# Replace these with your values
bucket_name = 'leblanc-fantasy-footabll'
s3_key = '/2023_schedule.csv'

# Upload the file
s3 = boto3.client('s3')
s3.upload_fileobj(io.BytesIO(csv_buffer.getvalue()), bucket_name, s3_key)
def lambda_handler(event, context):

    return {
        'statusCode': 200,
        'body': f'df uploaded to {bucket_name}/{s3_key}'
    }

