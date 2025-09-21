import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os

def scrape_fmcsa_data(batch_number):
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={batch_number}"
    
    try:
        response = requests.get(url, timeout=30 )
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        tables = soup.find_all('table', {'class': 'query_result'})
        if not tables:
            return None

        df = pd.read_html(str(tables[0]))[0]
        
        # Data cleaning
        df.columns = ['Field', 'Value']
        df['Field'] = df['Field'].str.replace(':', '').str.strip()
        df = df.set_index('Field')
        
        return df.to_dict()['Value']
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL for batch {batch_number}: {e}")
        return None
    except Exception as e:
        print(f"An error occurred during scraping for batch {batch_number}: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) > 1:
        batch = sys.argv[1]
        print(f"Processing batch: {batch}")
        
        data = scrape_fmcsa_data(batch)
        
        if data:
            output_dir = 'output'
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            df_final = pd.DataFrame([data])
            file_path = os.path.join(output_dir, f'fmcsa_data_{batch}.csv')
            df_final.to_csv(file_path, index=False)
            print(f"Data for batch {batch} saved to {file_path}")
        else:
            print(f"No data found or error occurred for batch {batch}")
    else:
        print("No batch number provided.")
