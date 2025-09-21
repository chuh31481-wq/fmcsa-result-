import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os

def scrape_fmcsa_data(batch_number):
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={batch_number}"
    
    # --- YAHAN ASAL TABDEELI HAI ---
    # Hum "insan" bann kar request bhej rahe hain
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64 ) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Humne yahan headers add kiye hain
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status() # Agar 4xx ya 5xx error aaye to foran ruk jao
        
        soup = BeautifulSoup(response.content, 'lxml')
        
        tables = soup.find_all('table', {'class': 'query_result'})
        if not tables:
            return None

        df = pd.read_html(str(tables[0]))[0]
        df.columns = ['Field', 'Value']
        df['Field'] = df['Field'].str.replace(':', '').str.strip()
        df = df.set_index('Field')
        
        return df.to_dict()['Value']
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"Access Denied (403) for batch {batch_number}. The website is blocking us.")
        else:
            print(f"HTTP Error for batch {batch_number}: {e}")
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
            print(f"Data for batch {batch} saved to temporary file {file_path}")
        else:
            print(f"No data found or error occurred for batch {batch}")
    else:
        print("No batch number provided.")
