import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os

def scrape_fmcsa_data(mc_number):
    """Ek MC number ke liye data scrape karta hai."""
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string={mc_number}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Data extract karne ka logic yahan aayega
        # Yeh ek misaal hai, aap isay apne hisaab se behtar bana sakte hain
        data = {'MC_Number': mc_number}
        
        tables = soup.find_all('table', {'class': 'query_result'})
        if not tables:
            return None

        df = pd.read_html(str(tables[0]))[0]
        df.columns = ['Field', 'Value']
        df['Field'] = df['Field'].str.replace(':', '').str.strip()
        df = df.set_index('Field')
        
        # Zaroori fields nikalna
        data['Legal_Name'] = df.loc['Legal Name'].Value if 'Legal Name' in df.index else ''
        data['Phone'] = df.loc['Phone'].Value if 'Phone' in df.index else ''
        data['USDOT_Number'] = df.loc['USDOT Number'].Value if 'USDOT Number' in df.index else ''
        
        return data
        
    except Exception as e:
        print(f"Error scraping MC {mc_number}: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No batch file path provided.")
        sys.exit(1)
        
    batch_file_path = sys.argv[1]
    batch_index = os.path.basename(batch_file_path) # e.g., "batch_aa"
    
    print(f"Processing batch file: {batch_file_path}")
    
    try:
        with open(batch_file_path, 'r') as f:
            mc_numbers = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"Error: Batch file not found at {batch_file_path}")
        sys.exit(1)

    results = []
    for mc in mc_numbers:
        scraped_data = scrape_fmcsa_data(mc)
        if scraped_data:
            results.append(scraped_data)
    
    if results:
        output_dir = 'output'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        df_final = pd.DataFrame(results)
        # Hum har batch ke liye ek alag CSV file bana rahe hain
        file_path = os.path.join(output_dir, f'results_{batch_index}.csv')
        df_final.to_csv(file_path, index=False)
        print(f"Data for batch {batch_index} saved to {file_path}")
    else:
        print(f"No data scraped for batch {batch_index}")
