import requests
from bs4 import BeautifulSoup
import pandas as pd
import sys
import os

def scrape_fmcsa_data(mc_number):
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&query_string={mc_number}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        data = {'MC_Number': mc_number}
        tables = soup.find_all('table', {'class': 'query_result'})
        if not tables:
            return None

        df = pd.read_html(str(tables[0]))[0]
        df.columns = ['Field', 'Value']
        df['Field'] = df['Field'].str.replace(':', '').str.strip()
        df = df.set_index('Field')
        
        data['Legal_Name'] = df.loc['Legal Name'].Value if 'Legal Name' in df.index else ''
        data['Phone'] = df.loc['Phone'].Value if 'Phone' in df.index else ''
        data['USDOT_Number'] = df.loc['USDOT Number'].Value if 'USDOT Number' in df.index else ''
        
        return data
    except Exception as e:
        print(f"Error scraping MC {mc_number}: {e}")
        return None

if __name__ == "__main__":
    # Hum ab command line se MC numbers ki ek comma-separated string le rahe hain
    if len(sys.argv) < 2:
        print("Error: No MC numbers provided.")
        sys.exit(1)
        
    mc_numbers_str = sys.argv[1]
    mc_numbers = [mc.strip() for mc in mc_numbers_str.split(',') if mc.strip()]
    
    print(f"Processing {len(mc_numbers)} MC numbers.")

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
        # Hum har job ke liye ek unique file bana rahe hain
        run_id = os.environ.get('GITHUB_RUN_ID', 'local')
        file_path = os.path.join(output_dir, f'results_{run_id}.csv')
        df_final.to_csv(file_path, index=False)
        print(f"Data saved to {file_path}")
    else:
        print(f"No data was scraped.")
