import os
import requests
import time

# GitHub ki "master key" ko environment se haasil karna
TOKEN = os.environ.get("GITHUB_TOKEN")
if not TOKEN:
    raise ValueError("GitHub token not found. Please set the GITHUB_TOKEN environment variable.")

OWNER = "chuh31481-wq"
REPO = "fmcsa-result-" # Isi repository ka naam

# --- YAHAN ASAL, AAKHRI TABDEELI HAI ---
# Humne workflow ka naam theek kar diya hai.
WORKFLOW_ID = "extractor.yml" # Jise hum trigger karna chahte hain

# GitHub API ko call karne ke liye zaroori headers
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"token {TOKEN}",
}

def trigger_workflow(batch_number):
    """Ek specific workflow run ko trigger karta hai."""
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/actions/workflows/{WORKFLOW_ID}/dispatches"
    
    # Data jo hum workflow ko bhej rahe hain
    data = {
        "ref": "main", # Branch ka naam
        "inputs": {
            "batch": str(batch_number ) # Batch number
        }
    }
    
    response = requests.post(url, headers=HEADERS, json=data)
    
    if response.status_code == 204:
        print(f"Successfully triggered workflow '{WORKFLOW_ID}' for batch {batch_number}.")
    else:
        print(f"Error triggering workflow for batch {batch_number}. Status: {response.status_code}, Response: {response.text}")
    
    return response.status_code

def main():
    """Main function jo 100 parallel jobs ko trigger karta hai."""
    print(f"Starting to dispatch 100 parallel jobs for workflow: '{WORKFLOW_ID}'...")
    
    successful_triggers = 0
    for i in range(1, 101): # 1 se 100 tak
        status = trigger_workflow(i)
        if status == 204:
            successful_triggers += 1
        
        # GitHub API ki rate limit se bachne ke liye, har trigger ke baad thora sa waqfa
        time.sleep(1) 
        
    print(f"\nDispatching complete. Successfully triggered {successful_triggers} out of 100 workflows.")

if __name__ == "__main__":
    main()
