import pandas as pd
import requests
import re
from bs4 import BeautifulSoup as bs
import os 
import glob
import time
from datetime import datetime, timedelta
today = datetime.now() - timedelta(days=1)
months_fr = ["janvier","février","mars","avril","mai","juin","juillet",
             "août","septembre","octobre","novembre","décembre"]
today_str = f"{today.day} {months_fr[today.month-1]} {today.year}"

URL = "https://www.bkam.ma/fr/Achats/Appels-d-offres/Avis-d-appels-d-offres"

resp = requests.get(URL)

soup = bs(resp.text, "html.parser")

# Find the table containing the data
table = soup.find('table')

# Find all rows in the table
rows = table.find_all('tr')

results =[]

for row in rows:
    # Get all cells (columns) in the current row
    tds = row.find_all('td')
    
    # Check if it's a valid data row (must have 4 columns)
    if len(tds) == 4:
        # Extract and clean text using a space separator to handle <br> and <p> tags gracefully
        reference = tds[0].get_text(separator=" ", strip=True)
        title = tds[1].get_text(separator=" ", strip=True)
        date = tds[2].get_text(separator=" ", strip=True)
        
        # Skip empty rows (the HTML has a dummy row with just &nbsp;)
        if not title or title == " ":
            continue
            
        # Extract the link from the <a> tag
        link_tag = tds[3].find('a')
        link = None
        if link_tag and 'href' in link_tag.attrs:
            link = link_tag['href']
            # If the link is a relative path, prepend the domain
            if link.startswith('/'):
                link = "https://www.bkam.ma" + link
                
        # Append the extracted data to our results list
        results.append({
            "Reference": reference,
            "Title": title,
            "Date": date,
            "Link": link
        })

# Print the extracted data (Showing the first 3 for brevity)
df = pd.DataFrame(results)

df['Date'] = df['Date'].str.replace(' à ', ' ', regex=False)
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')

# 2. Drop any rows that couldn't be parsed
df = df.dropna(subset=['Date'])

# 3. Define your "Minimum Deadline" (Today + 7 days)
today = pd.Timestamp.now().normalize()
min_deadline = today + pd.Timedelta(days=7)

# 4. FILTER: Deadline must be 7 days or MORE from today
df_long_deadline = df[df['Date'] >= min_deadline].sort_values(by='Date')

# To check your results:
print(f"Today is: {today.date()}")
print(f"Showing offers with deadlines on or after: {min_deadline.date()}")  

WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
# Updated loop part
for idx, row in df_long_deadline.iterrows():
    # Use idx+1 for the count, but let's be safe with the total count
    print(f"\n🚀 Sending row {idx+1}/{len(df_long_deadline)}: {row['Title']}")
    
    # Prepare payload
    payload = {
        "Reference": row["Reference"],
        "Title": row["Title"],
        "URL": row["Link"],
        # .strftime converts the Timestamp to a standard string
        "Date": row["Date"].strftime('%d/%m/%Y %H:%M') 
    }
    
    try:
        # Send POST request to n8n webhook
        response = requests.post(WEBHOOK_URL, json=payload, timeout=60)
        
        if response.status_code == 200:
            print(f"✅ Row {idx+1} sent successfully.")
        else:
            print(f"⚠️ Row {idx+1} status {response.status_code}: {response.text}")
        
        time.sleep(1)
        
    except Exception as e:
        print(f"❌ Error sending row {idx+1}: {e}")
