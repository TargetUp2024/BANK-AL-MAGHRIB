import pandas as pd
import cloudscraper  # Changed from requests
from bs4 import BeautifulSoup as bs
import os 
import time
from datetime import datetime, timedelta

# Create the scraper instance
# We specify a browser profile to bypass stricter anti-bot checks
scraper = cloudscraper.create_scraper(
    browser={
        'browser': 'chrome',
        'platform': 'windows',
        'desktop': True
    }
)

URL = "https://www.bkam.ma/fr/Achats/Appels-d-offres/Avis-d-appels-d-offres"

# Use the scraper instead of requests
try:
    resp = scraper.get(URL, timeout=30)
    resp.raise_for_status()
except Exception as e:
    print(f"❌ Failed to fetch page: {e}")
    exit(1)

soup = bs(resp.text, "html.parser")

# Find the table containing the data
table = soup.find('table')

# Safety check: If table is None, the site layout changed or we are still blocked
if table is None:
    print("❌ Error: Could not find the table. Site layout might have changed or bot protection is blocking us.")
    # Print a bit of the HTML for debugging in GitHub logs
    print(resp.text[:500])
    exit(1)

rows = table.find_all('tr')
results = []

for row in rows:
    tds = row.find_all('td')
    if len(tds) == 4:
        reference = tds[0].get_text(separator=" ", strip=True)
        title = tds[1].get_text(separator=" ", strip=True)
        date_str = tds[2].get_text(separator=" ", strip=True)
        
        if not title or title == " ":
            continue
            
        link_tag = tds[3].find('a')
        link = None
        if link_tag and 'href' in link_tag.attrs:
            link = link_tag['href']
            if link.startswith('/'):
                link = "https://www.bkam.ma" + link
                
        results.append({
            "Reference": reference,
            "Title": title,
            "Date": date_str,
            "Link": link
        })

df = pd.DataFrame(results)

# Clean and filter dates
df['Date'] = df['Date'].str.replace(' à ', ' ', regex=False)
df['Date'] = pd.to_datetime(df['Date'], dayfirst=True, errors='coerce')
df = df.dropna(subset=['Date'])

today = pd.Timestamp.now().normalize()
min_deadline = today + pd.Timedelta(days=7)
df_long_deadline = df[df['Date'] >= min_deadline].sort_values(by='Date')

WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")

for idx, row in df_long_deadline.iterrows():
    print(f"🚀 Sending: {row['Title']}")
    payload = {
        "Reference": row["Reference"],
        "Title": row["Title"],
        "URL": row["Link"],
        "Date": row["Date"].strftime('%d/%m/%Y %H:%M') 
    }
    
    try:
        # Use the same scraper for the POST request to maintain session if needed
        response = scraper.post(WEBHOOK_URL, json=payload, timeout=60)
        if response.status_code == 200:
            print(f"✅ Sent successfully.")
        else:
            print(f"⚠️ Status {response.status_code}")
        time.sleep(1)
    except Exception as e:
        print(f"❌ Error: {e}")
