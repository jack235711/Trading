
import requests
import json
import math

url = "http://localhost:8000/ohlc?start_date=2025-04-01"
print(f"Fetching {url}...")

try:
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    
    try:
        data = response.json()
        print(f"Data type: {type(data)}")
        
        if isinstance(data, list):
            print(f"List length: {len(data)}")
            if len(data) > 0:
                print(f"First item: {data[0]}")
                
            # Validate all items
            for i, item in enumerate(data):
                if item is None:
                    print(f"ERROR: Item {i} is None")
                    continue
                    
                for key in ['open', 'high', 'low', 'close']:
                    val = item.get(key)
                    if val is None:
                        print(f"ERROR: Item {i} key '{key}' is None")
                    elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                        print(f"ERROR: Item {i} key '{key}' is NaN/Inf: {val}")
                        
                if item.get('time') is None:
                    print(f"ERROR: Item {i} key 'time' is None")
                    
            print("Validation complete.")
        else:
            print(f"ERROR: Response is not a list: {data}")
            
    except json.JSONDecodeError as e:
        print(f"JSON Decode Error: {e}")
        print(f"Raw content (first 500 chars): {response.text[:500]}")

except Exception as e:
    print(f"Request failed: {e}")
