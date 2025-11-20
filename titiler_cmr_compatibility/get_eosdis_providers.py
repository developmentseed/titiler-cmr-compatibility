import requests
import json

def get_eosdis_shortnames():
    """
    Fetch provider data from NASA CMR and filter to EOSDIS consortium members.
    Returns a list of ShortNames for EOSDIS providers.
    """
    url = "https://cmr.earthdata.nasa.gov/search/providers"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        eosdis_shortnames = []
        
        # Iterate through all provider items
        for item in data.get('items', []):
            if item is None:
                continue
                
            # Check if provider has Consortiums field
            consortiums = item.get('Consortiums', [])
            
            # Filter for EOSDIS consortium members
            if 'EOSDIS' in consortiums:
                # Get the ShortName from Organizations
                organizations = item.get('Organizations', [])
                if organizations:
                    shortname = organizations[0].get('ShortName', '')
                    if shortname:
                        eosdis_shortnames.append(shortname)
        
        return sorted(eosdis_shortnames)
    
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return []

if __name__ == "__main__":
    shortnames = get_eosdis_shortnames()
    
    print(f"Found {len(shortnames)} EOSDIS providers:\n")
    for name in shortnames:
        print(f"  - {name}")
