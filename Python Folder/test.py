import requests
import pandas as pd

# API URL
url = "https://jsonplaceholder.typicode.com/posts"

# Send a GET request to the API
response = requests.get(url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()
    
    # Convert the JSON data to a DataFrame
    df = pd.DataFrame(data)
    
    # Display the DataFrame
    print(df)
else:
    print(f"Error: {response.status_code}")
    print("Failed to retrieve data from the API.")
# The code above sends a GET request to the Open Meteo API and retrieves weather forecast data.
# It then converts the JSON response into a pandas DataFrame for easier manipulation and analysis.
# The DataFrame is printed to the console.
# Note: Make sure to install the required libraries if you haven't already.