#!/usr/bin/env python
# coding: utf-8

# In[46]:


import requests
import pandas as pd
import datetime
import pyarrow as pa
import pyarrow.parquet as pq

class WeatherDataProcessor:
    def __init__(self, latitude, longitude, past_days=61):
        self.latitude = latitude
        self.longitude = longitude
        self.past_days = past_days
        self.api_url = f"https://api.open-meteo.com/v1/forecast?latitude={self.latitude}&longitude={self.longitude}&hourly=temperature_2m,rain,showers,visibility&past_days={self.past_days}"

    def fetch_data(self):
        try:
            response = requests.get(self.api_url)
            response.raise_for_status()
            data = response.json()
            return data
        except requests.RequestException as e:
            print(f"Error fetching data: {e}")
            return None

    def process_data(self, data):
        if data is None:
            return None
        
        daily_data = {}
  
        for index,item in enumerate(data['hourly']['time']):
 
            timestamp = datetime.datetime.strptime(item, '%Y-%m-%dT%H:%M')
            date_only = timestamp.date()
            
            date = date_only
            if date not in daily_data:
                daily_data[date] = {'temperature_2m': 0, 'rain': 0, 'showers': 0, 'visibility': 0}
           
            daily_data[date]['temperature_2m'] += data['hourly']['temperature_2m'][index]
            daily_data[date]['rain'] += data['hourly']['rain'][index]
            daily_data[date]['showers'] += data['hourly']['showers'][index]
            daily_data[date]['visibility'] += data['hourly']['visibility'][index]
        # Convert dictionary to DataFrame
        df = pd.DataFrame.from_dict(daily_data, orient='index')
        df.index.name = 'date'
        return df

    def save_to_parquet(self, dataframe, filename):
        try:
            table = pa.Table.from_pandas(dataframe)
            pq.write_table(table, filename)
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving data to Parquet: {e}")

# Define location
latitude = 51.5085
longitude = -0.1257

# Create instance of WeatherDataProcessor
processor = WeatherDataProcessor(latitude, longitude)

# Fetch data
data = processor.fetch_data()

# Process data
df = processor.process_data(data)
print(df)

# Save data to Parquet file
processor.save_to_parquet(df, 'weather_data.parquet')


# In[ ]:




