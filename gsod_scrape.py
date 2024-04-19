import requests
from bs4 import BeautifulSoup
import datetime
import pandas as pd
import calendar

def scrape_station_data(year,station):
    base_url = f"https://ogimet.com/cgi-bin/gsodres?lang=en&mode=0&state=Pola&ind={station}&ord=REV"
    dataframes = {}
    for month in range(1, 13):
        print(f'Przetwarzanie miesiąca: {month}')
        end_day = calendar.monthrange(year, month)[1]
        start_date = datetime.date(year, month, end_day)

        url = f"{base_url}&ano={start_date.year}&mes={start_date.month}&day={start_date.day}&ndays={end_day}"

        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find_all('table')[3]
            raw_data = []
            headers = []

            #get table contents
            rows = table.find_all('tr')
            for row in rows[1:]:
                cols = row.find_all('td')
                cols = [ele.text.strip() for ele in cols]
                raw_data.append(cols)

            # print(raw_data)
            # Fetch headers for each request
            for th in table.find_all('th'):
                # Check if the header has colspan attribute and add sub-headers accordingly
                colspan = th.get('colspan')
                if colspan:
                    sub_headers = [th.text.strip() + f" (Sub{i+1})" for i in range(int(colspan))]
                    headers.extend(sub_headers)
                else:
                    headers.append(th.text.strip())
            # clean up fetched headers
            # print(headers)
            clean_headers = []
            if 'Date' in headers:
                clean_headers.append('Date')
            if 'Temperature(°C) (Sub1)' in headers: 
                clean_headers.append('Temperature(°C) (Max)')
            if 'Temperature(°C) (Sub2)' in headers: 
                clean_headers.append('Temperature(°C) (Min)')
            if 'Temperature(°C) (Sub3)' in headers: 
                clean_headers.append('Temperature(°C) (Mean)')
            if 'Hr.Med(%)' in headers: 
                clean_headers.append('Hr.Med(%)')
            if 'Wind(km/h) (Sub1)' in headers and 'Gust' in headers: 
                clean_headers.append('Wind(km/h) (Gust)')
            if 'Wind(km/h) (Sub1)' in headers and not 'Gust' in headers: 
                clean_headers.append('Wind(km/h) (Max)')
            if 'Wind(km/h) (Sub2)' in headers and 'Gust' in headers: 
                clean_headers.append('Wind(km/h) (Max)')
            if 'Wind(km/h) (Sub2)' in headers and not 'Gust' in headers: 
                clean_headers.append('Wind(km/h) (Min)')
            if 'Wind(km/h) (Sub3)' in headers: 
                clean_headers.append('Wind(km/h) (Min)')
            if 'Pressure(mb) (Sub1)' in headers: 
                clean_headers.append('Pressure(mb) (SLP)')
            if 'Pressure(mb) (Sub2)' in headers: 
                clean_headers.append('Pressure(mb) (STN)')
            if 'Vis(km)' in headers: 
                clean_headers.append('Vis(km)')
            if 'Prec(mm)' in headers: 
                clean_headers.append('Prec(mm)')
            if 'Snow(cm)' in headers: 
                clean_headers.append('Snow(cm)')
            clean_headers.append('Diary')

            # print (clean_headers)
             
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")


        #join table and headers into dataframe
        df = pd.DataFrame(raw_data, columns=clean_headers)
             
        #append current dataframe to dictionary
        dataframes[f'df{month}'] = df

    weather_table = pd.concat(dataframes.values(), axis=0, sort=False)\
        .drop_duplicates(subset='Date')\
        .sort_values(by='Date')\
        .dropna(how='all')\
        .replace('\\.', ',', regex=True)
    print('Próbka danych:')    
    print(weather_table.head())

    return weather_table #returns dataframe


station = input('Station number: ')[:5]
# print(station)
year_str = input('Year: ')
year = int(year_str)
weather_table = scrape_station_data(year,station)
weather_table.to_excel(f'station_data_{station}_{year}.xlsx', index=False)
