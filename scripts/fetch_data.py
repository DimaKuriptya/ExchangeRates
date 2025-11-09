import os
from datetime import date, timedelta

import requests
import pandas as pd
from dotenv import load_dotenv


def fetch_api_data():
    load_dotenv()
    app_id = os.getenv("APP_ID")

    url = 'https://openexchangerates.org/api/historical/'
    params = {
        'app_id': app_id
    }

    start_date = date(2025, 7, 1)
    end_date = date.today()

    df = pd.DataFrame({
        'currency': pd.Series(dtype='str'),
        'rate': pd.Series(dtype='float'),
        'date': pd.Series(dtype='datetime64[D]')
    })

    current = start_date
    # According to openexchangerates docs, they publish new values at 23:59:59 UTC. So, if we are using the UTC time zone, there is no point in taking the current date, as we will be getting the latest available data, i.e. yesterday's data.
    while current < end_date:
        response = requests.get(f'{url}{current}.json', params=params).json()
        df_current = pd.DataFrame(response['rates'].items(), columns=['currency', 'rate'])
        df_current['date'] = pd.to_datetime(current, format='%Y-%m-%d')
        df = pd.concat([df, df_current], ignore_index=True)

        current += timedelta(days=1) 

    return df, current
