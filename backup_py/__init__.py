import time
import datetime
import requests
import numpy as np
import pandas as pd
from datetime import datetime

def toNow():
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    now = datetime.strptime(now, "%Y-%m-%d %H:%M:%S")
    return now

def toUnixtime(dataframe, format_current, col):
    dataframe[col] = (
                        pd.to_datetime(dataframe[col], format=format_current) - pd.Timestamp("1970-01-01")
                     ) // pd.Timedelta("1s") - 25200
    dataframe[col] = dataframe[col].replace(np.nan, None, regex=True)
    return dataframe

def seatalkNoti(bot_id, email_user, content):
    url = f'https://openapi.seatalk.io/webhook/group/{bot_id}'
    cmt = {"tag": "text",
           "text": {
                    "content": str(content),
                    "mentioned_email_list": [f"{email_user}"],
                    }
           }
    requests.post(url, json=cmt)

