---
layout: post
title: "Control tower"
subtitle: "Automatization of control tower to supply"
background: '/img/posts/web-scraping/back-ground-ws.jpg'
---

# Control Tower


```python
#!pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
```


```python
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import snowflake.connector
import requests
import io
from openpyxl import load_workbook
from openpyxl.styles import PatternFill
from datetime import datetime
from dateutil.relativedelta import relativedelta, MO, TU, WE, TH, FR, SA, SU

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from googleapiclient.http import MediaFileUpload
```


```python
conn = snowflake.connector.connect(user='', 
                                   authenticator='', 
                                   account='', 
                                   warehouse="",
                                   database="")
```

## Descarga WL