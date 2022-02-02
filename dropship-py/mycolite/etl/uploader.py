from datetime import datetime

import contextlib
import os
import shutil
import time
import urllib.request
from pathlib import Path

import pandas as pd
import numpy as np
from sqlalchemy import MetaData, create_engine
from sqlalchemy import text as sa_text

# import broadcaster

print('Waiting for uploads...')
# Define path to process
home = str(Path.home())
path_to_watch = os.path.join(home, 'myco-in')
# Define database to load
DATABASE_URL = os.getenv('DATABASE_URI')
engine = create_engine(DATABASE_URL)
meta = MetaData()
# Define Messaging
# bc = broadcaster.Publish()

def upload_file(files):
    df = pd.read_excel(os.path.join(path_to_watch))
    df.to_sql('companies', engine, index=True, index_label='id', if_exists='replace')
    print("upload success")

while 1:
  time.sleep(3)
  uploaded = os.listdir(path_to_watch)
  if uploaded:
    #   if not bc.check_conn():
    #       print('reconnecting...')
    #       bc.connect()
    pstart = time.process_time()
    upload_file(uploaded)
    pstop = time.process_time()
    text = f'All files are processed in {round(pstop-pstart, 2)} second'
    print(text)