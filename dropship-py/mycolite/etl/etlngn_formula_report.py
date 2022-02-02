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
path_to_watch = os.path.join(home, 'myco-report/')
# Define database to load
DATABASE_URL = os.getenv('DATABASE_URI')
engine = create_engine(DATABASE_URL)
meta = MetaData()
# Define Messaging
# bc = broadcaster.Publish()


def remove_files():
    for filename in os.listdir(path_to_watch):
        file_path = os.path.join(path_to_watch, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def truncate_table(tablename):
    query = f"truncate table {tablename}"
    engine.execute(sa_text(query).execution_options(autocommit=True))


def getPeriod():
    periodf = pd.read_sql(
        "SELECT period FROM periods WHERE is_active IS TRUE", engine)
    period = periodf['period'][0]
    return period


def savingErrLog(message):
    query = f"INSERT INTO log_error (note, period, logdate) VALUES ('{message}', '{getPeriod()}', '{datetime.now()}')"
    engine.execute(sa_text(query).execution_options(autocommit=True))


def sendMessage(text):
    try:
        message = str(text).replace(' ', '%20')
        with urllib.request.urlopen(f"http://localhost:8000/push/{message}") as f:
            print(f.read(100))
    except Exception as e:
        print(e)


def updateOriAfterMapping(account_code):
    query = f"UPDATE ori_accounts set mapped = true where account_code = '{account_code}'"
    engine.execute(sa_text(query).execution_options(autocommit=True))


def process_file(files):
    # Start extracting when files are found
    for f in files:
        try:
            df = pd.read_excel(os.path.join(path_to_watch, f), sheet_name=1, skiprows=8)
            if 'Neraca' in f:
                print("Memproses file formula neraca")
                sendMessage(f'Loading template neraca...({f})')

                truncate_table('log_error')

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = ['key', 'keterangan', 'formula']
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print(e)

                try:
                    query = f"""DELETE FROM formula_neraca"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True)
                    )
                    df.fillna(0)
                    df.to_sql('formula_neraca', engine,
                              if_exists='append', index=False)
                    print('successfully save valid data of formula neraca')
                except Exception as e:
                    print(e)
            if 'Laba_Rugi' in f:
                print("Memproses file formula laba rugi")
                sendMessage(f'Loading template laba rugi...({f})')

                truncate_table('log_error')

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = ['key', 'keterangan', 'formula']
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print(e)

                try:
                    query = f"""DELETE FROM formula_laba_rugi"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True)
                    )
                    df.fillna(0)
                    df.to_sql('formula_laba_rugi', engine,
                              if_exists='append', index=False)
                    print('successfully save valid data of formula laba rugi')
                except Exception as e:
                    print(e)
        except (Exception, pd.errors.ParserError) as e:
            print('this is from while uploading files : ', e)
    remove_files()


# Working code (without watchdog)
while 1:
    time.sleep(3)
    uploaded = os.listdir(path_to_watch)
    if uploaded:
        #   if not bc.check_conn():
        #       print('reconnecting...')
        #       bc.connect()
        pstart = time.process_time()
        process_file(uploaded)
        pstop = time.process_time()
        text = f'All files are processed in {round(pstop-pstart, 2)} second'
        sendMessage(text)
        print(text)
