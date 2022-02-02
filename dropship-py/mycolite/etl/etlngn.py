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
path_to_watch = os.path.join(home, 'myco-in/')
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
            df = pd.read_excel(os.path.join(path_to_watch, f))
            if 'Company' in f:
                print("Memproses file company")
                sendMessage(f'Loading Original Account...({f}) ')

                truncate_table('log_error')

                # Check empty values
                valid = True
                dfna = df.isna().sum()
                for k, v in dfna.items():
                    if v > 0:
                        messg = f"Company ({f}): there are {v} empty values in {k}. Auto removed by system!"
                        savingErrLog(messg)
                        valid = False
                df = df.dropna(subset=['code', 'name'])

                df['code'] = df['code'].str.upper()
                df['name'] = df['name'].str.upper()

                sqldf = pd.read_sql(
                    f"SELECT * FROM companies", engine
                )
                if sqldf.empty == False:
                    df = pd.concat([df, sqldf])

                # Check duplicates
                dfdup = df.duplicated(subset=['code']).sum()
                if dfdup > 0:
                    messg = f"Company ({f}): {dfdup} duplicate(s) were found. The first {dfdup} row(s) preserved."
                    print(messg)
                    savingErrLog(messg)
                    valid = False
                df = df.drop_duplicates(subset=['code'])

                if not valid:
                    sendMessage(
                        f"Warning: There are invalid data in {f}. Please check Error Log.")

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = ['code', 'name',
                                    'note', 'parent', 'currency']
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print(e)

                try:
                    query = f"""DELETE FROM companies"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True)
                    )
                    df.to_sql('companies', engine,
                              if_exists='append', index=False)
                    sendMessage('successfully save valid data of companies')
                except Exception as e:
                    print(e)
            if 'Coa_Original' in f:
                print("Memproses file original coa")
                sendMessage(f'Loading Original Account...({f})')

                truncate_table('log_error')

                # Check empty values
                valid = True
                dfna = df.isna().sum()
                for k, v in dfna.items():
                    if v > 0:
                        messg = f"Original Account ({f}): there are {v} empty values in {k}. Auto removed by system!"
                        savingErrLog(messg)
                        valid = False
                df = df.dropna(
                    subset=['account_code', 'account_name', 'company'])
                # print("\n\n------After Null Check------")
                # print(df)

                df['account_code'] = df.account_code.astype(str)
                df['account_code'] = df['account_code'].str.upper()
                df['account_name'] = df['account_name'].str.upper()
                df['company'] = df['company'].str.upper()

                # Check ori company existence
                comdf = pd.read_sql("SELECT code FROM companies", engine)
                if not comdf.empty:
                    df.reset_index()
                    isComMatch = True
                    for idx, val in df['company'].items():
                        for cdx, cval in comdf['code'].items():
                            if val == cval:
                                isComMatch = True
                                break
                            else:
                                isComMatch = False
                        if isComMatch == False:
                            accode = df.loc[idx, 'account_code']
                            messg = f"Original Account ({f}): company {val} is unregistered at row {i} (acc. code: {accode}). Auto removed by system!"
                            savingErrLog(messg)
                            df = df[df.account_code != accode]
                            valid = False
                else:
                    messg = f"Original Account: companies are not there! Please register the companies first."
                    savingErrLog(messg)
                    valid = False
                # print("\n\n------After Company Exist Check------")
                # print(df)

                current_company = df.loc[1, 'company']
                sqldf = pd.read_sql(
                    f"SELECT * FROM ori_accounts WHERE company = '{current_company}'", engine)
                if sqldf.empty == False:
                    df = pd.concat([df, sqldf])

                # Check duplicates
                dfdup = df.duplicated(subset=['account_code', 'company']).sum()
                if dfdup > 0:
                    messg = f"Original Account ({f}): {dfdup} duplicate(s) were found. The first {dfdup} row(s) preserved."
                    print(messg)
                    savingErrLog(messg)
                    valid = False
                df = df.drop_duplicates(subset=['account_code', 'company'])
                # print("\n\n------After Duplication Check------")
                # print(df)

                if not valid:
                    sendMessage(
                        f"Warning: There are invalid data in {f}. Please check Error Log.")

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = ['company', 'account_code', 'account_name',
                                    'account_type', 'account_indicator']
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print('this is from expected cols' + e)
                try:
                    query = f"""DELETE FROM ori_accounts WHERE company = '{current_company}'"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True))
                    df.drop_duplicates(subset=['account_code', 'company'])
                    df.to_sql('ori_accounts', engine,
                              if_exists='append', index=False)
                    sendMessage(
                        'successfully save valid data of original account')
                    mappingoridf = pd.read_sql(
                        f"SELECT ori_account FROM mapping_accounts WHERE company = '{current_company}'", engine)
                    for idx, val in mappingoridf['ori_account'].items():
                        query = f""" UPDATE ori_accounts SET mapped = true WHERE account_code = '{val}' """
                        engine.execute(
                            sa_text(query).execution_options(autocommit=True))
                except Exception as e:
                    print(e)
            if 'Coa_Consol' in f:
                print("Memproses file consol coa")
                sendMessage(f'Loading consol account... ({f}) ')

                truncate_table('log_error')

                # Check empty values
                valid = True
                dfna = df.isna().sum()
                for k, v in dfna.items():
                    if v > 0:
                        messg = f"Consol Account ({f}): there are {v} empty values in {k}. Auto removed by system "
                        savingErrLog(messg)
                        valid = False
                df = df.dropna(
                    subset=['account_code', 'account_name', 'company_parent'])

                df['account_code'] = df.account_code.astype(str)
                df['account_code'] = df['account_code'].str.upper()
                df['account_name'] = df['account_name'].str.upper()

                sqldf = pd.read_sql(
                    f"SELECT * FROM consol_accounts", engine
                )
                if sqldf.empty == False:
                    df = pd.concat([df, sqldf])

                # Check duplicates
                dfdup = df.duplicated(
                    subset=['company_parent', 'account_code']).sum()
                if dfdup > 0:
                    messg = f"Consol Accounts ({f}): {dfdup} duplicate(s) were found. The first {dfdup} row(s) preserved."
                    print(messg)
                    savingErrLog(messg)
                    valid = False
                df = df.drop_duplicates(
                    subset=['company_parent', 'account_code'])

                if not valid:
                    sendMessage(
                        f"Warning: There are invalid data in {f}. Please check Error Log.")

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = ['account_code', 'account_name', 'parent',
                                    'posting_account', 'account_type', 'company_parent']
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print(e)

                try:
                    query = f"""DELETE FROM consol_accounts"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True)
                    )
                    df.to_sql('consol_accounts', engine,
                              if_exists='append', index=False)
                    sendMessage(
                        'successfully save valid data of consol account')
                except Exception as e:
                    print(e)

            if 'Coa_Mapping' in f:
                print('Memproses file coa mapping')
                sendMessage(f'Loading Account Mapping...({f})')

                truncate_table('log_error')

                # check empty values
                valid = True
                dfna = df.isna().sum()
                for k, v in dfna.items():
                    if v > 0:
                        messg = f"Mapping account ({f}): there are {v} empty values in {k}. Auto removed by system!"
                        savingErrLog(messg)
                        valid = False
                df = df.dropna(
                    subset=['ori_account', 'consol_account',
                            'company', 'company_parent']
                )

                df['ori_account'] = df.ori_account.astype(str)
                df['ori_account'] = df['ori_account'].str.upper()
                df['consol_account'] = df.consol_account.astype(str)
                df['consol_account'] = df['consol_account'].str.upper()

                # check existence of ori account
                oridf = pd.read_sql(
                    "SELECT account_code FROM ori_accounts", engine)
                if not oridf.empty:
                    df.reset_index()
                    isOriExist = True
                    for idx, val in df['ori_account'].items():
                        for cdx, cval in oridf['account_code'].items():
                            if val == cval:
                                isOriExist = True
                                break
                            else:
                                isOriExist = False
                        if isOriExist == False:
                            oriAcc = df.loc[idx, 'ori_account']
                            messg = f"error Mapping Account ({f}): original account {val} is not exist!. Mapping account {oriAcc} is removed..."
                            print(messg)
                            savingErrLog(messg)
                            sendMessage(messg)
                            df = df[df.ori_account != oriAcc]
                            valid = False
                        else:
                            updateOriAfterMapping(df.loc[idx, 'ori_account'])
                else:
                    messg = f"Mapping Account: original account is not exist!. Insert original account first"
                    savingErrLog(messg)
                    valid = False

                # check existence of consol account
                consoldf = pd.read_sql(
                    "SELECT account_code FROM consol_accounts", engine)
                if not consoldf.empty:
                    df.reset_index()
                    isConsolExist = True
                    for idx, val in df['consol_account'].items():
                        for cdx, cval in consoldf['account_code'].items():
                            if val == cval:
                                isConsolExist = True
                                break
                            else:
                                isConsolExist = False
                        if isConsolExist == False:
                            consolAcc = df.loc[idx, 'consol_account']
                            messg = f"error Mapping Account ({f}): consol account {val} is not exist!. Mapping account {consolAcc} is removed..."
                            print(messg)
                            savingErrLog(messg)
                            sendMessage(messg)
                            df = df[df.consol_account != consolAcc]
                            valid = False
                else:
                    messg = f"Mapping Account: consol account is not exist!. Insert consol account first"
                    savingErrLog(messg)
                    valid = False

                current_company = df.loc[1, 'company']
                sqldf = pd.read_sql(
                    f"SELECT * FROM mapping_accounts WHERE company = '{current_company}'", engine
                )
                if sqldf.empty == False:
                    df = pd.concat([df, sqldf])

                # Check duplicates
                dfdup = df.duplicated(subset=['company', 'ori_account']).sum()
                if dfdup > 0:
                    messg = f"Error Mapping Account ({f}): {dfdup} duplicate(s) were found."
                    print(messg)
                    savingErrLog(messg)
                    valid = False
                df = df.drop_duplicates(subset=['company', 'ori_account'])

                if not valid:
                    sendMessage(
                        f"Warning: There are invalid data in {f}. Please check Error Log.")

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = [
                        'ori_account', 'consol_account', 'company', 'company_parent']
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print(e)

                try:
                    query = f"""DELETE FROM mapping_accounts WHERE company = '{current_company}'"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True)
                    )
                    df.to_sql('mapping_accounts', engine,
                              if_exists='append', index=False)
                    sendMessage(
                        'successfully save valid data of mapping account')
                except Exception as e:
                    print(e)
            if 'Matrix' in f:
                sendMessage(f'Loading Elimination Matrix...({f})')

                # Check empty values
                valid = True
                dfna = df.isna().sum()
                for k, v in dfna.items():
                    if k == 'group_no' and k == 'version':
                        if v > 0:
                            messg = f"Matrix ({f}): there are {v} empty values in {k.lstrip()}. Auto removed by system!"
                            savingErrLog(messg)
                            valid = False
                df = df.dropna(subset=['group_no', 'version'])

                # Join the existing data
                df_version = df.loc[1, 'version']
                sqldf = pd.read_sql(
                    f"SELECT * FROM matrix WHERE version = '{df_version}'", engine)
                if not sqldf.empty:
                    df = pd.concat([df, sqldf])

                # Check duplicates
                dups = df.duplicated(subset=[
                                     'company_code1', 'coa_consol_code1', 'company_code2', 'coa_consol_code2']).sum()
                if dups > 0:
                    messg = f"Matrix ({f}): {dups} duplicate(s) found. The first data is preserved"
                    savingErrLog(messg)
                    valid = False

                df = df.drop_duplicates(
                    subset=['company_code1', 'coa_consol_code1', 'company_code2', 'coa_consol_code2'])

                if not valid:
                    sendMessage(
                        f"Warning: There are invalid data in {f}. The system reject this file. Please check Error Log.")

                try:
                    query = f"DELETE FROM matrix WHERE version = '{df_version}'"
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True))
                    for i in range(len(df)):
                        group_no = df.loc[i, 'group_no']
                        elimination_group = "-" if np.nan_to_num(
                            df.loc[i, 'elimination_group']) == 0.0 else df.loc[i, 'elimination_group']
                        company_code1 = "-" if np.nan_to_num(
                            df.loc[i, 'company_code1']) == 0.0 else df.loc[i, 'company_code1']
                        coa_consol_code1 = "-" if np.nan_to_num(
                            df.loc[i, 'coa_consol_code1']) == 0.0 else df.loc[i, 'coa_consol_code1']
                        company_code2 = "-" if np.nan_to_num(
                            df.loc[i, 'company_code2']) == 0.0 else df.loc[i, 'company_code2']
                        coa_consol_code2 = "-" if np.nan_to_num(
                            df.loc[i, 'coa_consol_code2']) == 0.0 else df.loc[i, 'coa_consol_code2']
                        version = df.loc[i, 'version']
                        query = f"""INSERT INTO matrix (group_no, elimination_group, company_code1, coa_consol_code1, 
                        company_code2, coa_consol_code2, version) VALUES ('{group_no}', '{elimination_group}', '{company_code1}', 
                        '{coa_consol_code1}', '{company_code2}', '{coa_consol_code2}', '{version}')"""
                        engine.execute(
                            sa_text(query).execution_options(autocommit=True))
                    sendMessage(
                        'successfully save valid data of original account')
                except Exception as e:
                    sendMessage(
                        f'Error: There is something wrong while uploading {f}. Please inform your admin.')
                    print("this is exception : ", e)
            if 'TB' in f:
                sendMessage(f'Loading TB...({f})')

                dbactive_period = ''
                active_period = df.loc[1, 'period']
                current_company = df.loc[1, 'company']

                rs = engine.execute(
                    f"select * from periods where is_active is true")
                if rs.returns_rows:
                    for row in rs:
                        dbactive_period = row['period']

                if dbactive_period == '' or active_period != dbactive_period:
                    sendMessage(
                        f"You're uploading period {active_period} while current active period is {dbactive_period}.This file is rejected")
                    return
                else:
                    # Check empty values
                    valid = True
                    dfna = df.isna().sum()
                    for k, v in dfna.items():
                        if v > 0:
                            messg = f"Matrix ({f}): there are {v} empty values in {k.lstrip()}. Auto removed by system!"
                            savingErrLog(messg)
                            valid = False
                    df = df.dropna()

                    df['account_code'] = df.account_code.astype(str)
                    df['account_code'] = df['account_code'].str.upper()

                    # Check existence of account
                    mapcom = df.loc[1, 'company']
                    tb_df = pd.concat([df['company'], df['account_code']], axis=1, keys=[
                                      'company', 'account_code'])
                    sqloridf = pd.read_sql(
                        f"SELECT company, account_code FROM ori_accounts WHERE company = '{mapcom}'", engine)
                    orimerge = pd.merge(
                        tb_df, sqloridf, how='left', indicator='Exist')
                    orimerge['Exist'] = np.where(
                        orimerge.Exist == 'both', True, False)
                    for i in range(len(orimerge)):
                        acc_code = orimerge.loc[i, 'account_code']
                        company = orimerge.loc[i, 'company']
                        if orimerge.loc[i, 'Exist'] == False:
                            messg = f"{f}: {acc_code} or {company} are not exist in table original account. Auto add new original account"
                            print('New Account: ', acc_code)
                            savingErrLog(messg)
                            query = f"INSERT INTO ori_accounts (company, account_code, account_name, account_type, account_indicator, auto_created) VALUES ('{company}', '{acc_code}', '', '', '', true)"
                            engine.execute(
                                sa_text(query).execution_options(autocommit=True))
                            valid = False

                    # Check duplicates
                    dups = df.duplicated(
                        subset=['company', 'account_code', 'period']).sum()
                    if dups > 0:
                        messg = f"Trial Balance ({f}): {dups} duplicate(s) found. The first data is preserved"
                        savingErrLog(messg)
                        valid = False
                    df = df.drop_duplicates(
                        subset=['company', 'account_code', 'period'])

                    if not valid:
                        sendMessage(
                            f"Warning: There are invalid data in {f}. Please check Error Log.")

                    try:
                        df = df.reset_index(drop=False)
                        filecols = df.columns.tolist()
                        expectedcols = ['company', 'period',
                                        'account_code', 'amount']
                        for fc in filecols:
                            if fc not in expectedcols:
                                df = df.drop(fc, axis=1)
                    except Exception as e:
                        print(e)

                    try:
                        query = f"DELETE FROM trial_balance WHERE period = '{active_period}' and company = '{current_company}'"
                        engine.execute(
                            sa_text(query).execution_options(autocommit=True))
                        df.to_sql('trial_balance', engine,
                                  if_exists='append', index=False)
                    except Exception as e:
                        sendMessage(
                            f'Error: There is something wrong while uploading {f}. Please inform your admin.')
                        print(e)
            if 'Journal' in f:
                sendMessage(f'Loading Journal...({f})')

                truncate_table('log_error')

                # Set active period
                active_period = ''
                rs = engine.execute(
                    f"select * from periods where is_active is true"
                )
                if rs.returns_rows:
                    for row in rs:
                        active_period = row['period']

                if active_period == '':
                    sendMessage(
                        f"No period is active. Please activate a period."
                    )
                    return
                
                # Check empty values
                valid = True
                dfna = df.isna().sum()
                for k, v in dfna.items():
                    if v > 0:
                        messg = f"Journal ({f}): there are {v} empty values in {k.lstrip()}. Auto fill empty string by system! "
                        savingErrLog(messg)
                
                df['account_code'] = df.account_code.astype(str)
                df['account_code'] = df['account_code'].str.upper()
                df['journal_number'] = df.journal_number.astype(str)
                df['journal_number'] = df['journal_number'].str.upper()
                df['company'] = df.company.astype(str)
                df['company'] = df['company'].str.upper()

                sqldf = pd.read_sql(
                    f"SELECT * FROM journal WHERE period = '{active_period}'", engine
                )
                if sqldf.empty == False:
                    df = pd.concat([df, sqldf])
                
                # Check existence of consol account
                consoldf = pd.read_sql(
                    "SELECT account_code FROM consol_accounts", engine
                )
                if not consoldf.empty:
                    df.reset_index()
                    isConsolExist = True
                    for idx, val in df['account_code'].items():
                        for cdx, cval in consoldf['account_code'].items():
                            if val == cval:
                                isConsolExist = True
                                break
                            else:
                                isConsolExist = False
                        if isConsolExist == False:
                            consolAcc = df.loc[idx, 'account_code']
                            messg = f"error journal ({f}): consol account is not exist {val} is not exist!. Journal {consolAcc} is removed... "
                            print(messg)
                            savingErrLog(messg)
                            sendMessage(messg)
                            df = df[df.account_code != consolAcc]
                            valid = False
                else:
                    messg = f"Journal : consol account for journal is not exist!. Insert consol account first "
                    savingErrLog(messg)
                    valid = False
            
                current_company = df.loc[1, 'company']
                sqldf = pd.read_sql(
                    f"SELECT * FROM journal WHERE period = '{active_period}'", engine
                )
                if sqldf.empty == False:
                    df = pd.concat([df, sqldf])
                
                # Check duplicates
                dfdup = df.duplicated(subset=['journal_number', 'account_code']).sum()
                if dfdup > 0:
                    messg = f"Error Journal ({f}): {dfdup} duplicate(s) were found. "
                    print(messg)
                    savingErrLog(messg)
                    valid = False
                df = df.drop_duplicates(subset=['journal_number', 'account_code'])

                if not valid:
                    sendMessage(
                        f"Warning: There are invalid data in {f}. Please check error log. "
                    )

                try:
                    df = df.reset_index(drop=False)
                    filecols = df.columns.tolist()
                    expectedcols = [
                        'journal_number', 'description', 'company', 'account_code', 
                        'account_name', 'debit', 'credit', 'journal_type', 'correction_group'
                    ]
                    for fc in filecols:
                        if fc not in expectedcols:
                            df = df.drop(fc, axis=1)
                except Exception as e:
                    print(e)
                
                try:
                    query = f"""DELETE FROM journal"""
                    engine.execute(
                        sa_text(query).execution_options(autocommit=True)
                    )
                    df['period'] = active_period
                    df['account_name'] = ''
                    df = df.fillna(0)
                    df.to_sql('journal', engine, if_exists='append', index=False)
                    sendMessage(
                        'succesfully save valid data of journal'
                    )
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
