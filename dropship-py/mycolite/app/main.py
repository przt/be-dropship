from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from api.setup_api import dropship_setup
from orm.db import database, engine, metadata

import uvicorn
import logging

metadata.create_all(engine)

app = FastAPI()

origins = ['*']

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

@app.on_event("startup")
async def startup():
    await database.connect()
    # Bootstraping
    # account_types = await database.fetch_all(query="SELECT * FROM account_types")
    # if len(account_types) == 0:
    #     logging.info('Account types not found. creating...')
    #     sqls = [
    #         "insert into account_types (name) values ('BS Activa')",
    #         "insert into account_types (name) values ('BS Pasiva')",
    #         "insert into account_types (name) values ('PL Expense')",
    #         "insert into account_types (name) values ('PL Revenue')",
    #     ]
    #     for sql in sqls:
    #         await database.execute(query=sql)

    # journal_types = await database.fetch_all(query="SELECT * FROM journal_types")
    # if len(journal_types) == 0:
    #     logging.info('JT not found. creating...')
    #     sqls = [
    #         "insert into journal_types (journal_type, name) values ('1', 'Elimination')", 
    #         "insert into journal_types (journal_type, name) values ('2', 'Elimination Correction')", 
    #         "insert into journal_types (journal_type, name) values ('3', 'Adustment')", 
    #         "insert into journal_types (journal_type, name) values ('4', 'Consolidation Journal')"
    #     ]
    #     for sql in sqls:
    #         await database.execute(query=sql)

    # periods = await database.fetch_all(query="SELECT * FROM periods")
    # if len(periods) == 0:
    #     logging.info('Period not found. creating...')
    #     sqls = [
    #         "insert into periods (period, start_op, end_op, is_active) values ('2019-01', '', '', true)"
    #     ]
    #     for sql in sqls:
    #         await database.execute(query=sql)

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

app.include_router(dropship_setup)
# app.include_router(myco_setup)
# app.include_router(myco_staging)
# app.include_router(myco_privileges)
# app.include_router(myco_report)
# app.include_router(myco_shield)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0")
