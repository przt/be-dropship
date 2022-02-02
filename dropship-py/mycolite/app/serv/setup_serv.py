from fastapi.params import Query
from orm.db import database, engine
from orm.db_setup import supplier, barang, customer

from schema.setup import SupplierIn, SupplierOut, SupplierUpdate
from schema.setup import BarangIn, BarangOut, BarangUpdate, CustomerIn, CustomerOut, CustomerUpdate

from pathlib import Path
import urllib.request
import pandas as pd
import logging
import os

# DOWNLOAD PATH
download_dir = str(Path.home()) + '/myco-out/'
os.makedirs(download_dir, exist_ok=True)

def sendMessage(text):
    try:
        message = str(text).replace(' ', '%20')
        with urllib.request.urlopen(f"http://localhost:8000/push/{message}") as f:
            print(f.read(100))
    except Exception as e:
        print(e)

# Supplier
async def add_supplier(payload: SupplierIn):
    query = supplier.insert().values(**payload.dict())
    return await database.execute(query=query)

async def get_all_supplier():
    query = supplier.select()
    return await database.fetch_all(query=query)

async def get_supplier(id):
    query = supplier.select(supplier.c.id==id)
    return await database.fetch_one(query=query)

async def delete_supplier(id: int):
    query = supplier.delete().where(supplier.c.id==id)
    return await database.execute(query=query)

async def update_supplier(id: int, payload: SupplierIn):
    query = (
        supplier
        .update()
        .where(supplier.c.id == id)
        .values(**payload.dict())
    )
    return await database.execute(query=query)

# Barang

async def add_barang(payload: BarangIn):
    query = barang.insert().values(**payload.dict())
    return await database.execute(query=query)

async def get_all_barang():
    query = barang.select()
    return await database.fetch_all(query=query)

async def get_barang(id):
    query = barang.select(barang.c.id==id)
    return await database.fetch_one(query=query)

async def delete_barang(id: int):
    query = barang.delete().where(barang.c.id==id)
    return await database.execute(query=query)

async def update_barang(id: int, payload: BarangIn):
    query = (
        barang
        .update()
        .where(barang.c.id == id)
        .values(**payload.dict())
    )
    return await database.execute(query=query)

# Customer

async def add_customer(payload: CustomerIn):
    query = customer.insert().values(**payload.dict())
    return await database.execute(query=query)

async def get_all_customer():
    query = customer.select()
    return await database.fetch_all(query=query)

async def get_customer(id):
    query = customer.select(customer.c.id==id)
    return await database.fetch_one(query=query)

async def delete_customer(id: int):
    query = customer.delete().where(customer.c.id==id)
    return await database.execute(query=query)

async def update_customer(id: int, payload: CustomerIn):
    query = (
        customer
        .update()
        .where(customer.c.id == id)
        .values(**payload.dict())
    )
    return await database.execute(query=query)