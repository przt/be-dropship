from typing import List
from pathlib import Path

from fastapi import APIRouter, Header, Response
from fastapi.exceptions import HTTPException
from fastapi.responses import FileResponse
from pydantic import ValidationError

from schema.setup import (SupplierIn, SupplierOut, SupplierUpdate, BarangIn, BarangOut, BarangUpdate, CustomerIn,
CustomerOut, CustomerUpdate)
from serv import setup_serv

import os
import urllib.request

# DOWNLOAD PATH
download_dir = str(Path.home()) + '/myco-out/'

dropship_setup = APIRouter()

def sendMessage(text):
    try:
        message = str(text).replace(' ', '%20')
        with urllib.request.urlopen(f"http://localhost:8000/push/{message}") as f:
            print(f.read(100))
    except Exception as e:
        print(e)

# Supplier API
@dropship_setup.get('/supplier/', response_model=List[SupplierOut])
async def suppliers():
    return await setup_serv.get_all_supplier()

@dropship_setup.post('/supplier/', status_code=201)
async def add_supplier(payload: SupplierIn):
    try:
        supplier_id = await setup_serv.add_supplier(payload)
        response = {
            'id': supplier_id,
            **payload.dict()
        }
        return response
    except ValidationError as e:
        return e.json()

@dropship_setup.get('/supplier-by-id/{id}', response_model=SupplierOut)
async def get_a_supplier(id: int):
    return await setup_serv.get_supplier(id)

@dropship_setup.put('/supplier/{id}')
async def update_supplier(id: int, payload: SupplierIn):
    try:
        supplier = await setup_serv.get_supplier(id)
        if not supplier:
            raise HTTPException(status_code=404, detail="supplier not found")

        update_data = payload.dict(exclude_unset=True)
        supplier_in_db = SupplierIn(**supplier)

        updated_supplier = supplier_in_db.copy(update=update_data)
        return await setup_serv.update_supplier(id, updated_supplier)
    except ValidationError as e:
        return e.json()

@dropship_setup.delete('/supplier/{id}')
async def delete_supplier(id: int):
    supplier = await setup_serv.get_supplier(id)
    if not supplier:
        raise HTTPException(status_code=404, detail="supplier setup not found")
    return await setup_serv.delete_supplier(id)

# Barang API
@dropship_setup.get('/barang/', response_model=List[BarangOut])
async def barang():
    return await setup_serv.get_all_barang()

@dropship_setup.post('/barang/', status_code=201)
async def add_barang(payload: BarangIn):
    try:
        barang_id = await setup_serv.add_barang(payload)
        response = {
            'id': barang_id,
            **payload.dict()
        }
        return response
    except ValidationError as e:
        return e.json()

@dropship_setup.get('/barang-by-id/{id}', response_model=BarangOut)
async def get_a_barang(id: int):
    return await setup_serv.get_barang(id)

@dropship_setup.put('/barang/{id}')
async def update_barang(id: int, payload: BarangIn):
    try:
        barang = await setup_serv.get_barang(id)
        if not barang:
            raise HTTPException(status_code=404, detail="barang not found")

        update_data = payload.dict(exclude_unset=True)
        barang_in_db = BarangIn(**barang)

        updated_barang = barang_in_db.copy(update=update_data)
        return await setup_serv.update_barang(id, updated_barang)
    except ValidationError as e:
        return e.json()

@dropship_setup.delete('/barang/{id}')
async def delete_barang(id: int):
    barang = await setup_serv.get_barang(id)
    if not barang:
        raise HTTPException(status_code=404, detail="barang setup not found")
    return await setup_serv.delete_barang(id)

# Customer API
@dropship_setup.get('/customer/', response_model=List[CustomerOut])
async def customer():
    return await setup_serv.get_all_customer()

@dropship_setup.post('/customer/', status_code=201)
async def add_customer(payload: CustomerIn):
    try:
        customer_id = await setup_serv.add_customer(payload)
        response = {
            'id': customer_id,
            **payload.dict()
        }
        return response
    except ValidationError as e:
        return e.json()

@dropship_setup.get('/customer/{id}', response_model=CustomerOut)
async def get_a_customer(id: int):
    return await setup_serv.get_customer(id)

@dropship_setup.put('/customer/{id}')
async def update_customer(id: int, payload: CustomerIn):
    try:
        customer = await setup_serv.get_customer(id)
        if not customer:
            raise HTTPException(status_code=404, detail="customer not found")

        update_data = payload.dict(exclude_unset=True)
        customer_in_db = CustomerIn(**customer)

        updated_customer = customer_in_db.copy(update=update_data)
        return await setup_serv.update_customer(id, updated_customer)
    except ValidationError as e:
        return e.json()

@dropship_setup.delete('/customer/{id}')
async def delete_customer(id: int):
    customer = await setup_serv.get_customer(id)
    if not customer:
        raise HTTPException(status_code=404, detail="customer setup not found")
    return await setup_serv.delete_customer(id)


