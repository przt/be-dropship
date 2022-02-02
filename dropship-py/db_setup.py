from sqlalchemy import (Column, Integer, String, Table, Boolean, Numeric, Date)
from datetime import datetime
from orm.db import engine, metadata, database

supplier = Table (
    'supplier',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('nama_supplier', String(50), nullable=False),
    Column('nomor_telfon', String(15)),
    Column('alamat', String, nullable=False),
 
)
barang = Table (
    'barang',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('nama_barang', String(50), nullable=False),
    Column('nama_supplier', String(50), nullable=False),
    Column('kategori', String),
    Column('satuan', String(15)),
    Column('harga_pokok', Numeric),
    Column('harga_jual', Numeric),
 
)

customer = Table (
    'customer',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('nama_customer', String(50), nullable=False),
    Column('nomor_telfon', String(15)),
    Column('alamat', String, nullable=False),
 
)
transaksi = Table (
    'transaksi',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('nomor_transaksi', String(50), nullable=False, unique=True),
    Column('nama_customer', String(50), nullable=False),
    Column('tgl_transaksi', String(20)),
    Column('total_transaksi', Numeric, nullable=False)
 
)
detail_transaksi = Table (
    'detail_transaksi',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('nomor_transaksi', String(50), nullable=False, unique=True),
    Column('nama_barang', String(50), nullable=False),
    Column('qty', Numeric),
)


