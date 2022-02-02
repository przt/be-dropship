from typing import Optional
from pydantic import BaseModel

# Supplier
class SupplierIn(BaseModel):
    nama_supplier: str
    nomor_telfon: str
    alamat: str

class SupplierOut(BaseModel):
    id: int

class SupplierUpdate(SupplierIn):
    nama_supplier: Optional[str] = None
    nomor_telfon: Optional[str] = None
    alamat: Optional[str] = None

# Barang 
class BarangIn(BaseModel):
    nama_barang: str
    nama_supplier: str
    kategori: str
    satuan: str
    harga_pokok: float
    harga_jual: float

class BarangOut(BaseModel):
    id: int

class BarangUpdate(BarangIn):
    nama_barang: Optional[str] = None
    nama_supplier: Optional[str] = None
    kategori: Optional[str] = None
    satuan: Optional[str] = None
    harga_pokok: Optional[float] = None
    harga_jual: Optional[float] = None

# Customer
class CustomerIn(BaseModel):
    nama_customer: str
    nomor_telfon: str
    alamat: str

class CustomerOut(BaseModel):
    id: int

class CustomerUpdate(CustomerIn):
    nama_supplier: Optional[str] = None
    nomor_telfon: Optional[str] = None
    alamat: Optional[str] = None

# Transaksi
class TransaksiIn(BaseModel):
    nomor_transaksi: str
    nama_customer: str
    tgl_transaksi: str
    total_transaksi: float

class TransaksiOut(BaseModel):
    id: int

class TransaksiUpdate(TransaksiIn):
    nomor_transaksi: Optional[str]
    nama_customer: Optional[str]
    tgl_transaksi: Optional[str]
    total_transaksi: Optional[float]

# Detail Transaksi
class DetailTransaksiIn(BaseModel):
    nomor_transaksi: str
    nama_barang: str
    qty: float

class DetailTransaksiOut(BaseModel):
    id: str

class DetailTransaksiUpdate(DetailTransaksiIn):
    nomor_transaksi: Optional[str]
    nama_barang: Optional[str]
    qty: Optional[float]

