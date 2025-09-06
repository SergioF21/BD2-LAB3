# static hashing implementation

# estructura del bucket y la clase que gestiona el archivo de datos
import struct
import os

def import_csv(filename):
    import csv
    records = []
    with open(filename, 'r') as file:
        reader = csv.reader(file, delimiter=';')
        next(reader) # saltar la cabecera
        for row in reader:
            id_venta = int(row[0])
            nombre_producto = row[1]
            cantidad_vendida = int(row[2])
            precio_unitario = float(row[3])
            fecha_venta = row[4]
            record = Record(id_venta, nombre_producto, cantidad_vendida, precio_unitario, fecha_venta)
            records.append(record)
    return records

class Record:
    # id de venta, nombre producto, cantidad vendida, precio unitario, fecha de venta

    FORMAT = 'i30sif10s' # int, 30 chars, int, float, 10 chars
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id_venta:int, nombre_producto:str, cantidad_vendida:int, precio_unitario:float, fecha_venta:str):
        self.id_venta = id_venta
        self.nombre_producto = nombre_producto
        self.cantidad_vendida = cantidad_vendida
        self.precio_unitario = precio_unitario
        self.fecha_venta = fecha_venta
    
    def pack(self)->bytes:
        return struct.pack(self.FORMAT, 
        self.id_venta,
        self.nombre_producto[:30].ljust(30).encode(),
        self.cantidad_vendida,
        self.precio_unitario,
        self.fecha_venta[:10].ljust(10).encode()
        )

    @staticmethod
    def unpack(data: bytes):
        id_venta, nombre_producto, cantidad_vendidad, precio_unitario,fecha_vendida = struct.unpack(Record.FORMAT, data)
        return Record(id_venta, nombre_producto.decode().rstrip(), cantidad_vendidad, precio_unitario, fecha_vendida.decode().rstrip())

    def __str__(self):
        return str(self.id_venta) + '|' + self.nombre_producto + '|' + str(self.cantidad_vendida) + '|' + str(self.precio_unitario) + '|' + self.fecha_venta






BLOCK_FACTOR = 4 # numero de registros por bloque
N_MAIN_BUCKETS = 10 # numero de buckets principales

class Bucket:
    HEADER_FORMAT = 'ii' # size, next_bucket
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_BUCKET = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE_OF_RECORD
    def __init__(self, records = [], next_bucket = -1):
        self.records = records
        self.next_bucket = next_bucket
    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_bucket)
        record_data = b''
        for record in self.records:
            record_data += record.pack()
        i = len(self.records)
        while i < BLOCK_FACTOR:
            record_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1
        return header_data + record_data
    @staticmethod
    def unpack(data : bytes):
        size, next_bucket = struct.unpack(Bucket.HEADER_FORMAT, data[:Bucket.HEADER_SIZE])
        offset = Bucket.HEADER_SIZE
        records = []
        for i in range(size):
            record_data = data[offset: offset + Record.SIZE_OF_RECORD]
            records.append(Record.unpack(record_data))
            offset += Record.SIZE_OF_RECORD
        return Bucket(records, next_bucket)
    
class StaticHashing:
    def __init__(self, file):
        self.file = file
        self.file.seek(0,2)
        filesize = self.file.tell()
        if filesize < N_MAIN_BUCKETS * Bucket.SIZE_OF_BUCKET:
            # inicializar el archivo con buckets vacios
            self.file.seek(0)
            for _ in range(N_MAIN_BUCKETS):
                bucket = Bucket()
                self.file.write(bucket.pack())
    def hash(self, key):
        return key % N_MAIN_BUCKETS
    def add(self, record: Record):
        bucket_index = self.hash(record.id_venta)
        pos = bucket_index * Bucket.SIZE_OF_BUCKET
        self.file.seek(pos)
        bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
        # buscar espacio en el main bucket
        if len(bucket.records) < BLOCK_FACTOR:
            bucket.records.append(record)
            self.file.seek(pos)
            self.file.write(bucket.pack())
            return
        # no hay espacio en el main bucket, buscar en los overflow buckets
        prev_bucket_pos = pos
        while bucket.next_bucket != -1:
            prev_bucket_pos = bucket.next_bucket
            self.file.seek(bucket.next_bucket)
            bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
            if len(bucket.records) < BLOCK_FACTOR:
                bucket.records.append(record)
                self.file.seek(prev_bucket_pos)
                self.file.write(bucket.pack())
                return
        # no hay espacio en los overflow buckets, crear uno nuevo
        new_bucket = Bucket([record])
        self.file.seek(0,2)
        new_bucket_pos = self.file.tell()
        self.file.write(new_bucket.pack())
        # actualizar el puntero del ultimo bucket
        bucket.next_bucket = new_bucket_pos
        self.file.seek(prev_bucket_pos)
        self.file.write(bucket.pack())
    def scanAll(self):
        self.file.seek(0,2)
        filesize = self.file.tell()
        num_buckets = filesize // Bucket.SIZE_OF_BUCKET
        for i in range(num_buckets):
            pos = i * Bucket.SIZE_OF_BUCKET
            self.file.seek(pos)
            bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
            print(f" --- Bucket {i} --- :")
            
            for record in bucket.records:
                print(record)
            # recorrer los overflow buckets
            
            while bucket.next_bucket != -1:
                self.file.seek(bucket.next_bucket)
                bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
                for record in bucket.records:
                    print(record)
            
    def search(self, id_venta):
        bucket_index = self.hash(id_venta)
        pos = bucket_index * Bucket.SIZE_OF_BUCKET
        self.file.seek(pos)
        bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
        for record in bucket.records:
            if record.id_venta == id_venta:
                return record
        while bucket.next_bucket != -1:
            self.file.seek(bucket.next_bucket)
            bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
            for record in bucket.records:
                if record.id_venta == id_venta:
                    return record
        return None
    def delete(self, id_venta):
        bucket_index = self.hash(id_venta)
        pos = bucket_index * Bucket.SIZE_OF_BUCKET
        self.file.seek(pos)
        bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
        # buscar y eliminar en el main bucket
        for i, record in enumerate(bucket.records):
            if record.id_venta == id_venta:
                del bucket.records[i]
                self.file.seek(pos)
                self.file.write(bucket.pack())
                return True
        # buscar y eliminar en los overflow buckets
        prev_bucket_pos = pos
        while bucket.next_bucket != -1:
            prev_bucket_pos = bucket.next_bucket
            self.file.seek(bucket.next_bucket)
            bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
            for i, record in enumerate(bucket.records):
                if record.id_venta == id_venta:
                    del bucket.records[i]
                    self.file.seek(prev_bucket_pos)
                    self.file.write(bucket.pack())
                    return True
        return False
    

if __name__ == "__main__":
    filename = 'datahashing.dat'
    # eliminar el archivo si existe
    if os.path.exists(filename):
        os.remove(filename)
    with open(filename, 'w+b') as file:
        static_hashing = StaticHashing(file)
        records = import_csv('sales_dataset_unsorted.csv')
        x = 100
        print(f"insercion de {x} registros:")
        selected_records = records[:x]
        for record in selected_records:
            static_hashing.add(record)
        print("-----------------------")
        print(f"Todos los {x} registros:")
        static_hashing.scanAll()
        '''
        print("-----------------------")
        print("\nBuscar registro con id_venta=3:")
        record = static_hashing.search(3)
        if record:
            print(record)
        else:
            print("Registro no encontrado")
        print("-----------------------")
        print("\nEliminar registro con id_venta=3")
        if static_hashing.delete(3):
            print("Registro eliminado")
        else:
            print("Registro no encontrado")
        print("-----------------------")
        print("\nTodos los registros despues de la eliminacion:")
        static_hashing.scanAll()
'''
