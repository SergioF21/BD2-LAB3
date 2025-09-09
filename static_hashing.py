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
        # Solo recorrer los buckets principales
        for i in range(N_MAIN_BUCKETS):
            pos = i * Bucket.SIZE_OF_BUCKET
            self.file.seek(pos)
            bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
            print(f"--- Bucket {i} (principal) ---")
            for record in bucket.records:
                print(record)
            # Recorrer los overflow buckets
            overflow_idx = 1
            next_pos = bucket.next_bucket
            while next_pos != -1:
                self.file.seek(next_pos)
                overflow_bucket = Bucket.unpack(self.file.read(Bucket.SIZE_OF_BUCKET))
                print(f"    --- Overflow {overflow_idx} de Bucket {i} ---")
                for record in overflow_bucket.records:
                    print("    ", record)
                next_pos = overflow_bucket.next_bucket
                overflow_idx += 1
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
    print("=== LABORATORIO 3: Static Hashing ===")
    print(f"BLOCK_FACTOR: {BLOCK_FACTOR}")
    print(f"N_MAIN_BUCKETS: {N_MAIN_BUCKETS}")

    filename = 'datahashing.dat'
    csv_filename = 'sales_dataset_unsorted.csv'

    # Eliminar el archivo si existe
    if os.path.exists(filename):
        os.remove(filename)

    print("\n1. Creando archivo de datos con hashing estático...")
    with open(filename, 'w+b') as file:
        static_hashing = StaticHashing(file)

        print("\n2. Cargando registros desde CSV...")
        records = import_csv(csv_filename)
        if not records:
            print("No se pudieron cargar registros. Terminando.")
            exit()

        test_records = records[:12]
        print("\n3. Registros seleccionados:")
        for i, record in enumerate(test_records, 1):
            print(f" {i}: {record}")

        print("\n4. Insertando registros en el archivo hash...")
        for record in test_records:
            static_hashing.add(record)
        print(f" - Insertados {len(test_records)} registros.")

        print("\n5. Contenido inicial de los buckets:")
        static_hashing.scanAll()

        print("\n6. Pruebas de búsqueda:")
        search_ids = [test_records[0].id_venta, test_records[-1].id_venta, 99999]
        for search_id in search_ids:
            result = static_hashing.search(search_id)
            if result:
                print(f"✓ Encontrado: {result}")
            else:
                print(f"✗ No encontrado: ID {search_id}")

        print("\n7. Pruebas de eliminación:")
        print("Eliminando registros para demostrar diferentes casos...")
        # Eliminar el primer registro (debería quedar el bucket no vacío)
        del_id = test_records[0].id_venta
        print(f"\n--- Eliminando registro ID {del_id} (bucket NO queda vacío) ---")
        static_hashing.delete(del_id)

        # Eliminar todos los registros de un bucket para demostrar bucket vacío
        bucket_id = static_hashing.hash(test_records[1].id_venta)
        print(f"\n--- Eliminando todos los registros del bucket {bucket_id} para demostrar bucket vacío ---")
        for record in test_records:
            if static_hashing.hash(record.id_venta) == bucket_id:
                static_hashing.delete(record.id_venta)

        print("\n--- Contenido después de crear bucket vacío ---")
        static_hashing.scanAll()

        print("\n8. Pruebas de inserción en overflow:")
        print("Insertando registros para llenar un bucket y crear overflow...")
        overflow_records = records[12:20]
        for record in overflow_records:
            static_hashing.add(record)
        print(f" - Insertados {len(overflow_records)} registros adicionales.")

        print("\n9. Contenido final después de overflow:")
        static_hashing.scanAll()

        print("\n=== FIN DEL LABORATORIO ===")
