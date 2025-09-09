import struct
import csv
import os

BLOCK_FACTOR = 3
MAX_INDEX_ENTRIES = 5

class Record:
    FORMAT = 'i30s5sff10s'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id_venta: int, nombre_producto: str, cantidad:int, precio: float, fecha: str= ""):
        self.id_venta = id_venta
        self.nombre_producto = nombre_producto[:29]
        self.cantidad = cantidad
        self.precio = precio
        self.fecha = fecha[:9]
    
    def pack(self):
        cantidad_str = str(self.cantidad).encode('utf-8')[:4]
        return struct.pack(self.FORMAT,
        self.id_venta,
        self.nombre_producto.encode('utf-8'),
        cantidad_str,
        float(self.cantidad),
        self.precio,
        self.fecha.ljust(10).encode('utf-8')
        )

    @staticmethod
    def unpack(data):
        unpacked = struct.unpack(Record.FORMAT, data)
        return Record(
            unpacked[0],
            unpacked[1].decode('utf-8').rstrip('\x00'),
            int(unpacked[3]),
            unpacked[4],
            unpacked[5].decode('utf-8').rstrip('\x00')
        )

    def __str__(self):
        return f"ID: {self.id_venta} - {self.nombre_producto} - Cant: {self.cantidad}, ${self.precio}"

class Page:
    SIZE_OF_PAGE = 200

    def __init__(self, records = None, next_page = -1):
        self.records = records if records is not None else []
        self.next_page = next_page

    def pack(self):
        data = bytearray(self.SIZE_OF_PAGE)
        offset = 0

        struct.pack_into('i', data, offset, len(self.records))
        offset += 4

        for record in self.records:
            if offset + Record.SIZE_OF_RECORD <= self.SIZE_OF_PAGE - 4:
                record_data = record.pack()
                data[offset:offset + Record.SIZE_OF_RECORD] = record_data
                offset += Record.SIZE_OF_RECORD
        
        struct.pack_into('i', data, self.SIZE_OF_PAGE - 4, self.next_page)

        return bytes(data)

    @staticmethod
    def unpack(data):
        num_records = struct.unpack_from('i', data, 0)[0]
        offset = 4

        records = []
        for _ in range(num_records):
            if offset + Record.SIZE_OF_RECORD <= len(data) - 4:
                record_data = data[offset:offset + Record.SIZE_OF_RECORD]
                records.append(Record.unpack(record_data))
                offset += Record.SIZE_OF_RECORD
        
        next_page = struct.unpack_from('i', data, len(data) - 4)[0]

        return Page(records, next_page)

   
class DataFile:
    def __init__(self, filename: str, indexname: str = None):
        self.filename = filename
        self.index = IndexFile(indexname) if indexname else None

    def build_initial_file(self, sorted_records):
        if os.path.exists(self.filename):
            os.remove(self.filename)
        
        if self.index:
            self.index.index = {}
        
        with open(self.filename, 'wb') as file:
            current_page_records = []

            for record in sorted_records:
                current_page_records.append(record)

                if len(current_page_records) == BLOCK_FACTOR:
                    page = Page(current_page_records)
                    page_position = file.tell()
                    file.write(page.pack())

                    if self.index:
                        first_record_id = current_page_records[0].id_venta
                        self.index.add(first_record_id, page_position)

                    current_page_records = []
            
            if current_page_records:
                page = Page(current_page_records)
                page_position = file.tell()
                file.write(page.pack())

                if self.index:
                    first_record_id = current_page_records[0].id_venta
                    self.index.add(first_record_id, page_position)

        if self.index:
            self.index.save_index()
        
        print(f"Archivo inicial construido con {len(sorted_records)} registros ordenados.")

    def add(self, record: Record):
        if not os.path.exists(self.filename):
            print("Error: Debe construir el archivo inicial primero con build_initial_file().")
            return
        
        with open(self.filename, 'r+b') as file:
            target_position = self._find_target_position(file, record.id_venta)

            if self._try_insert_in_page(file, target_position, record):
                print(" - Registro insertado en la página existente.")
                return
            
            if self.index and not self.index.is_full():
                print("CASO 1: División de página (hay espacio en índice)")
                self._handle_page_split(file, target_position, record)
            
            else:
                print("CASO 2: Encadenamiento de página (índice lleno)")
                self._handle_page_chain(file, target_position, record)
    
    def _find_target_position(self, file, key):
        if not self.index:
            file.seek(0, 2)
            size = file.tell()
            return max(0, size - Page.SIZE_OF_PAGE) if size > 0 else 0
        return self.index.find_page_for_key(key)
        
    def _try_insert_in_page(self, file, position, record):
        current_pos = position
        while current_pos != -1:
            file.seek(current_pos)
            page_data = file.read(Page.SIZE_OF_PAGE)
            page = Page.unpack(page_data)

            should_insert_here = self._should_insert_in_this_page(page, record)

            if should_insert_here and len(page.records) < BLOCK_FACTOR:
                return self._insert_record_in_page(file, current_pos, page, record)
            
            if should_insert_here:
                return False
            
            current_pos = page.next_page

        return False
    
    def _should_insert_in_this_page(self, page, record):
        if not page.records:
            return True
        min_id = min(r.id_venta for r in page.records)
        max_id = max(r.id_venta for r in page.records)
        return min_id <= record.id_venta <= max_id or record.id_venta < min_id

    def _insert_record_in_page(self, file, position, page, record):
        old_first_id = page.records[0].id_venta if page.records else None

        page.records.append(record)
        page.records.sort(key=lambda x: x.id_venta)
        
        file.seek(position)
        file.write(page.pack())

        new_first_id = page.records[0].id_venta
        if self.index and old_first_id != new_first_id:
            if old_first_id is not None and old_first_id in self.index.index:
                del self.index.index[old_first_id]
            self.index.add(new_first_id, position)
            self.index.save_index()
        return True
    
    def _handle_page_split(self, file, position, record):
        file.seek(position)
        page_data = file.read(Page.SIZE_OF_PAGE)
        page = Page.unpack(page_data)

        all_records = page.records + [record]
        all_records.sort(key=lambda x: x.id_venta)

        mid = BLOCK_FACTOR // 2 + 1
        first_half = all_records[:mid]
        second_half = all_records[mid:]

        updated_page = Page(first_half)
        file.seek(position)
        file.write(updated_page.pack())

        file.seek(0, 2)
        new_position = file.tell()
        new_page = Page(second_half)
        file.write(new_page.pack())

        if self.index:
            old_first_id = page.records[0].id_venta if page.records else None
            new_first_id = updated_page.records[0].id_venta

            if old_first_id and old_first_id != new_first_id:
                if old_first_id in self.index.index:
                    del self.index.index[old_first_id]
                self.index.add(new_first_id, position)

            self.index.add(second_half[0].id_venta, new_position)
            self.index.save_index()

        print(f" - Página dividida: {len(first_half)} + {len(second_half)} registros.")
        print(f" - Nuevo índice: ID {second_half[0].id_venta} -> {new_position}")

    def _handle_page_chain(self, file, position, record):
        file.seek(position)
        page_data = file.read(Page.SIZE_OF_PAGE)
        page = Page.unpack(page_data)
        
        current_pos = position
        previous_pos = -1

        while current_pos != -1:
            file.seek(current_pos)
            page_data = file.read(Page.SIZE_OF_PAGE)
            page = Page.unpack(page_data)

            if self._should_insert_in_this_page(page, record):
                all_records = page.records + [record]
                all_records.sort(key=lambda x: x.id_venta)

                mid = len(all_records) // 2
                stay_records = all_records[:mid]
                move_records = all_records[mid:]

                page.records = stay_records
                original_next = page.next_page

                file.seek(0, 2)
                new_position = file.tell()
                new_page = Page(move_records, original_next)
                file.write(new_page.pack())

                page.next_page = new_position
                file.seek(current_pos)
                file.write(page.pack())

                print(f" - Página encadenada: {len(stay_records)} + {len(move_records)} registros.")
                print(f" - Nueva página en posición: {new_position}")
                return
            previous_pos = current_pos
            current_pos = page.next_page

        file.seek(0, 2)
        new_position = file.tell()
        new_page = Page([record])
        file.write(new_page.pack())

        if previous_pos != -1:
            file.seek(previous_pos)
            page_data = file.read(Page.SIZE_OF_PAGE)
            last_page = Page.unpack(page_data)
            last_page.next_page = new_position
            file.seek(previous_pos)
            file.write(last_page.pack())

        print(f" - Nueva página encadenada al final: ID {record.id_venta} en posición {new_position}.")

    def search(self, key: int):
        if not os.path.exists(self.filename):
            print("Error: El archivo de datos no existe.")
            return None
        
        with open(self.filename, 'rb') as file:
            target_position = self._find_target_position(file, key)

            current_pos = target_position
            while current_pos != -1:
                file.seek(current_pos)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)

                for record in page.records:
                    if record.id_venta == key:
                        return record
                
                current_pos = page.next_page

            return None
        
    def delete(self, key: int):
        if not os.path.exists(self.filename):
            return False
        with open(self.filename, 'r+b') as file:
            start_position = self._find_target_position(file, key)

            current_pos = start_position
            previous_pos = -1

            while current_pos != -1:
                file.seek(current_pos)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)

                for i, record in enumerate(page.records):
                    if record.id_venta == key:
                        page.records.pop(i)

                        if not page.records:
                            print(f" - Página quedó vacía después de eliminar ID {key}.")
                            self._handle_empty_page(file, current_pos, previous_pos, page.next_page)
                        else:
                            file.seek(current_pos)
                            file.write(page.pack())

                            self._update_index_after_deletion(current_pos, page.records[0].id_venta)
                        
                        print(f" - Registro ID {key} eliminado exitosamente.")
                        return True
                previous_pos = current_pos
                current_pos = page.next_page
            print(f" - Registro ID {key} no encontrado.")
            return False
        
    def _handle_empty_page(self, file, empty_pos, previous_pos, next_pos):
        if self.index:
            keys_to_remove = [k for k, v in self.index.index.items() if v == empty_pos]
            for k in keys_to_remove:
                del self.index.index[k]
                print(f" - Entrada de índice para ID {k} eliminada.")
        
        if previous_pos != -1:
            file.seek(previous_pos)
            prev_page_data = file.read(Page.SIZE_OF_PAGE)
            prev_page = Page.unpack(prev_page_data)
            prev_page.next_page = next_pos
            file.seek(previous_pos)
            file.write(prev_page.pack())
            print(f" - Página anterior {empty_pos} ahora apunta a: {next_pos}.")

        if self.index:
            self.index.save_index()

    def _update_index_after_deletion(self, position, new_first_id):
        if not self.index:
            return
        old_first_id = None
        for k, v in self.index.index.items():
            if v == position:
                old_first_id = k
                break
        
        if old_first_id and old_first_id != new_first_id:
            del self.index.index[old_first_id]
            self.index.add(new_first_id, position)
            self.index.save_index()
            print(f" - Índice actualizado: ID {old_first_id} -> ID {new_first_id}")

    def scan_all_pages(self):
        if not os.path.exists(self.filename):
            print("Error: El archivo de datos no existe.")
            return
        
        with open(self.filename, 'rb') as file:
            file.seek(0, 2)
            filesize = file.tell()
            
            print("=== PÁGINAS DE DATOS ===")
            page_num = 1
            position = 0

            while position < filesize:
                file.seek(position)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)

                print(f"--- Page {page_num} (pos: {position})")

                for record in page.records:
                    print(f" {record}")
                
                if page.next_page != -1:
                    print(f" -> Encadenada a posición: {page.next_page}")
                
                position += Page.SIZE_OF_PAGE
                page_num += 1

class IndexFile:
    def __init__(self, indexname: str):
        self.indexname = indexname
        self.index = {}
        self.load_index()

    def add(self, key: int, position: int):
        self.index[key] = position
    
    def is_full(self):
        return len(self.index) >= MAX_INDEX_ENTRIES
    
    def find_page_for_key(self, key: int):
        if not self.index:
            return 0
        
        sorted_keys = sorted(self.index.keys())

        best_position = 0
        for index_key in sorted_keys:
            if index_key <= key:
                best_position = self.index[index_key]
            else:
                break
        return best_position
    
    def save_index(self):
        with open(self.indexname, 'wb') as file:
            file.write(struct.pack('i', len(self.index)))
            for key in sorted(self.index.keys()):
                file.write(struct.pack('ii', key, self.index[key]))
    
    def load_index(self):
        if not os.path.exists(self.indexname):
            return 
        with open(self.indexname, 'rb') as file:
            try:
                num_entries = struct.unpack('i', file.read(4))[0]

                for _ in range(num_entries):
                    key, position = struct.unpack('ii', file.read(8))
                    self.index[key] = position
            except:
                self.index = {}

    def show_index(self):
        print("=== ÍNDICE DISPERSO ===")
        for key in sorted(self.index.keys()):
            print(f"ID Venta: {key} -> Posición: {self.index[key]}")
        print("=======================")

def load_csv_data(filename):
    records = []

    try:
        with open(filename, 'r', encoding='utf-8-sig') as file:
            sample = file.read(1024)
            file.seek(0)

            delimiter = ',' if ',' in sample else ';'
            reader = csv.reader(file, delimiter=delimiter)
            headers = next(reader)
            print(f"Columnas del CSV: {headers}")

            for row in reader:
                if len(row) >= 4:
                    try:
                        id_venta = int(row[0])
                        nombre = row[1]
                        cantidad = int(row[2])
                        precio = float(row[3])
                        fecha = row[4] if len(row) > 4 else ""
                        record = Record(id_venta, nombre, cantidad, precio, fecha)
                        records.append(record)
                    except ValueError:
                        continue
        print(f"Cargados {len(records)} registros desde el CSV.")
        return records
    except FileNotFoundError:
        print(f"Error: El archivo {filename} no fue encontrado.")
        return []
    
if __name__ == "__main__":
    print("=== LABORATORIO 3: ISAM (Sparse Index) ===")
    print(f"BLOCK_FACTOR: {BLOCK_FACTOR}")
    print(f"MAX_INDEX_ENTRIES: {MAX_INDEX_ENTRIES}")
    
    print("\n1. Creando DataFile con índice...")
    data_file = DataFile("ventas.dat", "indice_ventas.dat")
    
    print("\n2. Cargando registros desde CSV...")
    records = load_csv_data("sales_dataset_unsorted.csv")
    
    if not records:
        print("No se pudieron cargar registros. Terminando.")
        exit()
    
    test_records = records[:12]
    print("\n3. Ordenando registros inicialmente por ID...")
    test_records.sort(key=lambda x: x.id_venta)
    print("Registros ordenados:")
    for i, record in enumerate(test_records, 1):
        print(f" {i}: {record}")
    
    print("\n4. Construyendo archivo inicial ordenado...")
    data_file.build_initial_file(test_records)
    
    print("\n5. Contenido del archivo inicial:")
    data_file.scan_all_pages()
    
    print("\n")
    data_file.index.show_index()
    print(f"Entradas en índice: {len(data_file.index.index)}/{MAX_INDEX_ENTRIES}")
    
    print("\n6. Agregando registros para demostrar DIVISIÓN (índice no lleno)...")
    division_records = [
        Record(25, "Producto A", 1, 100.0, "2023-01-01"),
        Record(999, "Producto B", 2, 200.0, "2023-01-02")
    ]
    
    for i, record in enumerate(division_records, 1):
        print(f"\n--- Insertando para división {i}: {record} ---")
        data_file.add(record)
    
    print("\n7. Contenido después de divisiones:")
    data_file.scan_all_pages()
    
    print("\n")
    data_file.index.show_index()
    print(f"Entradas en índice: {len(data_file.index.index)}/{MAX_INDEX_ENTRIES}")
    
    print("\n8. Agregando registros para demostrar ENCADENAMIENTO (índice lleno)...")
    chain_records = [
        Record(750, "Producto C", 3, 300.0, "2023-01-03"),
        Record(450, "Producto D", 4, 400.0, "2023-01-04")
    ]
    
    for i, record in enumerate(chain_records, 1):
        print(f"\n--- Insertando para encadenamiento {i}: {record} ---")
        data_file.add(record)
    
    print("\n9. Contenido final después de ambos casos:")
    data_file.scan_all_pages()
    
    print("\n")
    data_file.index.show_index()
    print(f"Entradas en índice: {len(data_file.index.index)}/{MAX_INDEX_ENTRIES}")
    
    print("\n10. Pruebas de búsqueda:")
    search_ids = [25, 33, 450, 750, 999, 99999]
    for search_id in search_ids:
        result = data_file.search(search_id)
        if result:
            print(f"✓ Encontrado: {result}")
        else:
            print(f"✗ No encontrado: ID {search_id}")
    
    print("\n11. Pruebas de eliminación:")
    print("Eliminando registros para demostrar diferentes casos...")
    print("\n--- Eliminando registro ID 107 (página NO queda vacía) ---")
    data_file.delete(107)
    
    print("\n--- Eliminando todos los registros de una página para demostrar página vacía ---")
    print("Eliminando ID 999 (único registro en página)...")
    data_file.delete(999)
    
    print("\n--- Contenido después de crear página vacía ---")
    data_file.scan_all_pages()
    print("\n")
    data_file.index.show_index()
    
    print("\n--- Eliminando registro ID 750 ---")
    data_file.delete(750)
    
    print("\n12. Contenido final después de eliminaciones:")
    data_file.scan_all_pages()
    
    print("\n")
    data_file.index.show_index()

    print("\n=== FIN DEL LABORATORIO ===")
