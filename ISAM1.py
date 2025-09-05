import struct

BLOCK_FACTOR = 3

class Record:
    # nombre(20), apellido(20), ciclo
    FORMAT = '20s20si'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, nombre: str, apellido: str, ciclo: int):
        self.nombre = nombre
        self.apellido = apellido
        self.ciclo = ciclo
    
    def pack(self)->bytes:
        return struct.pack(self.FORMAT, 
        self.nombre[:20].ljust(20).encode(),
        self.apellido[:20].ljust(20).encode(),
        self.ciclo
        )

    @staticmethod
    def unpack(data: bytes):
        nombre, apellido, ciclo = struct.unpack(Record.FORMAT, data)
        return Record(nombre.decode().rstrip(), apellido.decode().rstrip(), ciclo)

    def __str__(self):
        return self.nombre+" - "+self.apellido+" - "+str(self.ciclo);

class Page:
    HEADER_FORMAT = 'ii' #size, next_page
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    SIZE_OF_PAGE = HEADER_SIZE + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records = [], next_page = -1):
        self.records = records
        self.next_page = next_page

    def pack(self):
        header_data = struct.pack(self.HEADER_FORMAT, len(self.records), self.next_page)
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
        size, next_page = struct.unpack(Page.HEADER_FORMAT, data[:Page.HEADER_SIZE])
        offset = Page.HEADER_SIZE
        records = []
        for i in range(size):
            record_data = data[offset: offset + Record.SIZE_OF_RECORD]
            records.append(Record.unpack(record_data))
            offset += Record.SIZE_OF_RECORD
        return Page(records, next_page)

   
class DataFile:
    def __init__(self, filename: str):
        self.filename = filename

    def add(self, record: Record):
        import os
        #El archivo no existe
        if not os.path.exists(self.filename):
            with open(self.filename, 'wb') as file:
                page = Page([record])
                file.write(page.pack())
            return
        #El archivo si existe
        with open(self.filename, 'r+b') as file:
            file.seek(0, 2)
            filesize = file.tell()
            pos = filesize - Page.SIZE_OF_PAGE
            file.seek(pos, 0)
            page = Page.unpack(file.read(Page.SIZE_OF_PAGE))
            if len(page.records) < BLOCK_FACTOR:
                # si hay espacio hay que agregar
                page.records.append(record)
                file.seek(pos, 0)
                file.write(page.pack())
            else:
                # Si no hay espacio en la pagina, agregar nueva pagina
                page = Page([record])
                file.seek(0, 2)
                file.write(page.pack())
    
    def scanAll(self):
        with open(self.filename, 'rb') as file:
            file.seek(0, 2)
            num_pages = file.tell() // Page.SIZE_OF_PAGE
            file.seek(0, 0)
            for i in range(num_pages):
                print("--- Page ", i+1)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                for record in page.records:
                    print(record)

# Main
datafile = DataFile('datos.dat')
datafile.add(Record('Ana', 'Vega', 5))
datafile.add(Record('Berta', 'Galvez', 6))
datafile.add(Record('Dino', 'Vera', 10))
datafile.add(Record('Jorge', 'Meneses', 1))
datafile.add(Record('Heider', 'Sanchez', 11))
datafile.add(Record('Maria', 'Meneses', 1))
datafile.add(Record('Romina', 'Galvez', 11))
datafile.scanAll()


