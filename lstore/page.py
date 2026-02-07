
class Page:

    def __init__(self):
        self.num_records = 0
        # creates 4kb of space (# rows basically) for the new page it's creating
        self.data = bytearray(4096)

    def has_capacity(self):
        maxCapacity = 4096 // 8
        if maxCapacity > self.num_records: # Checks capacity
            return True
        else:
            return False

    # note: when writing data in, call the write function from the page_directory.py file,
    # not this one
    # returns the location (slot/offset #) of the written in data
    def write(self, value):
        if value is None:
            value = 0
        elif isinstance(value, str):
            value = int(value, 2) # convert bitmap to binary
        if self.has_capacity(): 
            offset = self.num_records * 8
            self.data[offset:offset+8] = value.to_bytes(8, byteorder='big', signed = True)
            self.num_records += 1
        else:
            return False

    def get_offset(self):
        # return the slot #/offset # for the previous entry
        return (self.num_records - 1) * 8
        
class BasePage:
    def __init__(self, num_cols):
        self.rid = Page()
        self.indirection = Page()
        self.time = Page()
        self.schema_encoding = Page()
        self.pages = [Page() for _ in range(num_cols)]

    # Retrieves a record
    def get_record(self, offset):
        record = (self.rid.data[offset:offset+8], self.indirection.data[offset:offset+8], self.time.data[offset:offset+8], self.schema_encoding.data[offset:offset+8])
        for page in self.pages:
            record += (page.data[offset:offset+8],)
        
        new_record = []
        for item in record:
            new_record.append(int.from_bytes(item, byteorder='big'))
        return new_record
    
    def get_offset(self):
        return self.rid.get_offset()

    # Appending records into the pages
    # Assuming that col_info is a tuple (e.g (12, "Smith", "A"))
    def append_record(self, rid, indirection, time, se, *col_info):
        if len(col_info) == len(self.pages):
            self.rid.write(rid)
            self.indirection.write(indirection)
            self.time.write(time)
            self.schema_encoding.write(se)
            for i in range(len(self.pages)):
                self.pages[i].write(col_info[i])
        else:
            return False


class TailPage:
    def __init__(self, num_cols):
        self.rid = Page()
        self.indirection = Page()
        self.time = Page()
        self.schema_encoding = Page()
        self.pages = [Page() for _ in range(num_cols)]
    
    # Retrieves a record
    def get_record(self, offset):
        record = (self.rid.data[offset:offset+8], self.indirection.data[offset:offset+8], self.time.data[offset:offset+8], self.schema_encoding.data[offset:offset+8])
        for page in self.pages:
            record += (page.data[offset:offset+8],)
        
        new_record = []
        for item in record:
            new_record.append(int.from_bytes(item, byteorder='big'))
        return new_record
    
    def get_offset(self):
        return self.rid.get_offset()

    # Appending records into the pages
    # Assuming that col_info is a tuple (e.g (12, "Smith", "A"))
    def append_record(self, rid, indirection, time, se, *col_info):
        if len(col_info) == len(self.pages):
            self.rid.write(rid)
            self.indirection.write(indirection)
            self.time.write(time)
            self.schema_encoding.write(se)
            for i in range(len(self.pages)):
                self.pages[i].write(col_info[i])
        else:
            return False
