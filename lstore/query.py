from lstore.table import Table, Record
from lstore.index import Index
import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        pass
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns

        # Check for duplicate key already in table
        key_val = columns[self.table.key] # Identify value of new key
        if key_val in self.table.index.primary_key_index: 
            return False

        # Assign RID & Get Time
        rid = self.table.get_rid()
        timestamp = int(time.time())

        # Find and add the record into the next avaliable space
        self.table.add_base_record(rid, None, timestamp, schema_encoding, columns)

        # Update the index
        self.table.index.primary_key_index[key_val] = rid

        # Add to the page directory: RID -> (page_range, page #, offset, BASEorTail)
        location = (len(self.table.page_range) - 1, self.table.page_range[-1].current_base_index, self.table.page_range[-1].base_pages[-1].get_offset(), "Base")
        self.table.page_directory[rid] = location
        return True
    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        pass

    
    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        pass


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # Get base RID using key, via the index
        try:    
            rid = self.table.index.primary_key_index[primary_key]
        except:
            return False

        # Get the location of record
        location = self.table.find_record(rid)

        # Get record using the page directory (metadata, col1, .., col n)
        record = self.table.get_record(rid)
        
        # Allocate a new tail record and assign an RID
        tail_rid = self.table.get_rid()
        timestamp = int(time.time())

        
        # Gets schema encoding 
        # Note binmask is an integer (need to convert to binary to see schema encoding)
        tail_bitmask = 0
        base_bitmask = int.from_bytes(self.table.page_range[location[0]].base_pages[location[1]].indirection.data[location[2]:location[2]+8], byteorder = 'big')

        for i in range(len(columns)):
            pos = len(columns) - (i + 1)
            if columns[i] != None:                
                tail_bitmask |= (1 << pos)
                base_bitmask |= (1 << pos)

        # Get indirection
        # POSSIBLE ISSUE: BASE AND TAIL RID CONFLICT
        if record[1] == 0: # first tail record created
            indirection = rid
        else:
            indirection = record[1]

        pageRange = location[0]
        
        # Write tail record with updated values - NON-CUMULATIVE
        self.table.add_tail_record(tail_rid, indirection, timestamp, tail_bitmask, columns, pageRange)

        # Change Schema Encoding & Indirection in base page
        self.table.page_range[location[0]].base_pages[location[1]].indirection.data[location[2]:location[2]+8] = tail_rid.to_bytes(8, byteorder='big', signed = True)
        self.table.page_range[location[0]].base_pages[location[1]].schema_encoding.data[location[2]:location[2]+8] = base_bitmask.to_bytes(8, byteorder='big', signed = True)
        
        # Add tail record to the page directory
        self.table.page_directory[tail_rid] = (pageRange, self.table.page_range[pageRange].current_tail_index, self.table.page_range[pageRange].tail_pages[-1].get_offset(), "Tail") 
        return True
        
        
            
        



    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        pass

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        pass

    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    # Helper Function
    # Columns is a tuple with either None or updated values
    def create_schema(bitmask, *columns):
        for i in range(len(columns)):
            pos = len(columns) - (i + 1)
            if columns[i] != None:                
                bitmask |= (1 << pos)
