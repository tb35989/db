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
        invalidate = [-1 for _ in range(self.table.num_columns)]

        # Sets the base record's indirection to a tail record that has all user columns set to -1
        self.update(primary_key, *invalidate)

        return True
    
    
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
        # Provides RID based on search_key value
        # Note that I am writing this SELECT function in a way that it probably only works for primary key
        # I think that this should be good for Milestone 1 (ask TA if needed)
        try:    
            rid_list = [self.table.index.locate(search_key_index, search_key)]
        except:
            return False
        
        record_objects = []

        for i in range(len(rid_list)):
            base_record = self.table.get_record(rid_list[i])
            cols = base_record[4:] # user columns in base record
            index_cols = projected_columns_index[:]

            # Check if there are any tail records at all
            if base_record[1] == 0:
                for j in range(len(index_cols)):
                    if index_cols[j] == 0:
                        cols[j] = None
                record_objects.append(Record(rid=rid_list[i], key = search_key, columns=cols))
                continue
            else:
                indirection = base_record[1] # use indirection to find latest tail page

                while 1 in index_cols and indirection != rid_list[i]: # Check if any columns still need update or if we return to base page
                    record = self.table.get_record(indirection) # use indirection to find latest tail page
                    for index, binary in enumerate(index_cols):
                        # Check if column needs to be returned
                        if binary == 0:
                            cols[index] = None
                        elif binary == -1: # Column has already been updated
                            continue
                        elif binary == 1:
                            if record[index + 4] != cols[index] and record[index+4] != 0: # new value is different and not equal to zero
                                cols[index] = record[index + 4]
                                index_cols[index] = -1
            
                    # Get new tail record, using indirection
                    indirection = record[1]

                # Store as a Record object
                # Question: Is the key supposed to be the search key or the index of the column with the search key?
                # Currently it is set to be the search key
                record_objects.append(Record(rid=rid_list[i], key=search_key, columns=cols))
        return record_objects


    
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
        try:    
            rid_list = [self.table.index.locate(search_key_index, search_key)]
        except:
            return False
        

        record_objects = []
        relVersion = relative_version
        index_cols = projected_columns_index[:]
        done = False

        for i in range(len(rid_list)):
            base_record = self.table.get_record(rid_list[i])

            # Iterate through the pages, (relative_version + 1) times
            # E.g if it is -1, it needs to traverse 2 times, if it is -2, it traverses 3 times
            if relative_version < 0:
                while relative_version <= 0:
                    if base_record[1] == 0:
                        break # no past version beyond this exists
                    else:
                        indirection = base_record[1]
                        base_record = self.table.get_record(indirection)
                        user_cols = base_record[4:]

                        # Check if we traversed all the way back to a base page
                        # Uses the page directory to see if it is a base page
                        if self.table.page_directory[base_record[0]][3] == "Base":
                            for k in range(len(index_cols)):
                                if index_cols[k] == 0:
                                    user_cols[k] = None
                            record_objects.append(Record(rid=rid_list[i], key=search_key, columns=user_cols))
                            
                            done = True
                            break

                    relative_version += 1

            if done: # if we traversed back to a base page above
                continue

            cols = base_record[4:] # user columns in base record

            # Check Schema Encoding to see which columns were updated in this tail record
            num_cols = len(cols)
            if relVersion < 0:
                schema = format(base_record[3], f'0{num_cols}b')
                for j in range(len(schema)):
                    if schema[j] == "1":
                        index_cols[j] = -1

            # Check if there are any tail records at all
            if base_record[1] == 0:
                for k in range(len(index_cols)):
                    if index_cols[k] == 0:
                        cols[k] = None
                record_objects.append(Record(rid=rid_list[i], key=search_key, columns=cols))
                continue
            else:
                indirection = base_record[1] # use indirection to find latest tail page

                while 1 in index_cols and indirection != rid_list[i]: # Check if any columns still need update or if we return to base page
                    record = self.table.get_record(indirection) # use indirection to find latest tail page

                    # Get schema encoding for tail record
                    tail_schema = format(record[3], f'0{num_cols}b')

                    for index, binary in enumerate(index_cols):
                        # Check if column needs to be returned
                        if binary == 0:
                            cols[index] = None
                        elif binary == -1: # Column has already been updated
                            continue
                        elif binary == 1:
                            if record[index + 4] != cols[index] and record[index+4] != 0: # new value is different and not equal to zero
                                cols[index] = record[index + 4]
                                index_cols[index] = -1
                            elif record[index + 4] != cols[index] and tail_schema[index] == "1":
                                cols[index] = record[index + 4]
                                index_cols[index] = -1

                    # Get new tail record, using indirection
                    indirection = record[1]

                cols[search_key_index] = search_key # Added because tail records don't have primary key saved; can anyone think of a better way?
                record_objects.append(Record(rid=rid_list[i], key=search_key, columns=cols))

        return record_objects


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
        base_bitmask = int.from_bytes(self.table.page_range[location[0]].base_pages[location[1]].schema_encoding.data[location[2]:location[2]+8], byteorder = 'big')

        for i in range(len(columns)):
            pos = len(columns) - (i + 1)
            if columns[i] != None:                
                tail_bitmask |= (1 << pos)
                base_bitmask |= (1 << pos)

        # Get indirection
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
        pageNum = self.table.page_range[pageRange].current_tail_index
        self.table.page_directory[tail_rid] = (pageRange, pageNum, self.table.page_range[pageRange].tail_pages[pageNum].get_offset(), "Tail") 
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
        summation = 0

        arr = [0] *self.table.num_columns
        arr[aggregate_column_index] = 1

        # Finds all RIDs in a given range using the primary key index, then adds up their values
        # Since for loops end before the stop, I added one to end_range
        for i in range(start_range, end_range + 1):
            key_col = self.table.key # index of primary key column

            # Gets the newest version of the record
            # Since this is a primary key, we should only get one record object in return, so we can grab the 0th index
            record = self.select_version(i, key_col, arr, 0)[0]
            
            # Finds the value of the record
            # If value is -1, it indicates that the record is deleted, so we will skip the value
            # We should check with the TA to see if this will cause any test case issues
            if record.columns[aggregate_column_index] == -1:
                continue
            else:
                record_value = record.columns[aggregate_column_index]
                
            summation += record_value
        
        return summation

    
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
        summation = 0

        arr = [0] *self.table.num_columns
        arr[aggregate_column_index] = 1

        # Finds all RIDs in a given range using the primary key index, then adds up their values
        # Since for loops end before the stop, I added one to end_range
        for i in range(start_range, end_range + 1):
            key_col = self.table.key # index of primary key column

            # Gets the newest version of the record
            # Since this is a primary key, we should only get one record object in return, so we can grab the 0th index
            record = self.select_version(i, key_col, arr, relative_version)[0]
            
            # Finds the value of the record
            # If value is -1, it indicates that the record is deleted, so we will skip the value
            # We should check with the TA to see if this will cause any test case issues
            if record.columns[aggregate_column_index] == -1:
                continue
            else:
                record_value = record.columns[aggregate_column_index]
                
            summation += record_value
        
        return summation

    
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
