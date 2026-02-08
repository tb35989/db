from lstore.index import Index
from time import time
from lstore.page_range import PageRange

# Switched RID and Indirection
RID_COLUMN = 0
INDIRECTION_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.tail_page_directory = {}
        self.page_range = [PageRange(num_columns)] # Stores all page ranges
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.total_columns = num_columns + 4
        self.rid_count = 1 # RID counter

    def __merge(self):
        print("merge is happening")
        pass
 
    # Note: RID -> (page_range, page #, offset)
    # Output: Location of the record as a tuple
    def find_record(self, rid):
        location = self.page_directory[rid]
        return location

    # Note: RID -> (page_range, page #, offset)
    # Output: The items in the record as a tuple (i.e (RID, indirection, time, schema, col1, col2, ..., coln))
    def get_record(self, rid):
        location = self.page_directory[rid]
        if location[3] == "Base":
            record = self.page_range[location[0]].base_pages[location[1]].get_record(location[2])
        elif location[3] == "Tail":
            record = self.page_range[location[0]].tail_pages[location[1]].get_record(location[2])
        return record

    # Get base RID
    def get_rid(self):
        rid = self.rid_count
        self.rid_count += 1
        return rid

    # Add base record
    def add_base_record(self, rid, indirection, time, se, col_info):
        # Add new page range
        if len(self.page_range[-1].base_pages) == 16 and self.page_range[-1].base_appends == 512: 
            p = PageRange(self.num_columns)
            self.page_range.append(p)

        self.page_range[-1].append_base(rid, None, time, se, col_info)
    
    # Add tail record given the index of the page range
    def add_tail_record(self, rid, indirection, time, se, col_info, pgRange):
        self.page_range[pgRange].append_tail(rid, indirection, time, se, col_info)