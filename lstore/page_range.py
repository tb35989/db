from lstore.page import Page, BasePage, TailPage

class PageRange:
    def __init__(self, num_columns):
        self.base_pages = [BasePage(num_columns)]
        self.tail_pages = [TailPage(num_columns)]
        self.current_base_index = 0
        self.current_tail_index = 0
        self.base_appends = 0
        self.tail_appends = 0
        self.num_columns = num_columns
    
    # Adds user info into columns inside base page (for INSERT)
    def append_base(self, rid, indirection, time, se, col_info):
        if self.base_appends == 512:
            new_page = self.add_base_page()
        self.base_pages[self.current_base_index].append_record(rid, indirection, time, se, *col_info)
        self.base_appends += 1 

    # Adds user info into columns inside tail page (for UPDATE)
    def append_tail(self, rid, indirection, time, se, col_info):
        if self.tail_appends == 512:
            new_page = self.add_tail_page()
        self.tail_pages[self.current_tail_index].append_record(rid, indirection, time, se, *col_info)
        self.tail_appends += 1

    # Used when previous base page is full
    def add_base_page(self):
        if len(self.base_pages) == 16: 
            return False # Tell table to create a new Page Range
        else:
            b = BasePage(self.num_columns)
            self.base_pages.append(b)
            self.current_base_index += 1
            self.base_appends = 0
        return b

    # Used when previous tail page is full
    def add_tail_page(self):
        t = TailPage(self.num_columns)
        self.tail_pages.append(t)
        self.current_tail_index += 1
        self.tail_appends = 0
        return t
