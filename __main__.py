from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []


query.insert(92107415, 18, 13, 6, 8)
query.update(92107415, *[None, None, 4, 0, 6])

recordObjecta = query.select_version(92107415, 0, [1,1,1,1,1], 0)[0]

print(recordObjecta.rid)
print(recordObjecta.key)
print(recordObjecta.columns)

'''
query.insert(906659671, 93, 0, 0, 0)
query.update(906659671, *[None, 10, None, None, None])
query.update(906659671, *[None, 11, None, None, None])
query.update(906659671, *[None, 12, 35, None, None])
query.update(906659671, *[None, None, None, 67, None])


#query.select(906659671, 0, [1,1,1,1,1])[0]


recordObjecta = query.select_version(906659671, 0, [1,1,1,1,1], 0)[0]
recordObjectb = query.select_version(906659671, 0, [1,1,1,1,1], -1)[0]
recordObjectc = query.select_version(906659671, 0, [1,1,1,1,1], -2)[0]
recordObjectd = query.select_version(906659671, 0, [1,1,1,1,1], -3)[0]

print(recordObjecta.rid)
print(recordObjecta.key)
print(recordObjecta.columns)

print(recordObjectb.rid)
print(recordObjectb.key)
print(recordObjectb.columns)

print(recordObjectc.rid)
print(recordObjectc.key)
print(recordObjectc.columns)

print(recordObjectd.rid)
print(recordObjectd.key)
print(recordObjectd.columns)

query.insert(906659671, 93, 79)

#print(grades_table.page_directory)
#print(grades_table.index.primary_key_index)

query.update(906659671, *[None, 10, None])


print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].rid.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].indirection.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].time.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].schema_encoding.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].pages[0].data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].pages[1].data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].pages[2].data[0:8], byteorder = 'big'))

print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].rid.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].indirection.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].time.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].schema_encoding.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[0].data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[1].data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[2].data[0:8], byteorder = 'big'))

query.update(906659671, *[None, None, 67])

print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].rid.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].indirection.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].time.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].schema_encoding.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].pages[0].data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].pages[1].data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].tail_pages[0].pages[2].data[8:16], byteorder = 'big'))

print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].rid.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].indirection.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].time.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].schema_encoding.data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[0].data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[1].data[0:8], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[2].data[0:8], byteorder = 'big'))

print(grades_table.page_directory)


query.insert(906659672, 94)
print(grades_table.page_directory)
print(grades_table.index.primary_key_index)

print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].rid.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].indirection.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].time.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].schema_encoding.data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[0].data[8:16], byteorder = 'big'))
print(int.from_bytes(db.tables[0].page_range[0].base_pages[0].pages[1].data[8:16], byteorder = 'big'))

print(len(grades_table.page_range))
print(len(grades_table.page_directory))
print(len(grades_table.index.primary_key_index))

insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)


# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    query.select(choice(keys),0 , [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)
'''