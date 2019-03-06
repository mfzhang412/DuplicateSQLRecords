from collections import namedtuple
from sqlalchemy import create_engine, select, insert, MetaData, Table, String, Column, Text, DateTime, Boolean, Integer
from sqlalchemy.dialects import postgresql
from datetime import datetime

def clear_all_records(table_name):
    connection.execute("DELETE FROM " + table_name)

def print_records(table_name):
    table = metadata.tables[table_name]

    s = select([table])

    rs = connection.execute(s)
    rows = rs.fetchall()
    
    print("Number of rows:", rs.rowcount, "\n")
    
    for row in rows:
        print(row)

def duplicate_records_2(table_name):
    table = metadata.tables[table_name]

    s = select([table])

    rs = connection.execute(s)
    rows = rs.fetchall()

    col_names = table.columns.keys()
    mod_col_names = col_names[1:] #shave off the id column
    ins_statement_list = []
    for row in rows:
        for i in range(number_of_dupes):
            value_list = list(row[1:])#shave off the id column
            value_list[dupe_col_index-1] = value_list[dupe_col_index-1] + str(i+1) #the -1 is from shaving the first column off
            insertion_dict = {k:v for (k,v) in zip(mod_col_names, value_list)}
            ins_statement_list.append(insertion_dict)

    ins = insert(table)
    connection.execute(ins, ins_statement_list)


def duplicate_records(table_name):
    table = metadata.tables[table_name]

    s = select([table])

    rs = connection.execute(s)
    rows = rs.fetchall()

    col_names = table.columns.keys()
    mod_col_names = col_names[1:]  # shave off the id column
    ins = insert(table)
    for row in rows:
        for i in range(number_of_dupes):
            value_list = list(row[1:])  # shave off the id column
            value_list[dupe_col_index-1] = value_list[dupe_col_index-1] + str(i+1)  # the -1 is from shaving the first column off
            insertion_dict = {k: v for (k, v) in zip(mod_col_names, value_list)}
            connection.execute(ins, insertion_dict)



number_of_dupes = 0 #
dupe_col_index = 0 #
table_name = '' #

ConnectionInfo = namedtuple('Connection', 'dialect driver user pass_ host port database')
connect_info = ConnectionInfo(
    dialect="", #
    driver="", #
    user="", #
    pass_="", #
    host="", #
    port="", #
    database="" #
    )
connection_string = "{}+{}://{}:{}@{}:{}/{}".format(*connect_info)
engine = create_engine(connection_string)
connection = engine.connect()

metadata = MetaData()
metadata.reflect(bind=engine)

#print_records(table_name)
print("\n____________________________________\n")
duplicate_records(table_name)
print("\n____________________________________\n")
print_records(table_name)
print("\n\nDone!")



