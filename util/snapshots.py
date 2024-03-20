import csv
import datetime
from decimal import Decimal, InvalidOperation
import os


def snapshot_database_tables(source_metadata, source_session):
    table_names = source_metadata.tables.keys()
    if not os.path.exists('snapshot'):
        os.makedirs('snapshot')

    for table_name in table_names:
        source_table = source_metadata.tables[table_name]
        rows = source_session.query(source_table).all()
        fieldnames = [source_column.name for source_column in source_table.columns]
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        types = [source_column.type for source_column in source_table.columns]
        filepath = 'snapshot/' + table_name + '_' + timestamp + '.csv'
        
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)
            writer.writerow(types)

            writer.writerows(rows)

import csv

# get the latest snapshot data as a list. Each row is a list of strings
def get_latest_snapshot_data(table_name):
    csv_snapshots = [file for file in os.listdir('snapshot') if file.startswith(table_name + '_')]
    latest_snapshot = sorted(csv_snapshots, reverse=True)[0]
    filepath = 'snapshot/' + latest_snapshot
    with open(filepath, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader) # skip header
        types = next(reader)
        data = []
        for row in reader:
            typed_row = []
            for value, type in zip(row, types):
                if value == "":
                    typed_row.append(None)
                elif 'int' in type.lower():
                    typed_row.append(int(value))
                elif 'decimal' in type.lower():
                    typed_row.append(Decimal(value))
                else:
                    typed_row.append(value)
            data.append(typed_row)
    return data
