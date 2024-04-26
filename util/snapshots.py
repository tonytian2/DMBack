import csv
import datetime
import os
from decimal import Decimal


def snapshot_database_tables(source_metadata, source_session, history_suffix):
    output = {}
    table_names = source_metadata.tables.keys()
    filtered_table_names = [tn for tn in table_names if history_suffix not in tn]
    if not os.path.exists("snapshot"):
        os.makedirs("snapshot")

    for table_name in filtered_table_names:
        source_table = source_metadata.tables[table_name]
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        rows = source_session.query(source_table).all()
        fieldnames = [source_column.name for source_column in source_table.columns]
        types = [source_column.type for source_column in source_table.columns]
        filepath = "snapshot/" + table_name + "_" + timestamp + ".csv"

        with open(filepath, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(fieldnames)
            writer.writerow(types)

            writer.writerows(rows)
        output[table_name] = len(rows)
    return output


# get the latest snapshot data as a list
def get_latest_snapshot_data(table_name):
    csv_snapshots = [
        file
        for file in os.listdir("snapshot")
        if file.rsplit("_", 1)[0].rsplit("_", 1)[0] == table_name
    ]
    if not csv_snapshots:
        return None
    latest_snapshot = sorted(csv_snapshots, reverse=True)[0]
    filepath = "snapshot/" + latest_snapshot
    with open(filepath, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header
        types = next(reader)
        data = []
        for row in reader:
            typed_row = []
            for value, type in zip(row, types):
                if value == "":
                    typed_row.append(None)
                elif "int" in type.lower():
                    typed_row.append(int(value))
                elif "decimal" in type.lower():
                    typed_row.append(Decimal(value))
                elif "time" in type.lower():
                    typed_row.append(
                        datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
                    )
                elif "date" in type.lower():
                    typed_row.append(
                        datetime.datetime.strptime(value, "%Y-%m-%d").date()
                    )
                else:
                    typed_row.append(value)
            data.append(tuple(typed_row))
    return data


def get_table_list():
    table_list = []
    for file_name in os.listdir("snapshot"):
        if file_name.endswith(".csv"):
            table_name = file_name.rsplit("_", 1)[0].rsplit("_", 1)[0]
            table_list.append(table_name)
    return list(table_list)
