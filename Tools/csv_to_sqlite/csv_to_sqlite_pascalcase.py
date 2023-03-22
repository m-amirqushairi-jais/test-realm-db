import os
import csv
import sqlite3
import re

def pascal_case(s):
    s = re.sub(r"[\-_\s]+", " ", s).strip()
    if s.lower() == 'id':
        return s
    words = re.findall(r'[A-Z][a-z]*|[a-z]+', s)
    return ''.join([word.capitalize() if i > 0 else word for i, word in enumerate(words)])

def create_sqlite_table(cursor, table_name, columns):
    column_definitions = ', '.join([f"{pascal_case(name)} {data_type}" for name, data_type in columns])
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions})"
    cursor.execute(create_table_query)

def insert_data_into_table(cursor, table_name, columns, data):
    placeholders = ', '.join(['?' for _ in columns])
    insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
    cursor.executemany(insert_query, data)

def process_csv_file(db_connection, csv_file_path):
    file_name = os.path.splitext(os.path.basename(csv_file_path))[0]
    table_name = pascal_case(file_name)
    
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        columns = next(reader)
        data_types = next(reader)
        columns_with_data_types = list(zip(columns, data_types))

        cursor = db_connection.cursor()
        create_sqlite_table(cursor, table_name, columns_with_data_types)
        insert_data_into_table(cursor, table_name, columns, reader)
        db_connection.commit()

def main():
    csv_directory = '../../csv'  # Change this to the directory containing your CSV files
    sqlite_db_file = 'csv_to_sqlite_pc.db'

    if not os.path.exists(csv_directory):
        print(f"The CSV directory '{csv_directory}' does not exist.")
        return

    db_connection = sqlite3.connect(sqlite_db_file)

    for file_name in os.listdir(csv_directory):
        if file_name.endswith('.csv'):
            file_path = os.path.join(csv_directory, file_name)
            print(f"Processing '{file_path}'...")
            process_csv_file(db_connection, file_path)

    db_connection.close()
    print(f"CSV files have been converted to SQLite tables in '{sqlite_db_file}'.")

if __name__ == '__main__':
    main()
