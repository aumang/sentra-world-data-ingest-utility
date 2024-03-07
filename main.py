import csv
import json
from sqlalchemy import create_engine, MetaData, Table, select

#function to read a csv file
def read_csv_to_dict(csv_file_path):
    data = []
    with open(csv_file_path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader)
        for row in reader:
            row_data = {}
            for i in range(len(header)):
                row_data[header[i]] = row[i]
            data.append(row_data)
    return data

#Function to return a json from a csv
def convert_csv_to_json(csv_file_path):
    data = read_csv_to_dict(csv_file_path)
    json_data = json.dumps(data, indent=2)
    return json_data

def validate_data(data, table_name):
    # Connect to your database (replace with your connection details)
    # TODO 1: Handle Exceptions
    engine = create_engine("postgresql://postgres:9852@localhost/TestDB")

    # Get the column names from the database table
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    columns_in_db = set(column.name for column in table.columns)

    # Get the column names from the CSV data
    if data:
        columns_in_csv = set(data[0].keys())
    else:
        print("CSV data is empty. Validation failed.")
        return False

    # Check if all columns in the CSV exist in the database table
    if columns_in_db.issubset(columns_in_csv):
        print("Validation successful. All columns exist in the database.")
        return True
    else:
        print("Validation failed. Some columns do not exist in the database.")
        return False

def insert_data_into_db(data, table_name):
    # Connect to your database (replace with your connection details)
    engine = create_engine('postgresql://postgres:9852@localhost/TestDB')
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    columns_in_db = set(col for col in table.columns.keys() if col != 'id')


    with engine.connect() as connection:
        for row in data:
            # Filter out any extra keys not present in the table columns
            valid_row = {col: row.get(col) for col in columns_in_db}
            connection.execute(table.insert().values(**valid_row))

        connection.commit()
        print("Data inserted into the database.")


def load_validation_rules(json_file_path):
    try:
        with open(json_file_path) as json_file:
            validation_rules = json.load(json_file)
        return validation_rules
    except FileNotFoundError:
        print(f"JSON file not found: {json_file_path}")
        return None
    except json.JSONDecodeError:
        print(f"Invalid JSON in file: {json_file_path}")
        return None

def validate_data_with_rules(json_data, validation_rules_path):
    validation_rules = load_validation_rules(validation_rules_path)
    if not validation_rules:
        print("Validation rules not loaded. Cannot validate.")
        return False

    for row in json_data:
        for col, rules in validation_rules.items():
            for rule in rules:
                if rule == "not-null" and (col not in row or row[col] is None):
                    print(f"Validation failed: {col} cannot be null.")
                    return False
                elif rule == "primary-key" and (col not in row or row[col] == ""):
                    print(f"Validation failed: {col} is missing or empty (primary key constraint).")
                    return False

    print("Validation successful.")
    return True


# Main
def main():
    # These can be env parameters in the dockerFile
    csv_file_path = 'ExampleFileCSV1.csv'
    table_name = "activitydata"
    validation_rules = "validationRules.txt"
    
    # TODO 1: use python packages instead
    # TODO 2: Handle Exception in convert_csv_to_json
    data = read_csv_to_dict(csv_file_path)
    print(data)

    if validate_data(data, table_name) & validate_data_with_rules(data,validation_rules):
        insert_data_into_db(data, table_name)

if __name__ == "__main__":
    main()
