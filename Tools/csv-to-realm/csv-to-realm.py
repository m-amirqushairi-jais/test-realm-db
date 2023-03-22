import csv
import realm
import os

# CSV file location
csv_path = '../../csv/'
csv_name = 'Heroes.csv'

# Define your Realm object schema
class Heroes(realm.Object):
    _id = realm.PrimaryKey(str)
    japaneseName = realm.Property(str)

# Configure and open the Realm
config = realm.Configuration(schema=[Heroes])
r = realm.open(config)

# Read CSV file
csv_file = os.path.join(csv_path, csv_name)

# Parse CSV and add data to the Realm database
with open(csv_file, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
            print(row)
            print(row['Id'])
            print(row['Japanese Name'])
            # # Begin a write transaction
            # r.begin_write()

            # # Create a new Person object and set its properties
            # person = Person()
            # person._id = row['_id']
            # person.name = row['name']
            # person.age = int(row['age'])

            # # Add the Person object to the Realm database
            # r.add(person)

            # # Commit the transaction
            # r.commit_write()
        except Exception as e:
            print(f"Error processing row: {row}")
            print(e)
            # Cancel the transaction in case of an error
            r.cancel_write()

# Close the Realm
r.close()
