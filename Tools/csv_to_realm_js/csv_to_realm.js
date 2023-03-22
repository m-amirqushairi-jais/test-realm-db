const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const Realm = require('realm');

const inputCsv = '../../csv/Heroes.csv'; // Replace with your CSV file path
const outputRealm = 'example.realm'; // Replace with your desired Realm database file path

class CsvRow extends Realm.Object {}
CsvRow.schema = {
  name: 'CsvRow',
  properties: {} // The schema will be updated dynamically based on the CSV file
};

async function main() {
  const columns = await parseCsvHeader(inputCsv);
  CsvRow.schema.properties = createRealmSchema(columns);

  const realmConfig = {
    path: outputRealm,
    schema: [CsvRow],
    schemaVersion: 1,
  };

  const realm = new Realm(realmConfig);
  await processCsvFile(inputCsv, realm);

  console.log(`CSV file '${inputCsv}' has been converted to Realm database '${outputRealm}'.`);
  realm.close();
}

function parseCsvHeader(csvFile) {
  return new Promise((resolve, reject) => {
    const columns = [];
    fs.createReadStream(csvFile)
      .pipe(csvParser())
      .on('headers', (headers) => {
        resolve(headers);
      })
      .on('error', reject);
  });
}

function createRealmSchema(columns) {
  const schema = {};
  for (const column of columns) {
    schema[column] = 'string';
  }
  return schema;
}

function processCsvFile(csvFile, realm) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFile)
      .pipe(csvParser())
      .on('data', (row) => {
        realm.write(() => {
          realm.create(CsvRow.schema.name, row);
        });
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

main().catch((error) => {
  console.error(`An error occurred: ${error.message}`);
});
