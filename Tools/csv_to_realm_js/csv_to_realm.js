const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const Realm = require('realm');

const inputFolder = '../../csv'; // Replace with your CSV files folder path
const outputRealm = 'cdc_test.realm'; // Replace with your desired Realm database file path

async function main() {
  const csvFiles = fs.readdirSync(inputFolder).filter(file => file.endsWith('.csv'));

  const realmConfig = {
    path: outputRealm,
    schema: [],
    schemaVersion: 1,
  };

  for (const csvFile of csvFiles) {
    const filePath = path.join(inputFolder, csvFile);
    const columns = await parseCsvHeader(filePath);
    const schemaName = getSchemaName(csvFile);
    const schema = createRealmSchema(schemaName, columns);

    realmConfig.schema.push(schema);

    const realm = new Realm(realmConfig);
    await processCsvFile(filePath, realm, schema);
    realm.close();
  }

  console.log(`CSV files in '${inputFolder}' have been converted to Realm database '${outputRealm}'.`);
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

function getSchemaName(fileName) {
  const baseName = path.basename(fileName, '.csv');
  return baseName.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase()).replace(/\s+/g, '');
}

function createRealmSchema(schemaName, columns) {
  const schema = {
    name: schemaName,
    properties: {}
  };

  for (const column of columns) {
    schema.properties[column] = 'string';
  }

  return schema;
}

function processCsvFile(csvFile, realm, schema) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFile)
      .pipe(csvParser())
      .on('data', (row) => {
        realm.write(() => {
          realm.create(schema.name, row);
        });
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

main().catch((error) => {
  console.error(`An error occurred: ${error.message}`);
});
