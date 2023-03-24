const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const Realm = require('realm');

const defaultInputFolder = 'csv_files'; // Replace with your CSV files folder path
const defaultOutputRealm = 'default.realm'; // Replace with your desired default Realm database file path

async function main() {
  const { inputFiles, inputFolder, outputRealm } = parseArguments();
  let csvFiles;

  if (inputFiles.length > 0) {
    csvFiles = inputFiles.filter(file => file.endsWith('.csv'));
  } else {
    csvFiles = fs.readdirSync(inputFolder).filter(file => file.endsWith('.csv'));
    csvFiles = csvFiles.map(file => path.join(inputFolder, file));
  }

  const realmConfig = {
    path: outputRealm,
    schema: [],
    schemaVersion: 1,
  };

  for (const csvFile of csvFiles) {
    const columns = await parseCsvHeader(csvFile);
    const schemaName = getSchemaName(csvFile);
    const schema = createRealmSchema(schemaName, columns);

    realmConfig.schema.push(schema);

    const realm = new Realm(realmConfig);
    await processCsvFile(csvFile, realm, schema);
    realm.close();
  }

  console.log(`CSV files have been converted to Realm database '${outputRealm}'.`);
}

function parseArguments() {
  const inputFiles = [];
  let inputFolder = defaultInputFolder;
  let outputRealm = defaultOutputRealm;

  for (let i = 2; i < process.argv.length; i++) {
    const arg = process.argv[i];

    if (arg === '--input') {
      i++;
      inputFolder = process.argv[i];
    } else if (arg === '--output') {
      i++;
      outputRealm = process.argv[i];
    } else {
      inputFiles.push(arg);
    }
  }

  return { inputFiles, inputFolder, outputRealm };
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
