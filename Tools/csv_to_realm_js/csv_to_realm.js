const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const Realm = require('realm');
const readline = require('readline');

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
    const sheets = await readCsvSheets(filePath);

    for (const [sheetName, sheet] of Object.entries(sheets)) {
      const schemaName = getSchemaName(csvFile, sheetName);
      const columns = sheet.columns;
      const schema = createRealmSchema(schemaName, columns);

      realmConfig.schema.push(schema);

      const realm = new Realm(realmConfig);
      await processCsvFile(realm, schema, sheet.rows);
      realm.close();
    }
  }

  console.log(`CSV files in '${inputFolder}' have been converted to Realm database '${outputRealm}'.`);
}

async function readCsvSheets(filePath) {
  const sheets = {};

  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  let sheetName = 'Sheet1';
  let sheetColumns = [];
  let sheetRows = [];

  for await (const line of rl) {
    if (line === '---') {
      sheets[sheetName] = { columns: sheetColumns, rows: sheetRows };
      sheetName = `Sheet${Object.keys(sheets).length + 1}`;
      sheetColumns = [];
      sheetRows = [];
    } else {
      const row = line.split(',');

      if (sheetColumns.length === 0) {
        sheetColumns = row;
      } else {
        sheetRows.push(row);
      }
    }
  }

  sheets[sheetName] = { columns: sheetColumns, rows: sheetRows };

  return sheets;
}

function getSchemaName(fileName, sheetName) {
  const baseName = path.basename(fileName, '.csv');
  return `${baseName}_${sheetName}`.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase()).replace(/\s+/g, '');
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

async function processCsvFile(realm, schema, rows) {
  for (const row of rows) {
    const object = {};
    for (let i = 0; i < schema.properties.length; i++) {
      object[Object.keys(schema.properties)[i]] = row[i];
    }

    realm.write(() => {
      realm.create(schema.name, object);
    });
  }
}

main().catch((error) => {
  console.error(`An error occurred: ${error.message}`);
});
