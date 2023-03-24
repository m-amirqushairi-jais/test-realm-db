const fs = require('fs');
const path = require('path');
const csvParser = require('csv-parser');
const xlsx = require('xlsx');
const Realm = require('realm');

const defaultInputFolder = 'csv_files'; // Replace with your files folder path
const defaultOutputRealm = 'default.realm'; // Replace with your desired default Realm database file path

async function main() {
  const { inputFiles, inputFolder, outputRealm } = parseArguments();
  let dataFiles;

  if (inputFiles.length > 0) {
    dataFiles = inputFiles.filter(file => file.endsWith('.csv') || file.endsWith('.xlsx'));
  } else {
    dataFiles = fs.readdirSync(inputFolder).filter(file => file.endsWith('.csv') || file.endsWith('.xlsx'));
    dataFiles = dataFiles.map(file => path.join(inputFolder, file));
  }

  const realmConfig = {
    path: outputRealm,
    schema: [],
    schemaVersion: 1,
  };

  for (const dataFile of dataFiles) {
    const isCsv = dataFile.endsWith('.csv');
    const columns = isCsv ? await parseCsvHeader(dataFile) : await parseXlsxHeader(dataFile);
    const schemaName = getSchemaName(dataFile);
    const schema = createRealmSchema(schemaName, columns);

    realmConfig.schema.push(schema);

    const realm = new Realm(realmConfig);

    if (isCsv) {
      await processCsvFile(dataFile, realm, schema);
    } else {
      await processXlsxFile(dataFile, realm, schema);
    }

    realm.close();
  }

  console.log(`Data files have been converted to Realm database '${outputRealm}'.`);
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

function parseXlsxHeader(xlsxFile) {
  return new Promise((resolve, reject) => {
    const workbook = xlsx.readFile(xlsxFile);
    const sheetName = workbook.SheetNames[0];
    const worksheet = workbook.Sheets[sheetName];
    const jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });
    const headers = jsonData[0];

    resolve(headers);
  });
}

async function processXlsxFile(xlsxFile, realm, schema) {
  const workbook = xlsx.readFile(xlsxFile);
  const sheetName = workbook.SheetNames[0];
  const jsonData = [];
  const jsonDataTemp = xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
  jsonDataTemp.forEach((res) => {
    jsonData.push(res)
  });
  // const worksheet = workbook.Sheets[sheetName];
  // const jsonData = xlsx.utils.sheet_to_json(worksheet);
  const dataRows = jsonData;
  for (const dataRow of dataRows) {
    const row = {};
    console.log(`the dataRow: ${dataRow}`);
    console.log(JSON.stringify(dataRow, null, 4));


    for (const key in dataRow) {
      console.log(`the dataRow key: ${key}`);

      row[key] = dataRow[key];
    }

    console.log(`the row: ${row}`);
    console.log(JSON.stringify(row, null, 4));
  }

  realm.write(() => {
    for (const dataRow of dataRows) {
      const row = {};

      for (const key in dataRow) {
        row[key] = dataRow[key];
      }

      realm.create(schema.name, row);
    }
  });
    
  // Reading our test file
  const file = xlsx.readFile(xlsxFile);
  let data = []
  const sheets = file.SheetNames
    
  for(let i = 0; i < sheets.length; i++)
  {
    console.log(i);
    const temp = xlsx.utils.sheet_to_json(file.Sheets[sheets[i]])
    temp.forEach((res) => {
      data.push(res)
    })
  }
    
  // Printing data
  console.log("Data value")
  // console.log(data)
}

main().catch((error) => {
  console.error(`An error occurred: ${error.message}`);
});
