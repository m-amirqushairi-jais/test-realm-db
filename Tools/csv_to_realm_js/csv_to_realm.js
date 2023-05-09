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
    const fileType = getFileType(dataFile);
    let headerData;
  
    if (fileType === 'csv') {
      headerData = await parseCsvHeader(dataFile);
    } else if (fileType === 'xlsx') {
      headerData = await parseXlsxHeader(dataFile);
    } else {
      console.warn(`Skipping unsupported file type: ${dataFile}`);
      continue;
    }
  
    const { headers, dataTypes } = headerData;
    const schemaName = getSchemaName(dataFile);
    const schema = createRealmSchema(schemaName, headers, dataTypes);
  
    realmConfig.schema.push(schema);
  
    const realm = new Realm(realmConfig);
    if (fileType === 'csv') {
      await processCsvFile(dataFile, realm, schema);
    } else {
      await processXlsxFile(dataFile, realm, schema);
    }
    realm.close();
  }

  console.log(`Data files have been converted to Realm database '${outputRealm}'.`);
}

function getFileType(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === '.csv') {
    return 'csv';
  } else if (ext === '.xlsx') {
    return 'xlsx';
  }
  return null;
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

// function parseCsvHeader(csvFile) {
//   return new Promise((resolve, reject) => {
//     const columns = [];
//     fs.createReadStream(csvFile)
//       .pipe(csvParser())
//       .on('headers', (headers) => {
//         resolve(headers);
//       })
//       .on('error', reject);
//   });
// }

// function parseCsvHeader(dataFile) {
//   return new Promise((resolve, reject) => {
//     const rows = [];
//     fs.createReadStream(dataFile)
//       .pipe(csvParser())
//       .on('data', (row) => {
//         rows.push(row);
//         console.log("csv header: " + JSON.stringify(rows, null, 4));
//         if (rows.length === 2) {
//           resolve({ headers: rows[0], dataTypes: rows[1] });
//         }
//       })
//       .on('error', reject);
//   });
// }

function parseCsvHeader(dataFile) {
  return new Promise((resolve, reject) => {
    const rows = [];
    fs.createReadStream(dataFile)
      .pipe(csvParser({ headers: false }))
      .on('data', (row) => {
        rows.push(row);
        if (rows.length === 2) {
          resolve({ headers: rows[0], dataTypes: rows[1] });
        }
      })
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
    const dataTypes = jsonData[1];

    resolve({ headers, dataTypes });
  });
}

function getSchemaName(fileName) {
  const baseName = path.basename(fileName, '.csv');
  return baseName.replace(/_/g, ' ').replace(/\b\w/g, char => char.toUpperCase()).replace(/\s+/g, '');
}

// function createRealmSchema(schemaName, columns) {
//   const schema = {
//     name: schemaName,
//     properties: {}
//   };

//   for (const column of columns) {
//     schema.properties[column] = 'string';
//   }

//   return schema;
// }

function createRealmSchema(schemaName, columns, dataTypes) {
  const schema = {
    name: schemaName,
    properties: {},
  };

  for (let i = 0; i < columns.length; i++) {
    const column = columns[i];
    const dataType = dataTypes[i] || 'string';
    schema.properties[column] = dataType;
  }

  return schema;
}

function validateDataType(value, dataType) {
  try {
    switch (dataType.toLowerCase()) {
      case 'string':
        return typeof value === 'string';
      case 'int':
      case 'integer':
        return Number.isInteger(value);
      case 'float':
        return typeof value === 'number' && !Number.isInteger(value);
      case 'double':
        return typeof value === 'number';
      case 'bool':
      case 'boolean':
        return typeof value === 'boolean';
      case 'date':
        return value instanceof Date;
      default:
        console.warn(`Unknown data type: ${dataType}. Skipping validation.`);
        return true;
    }
  } catch (error) {
    console.warn(`Error while validating data type: ${error.message}. Skipping validation.`);
    return true;
  }
}

// function processCsvFile(dataFile, realm, schema) {
//   return new Promise((resolve, reject) => {
//     fs.createReadStream(dataFile)
//       .pipe(csvParser())
//       .on('data', (row) => {
//         realm.write(() => {
//           realm.create(schema.name, row);
//         });
//       })
//       .on('end', resolve)
//       .on('error', reject);
//   });
// }

function processCsvFile(csvFile, realm, schema) {
  return new Promise((resolve, reject) => {
    const columns = Object.keys(schema.properties);
    fs.createReadStream(csvFile)
      .pipe(csvParser())
      .on('data', (row) => {
        const isValid = columns.every((column) => {
          return validateDataType(row[column], schema.properties[column]);
        });

        if (isValid) {
          realm.write(() => {
            realm.create(schema.name, row);
          });
        } else {
          console.warn(`Skipping row with invalid data types: ${JSON.stringify(row)}`);
        }
      })
      .on('end', resolve)
      .on('error', reject);
  });
}

// function parseXlsxHeader(xlsxFile) {
//   return new Promise((resolve, reject) => {
//     const workbook = xlsx.readFile(xlsxFile);
//     const sheetName = workbook.SheetNames[0];
//     const worksheet = workbook.Sheets[sheetName];
//     const jsonData = xlsx.utils.sheet_to_json(worksheet, { header: 1 });
//     const headers = jsonData[0];

//     resolve(headers);
//   });
// }

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
