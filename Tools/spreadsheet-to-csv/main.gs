// ExportSpreedToCsv
// Local環境のSpreadsheetからTSVを出力する
function exportLocalCsv() {
  // (初期設定)Local環境のSpreadsheet格納フォルダID
  var spreadSheetFolderId = "1S_M_wtvCPBUgEhdEnDf5u8aRwobtncxt";
 
  // (初期設定)Local環境のCSVファイル出力フォルダID
  var csvFolderId = "1xGlw8HXWE3KmMKu3uUlkz39RduLKNwxa";
 
  // ファイルタイプをスプレッドシートに指定
  var filetype = "application/vnd.google-apps.spreadsheet";
 
  // スプレッドシートファイル
  var files = DriveApp.getFolderById(spreadSheetFolderId).getFilesByType(filetype);
 
  // 出力先フォルダ
  var tsvFolder = DriveApp.getFolderById(csvFolderId);
 
  while (files.hasNext()) {
    const file = files.next();
    const fileId = file.getId();
    var spreadSheet = SpreadsheetApp.openById(fileId);
    var sheetArray = spreadSheet.getSheets();
    for(var i = 0; i < sheetArray.length; i++)
    {
        var sheet = sheetArray[i];
        writeCSV(spreadSheet, sheet.getSheetName(), tsvFolder);
    }
  }
}

// Check trash folder exit or not
function checkFolderExist(folderIterator)
{
  var folder = null;

  if(folderIterator == null)
    return folder;

  while (folderIterator.hasNext()) {
    folder = folderIterator.next();
    Logger.log(folder.getName());
    break;
  }
  return folder;
}

function formatStringData(format, value)
{
  var formatVale = "";

  if(format != "#,##0")
    return value;

  value = value.toString();

  var index = 1;
  for (var i = value.length - 1; i >= 0; i--) 
  {
    formatVale =  value.charAt(i) + formatVale;
    if(index%3 == 0 & index != value.length)
      formatVale = "," + formatVale;

    index++;
  }
  return formatVale;
}

function deleteOrRemove( file ) {
  var trashFolderName = Session.getActiveUser().getEmail() + "-trash";
  var folderIterator = DriveApp.getFoldersByName(trashFolderName);
  var trashFolder = checkFolderExist(folderIterator);

  if(trashFolder == null) {
    trashFolder = DriveApp.createFolder(trashFolderName);
    console.log("## Create trashFolder folder:" + trashFolderName)
  } else {
    console.log("## Found trashFolder folder:"+ trashFolderName);
  }
  
  var myAccess = file.getAccess(Session.getActiveUser());

  if (myAccess == DriveApp.Permission.OWNER) {
    file.setTrashed(true);
  }
  else {
    file.moveTo(trashFolder);
  }
}

// CSV出力
function writeCSV(spreadSheet, sheetName, tsvFolder) {
  var sheet = spreadSheet.getSheetByName(sheetName);
 
  // 最終列
  const lastCol = findLastCol(sheet, 2);
  const lastRow = sheet.getLastRow();
 
  const contentType = 'text/csv';
 
  // スプレッドシートの名前を使うのでマスターテーブル名と合わせておくこと
  var outputFileName = sheetName + '.csv';
 
  console.log("outputFileName:" + outputFileName);

  // Googleドライブは同名ファイルが別IDで作れるので既存のファイルは削除
  var deleteFiles = DriveApp.getFilesByName(outputFileName);
 
  while (deleteFiles.hasNext()) {
    // 削除
    deleteOrRemove(deleteFiles.next());
  }
 
  // カラムを解析、時間を取る
  var columnNames = sheet.getRange(2, 1, 1, lastCol).getValues();
  console.log("columnNames:" + columnNames);
 
  var skipColumnList = {};
  columnNames.forEach(function(row, rowId) {
      row.forEach(function(col, colId) {
          console.log("## columnNames colId:" + colId + " val:" + columnNames[rowId][colId]);

          if(columnNames[rowId][colId].indexOf("__") > -1)
          {
            skipColumnList[colId] = colId;
            console.log("## found skipped columnNames colId:" + colId);
          }

          temp = columnNames[rowId][colId].toLowerCase();
          if(colId > 0 && columnNames[rowId][colId] == temp)
          {
            SpreadsheetApp.getUi().alert("Alarm", "Found lowercase column:" + columnNames[rowId][colId] + " in sheet:" + sheetName, SpreadsheetApp.getUi().ButtonSet.OK);
          }

      });
  });
 
 
  // カラムを解析、時間を取る
  var columnTypes = sheet.getRange(3, 1, 1, lastCol).getValues();
  console.log("columnTypes:" + columnTypes);
 
  var timeList = {};
  columnTypes.forEach(function(row, rowId) {
      row.forEach(function(col, colId) {
          //console.log("## columnTypes colId:" + colId + "val:" + columnTypes[rowId][colId]);
          if( columnTypes[rowId][colId] == 'time')
          {
            timeList[colId] = colId;
          }
      });
  });
 
  // ヘッダー部（テーブルのカラム名）
  var data = sheet.getRange(2, 1, 2, lastCol).getValues();
 
 
  // __カラムのヘッダーを取り除く
  var csv = '';
  var charset = 'UTF-8';
  for(var i = 0; i < data.length; i++)
  {
    //console.log("data[+ " + i + "]" + data[i]);
    data[i].forEach(function(col, colId) {
      if(skipColumnList[colId] != colId)
      {
          if(colId == data[i].length - 1)
            csv +=  col + "\n";
          else
            csv +=  col + ',';
      }
    });
  }
 
  // データ部のスタート行Index
  const dataStartRowIndex = 5;
 
  // データ部のスタート列Index
  const dataStartColIndex = 1;
 
  if (lastRow >= dataStartRowIndex) {
    // データ部
    var dataNum = lastRow - dataStartRowIndex + 1;
    //var data = sheet.getRange(dataStartRowIndex, dataStartColIndex, dataNum, lastCol).getValues();  
    
    var cell = sheet.getRange(dataStartRowIndex, dataStartColIndex, dataNum, lastCol);
    var data = cell.getValues();
    var formats = cell.getNumberFormats();
    
    
    // 時刻のフォーマットを変更する
    data.forEach(function(row, rowId) {
        row.forEach(function(col, colId) {            
            //console.log("### Cell Format:" + formats[rowId][colId] + " Value:" + data[rowId][colId]); 
            if(colId > 0 && formats[rowId][colId] == '')
            {
               var temp = "\"" + data[rowId][colId] + "\"";
               data[rowId][colId] = temp;
               console.log("### Found Cell Format:" + formats[rowId][colId] + " Update Value:" + data[rowId][colId]); 
            }

            if(timeList[colId] == colId)
            { // Japan timezone
              data[rowId][colId] = Utilities.formatDate(data[rowId][colId], "GMT+9", "yyy-MM-dd HH:mm:ss");
              console.log(">> New format:" +  data[rowId][colId]);
           
            } 

        });
    });
 
    // __カラムのデータを取り除く
    for(var i = 0; i < data.length; i++) 
    {
      var temp = '';
      data[i].forEach(function(col, colId) 
      {        
        if( skipColumnList[colId] != colId)
        {
            if(colId == data[i].length - 1)
              temp +=  col;
            else
              temp +=  col + ',';
        }
      });    
      //console.log("newRow:" + temp);
      csv += temp + "\n";
    }
  }
 
  var blob = Utilities.newBlob('', contentType, outputFileName).setDataFromString(csv, charset);
  tsvFolder.createFile(blob);
}
 
// 指定行の「最終列番号」を返す
function findLastCol(sheet, row) {
 
  // 指定の行を二次元配列に格納する ※シート全体の最終行までとする
  var RowValues = sheet.getRange(row, 1, 1, sheet.getLastColumn()).getValues();

 
  //二次元配列を一次元配列に変換する
  RowValues = Array.prototype.concat.apply([], RowValues);
  var lastCol = RowValues.filter(String).length;
  return lastCol;
}