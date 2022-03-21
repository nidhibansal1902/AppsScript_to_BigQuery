function upload() {
 var Esheet = SpreadsheetApp.getActiveSheet();
 var project_id = 'sacred-armor-202709'
 var data_set_id = 'new_sheet';
 var table_id = 'NewSheet_data';
 var range = Esheet.getRange('A1:F8');
 var writeDisposition = 'WRITE_EMPTY';
 var has_header = true;
 var schema_bq = 'automatic';
 
 upload_to_BigQ(range,project_id,data_set_id,table_id,writeDisposition,has_header,schema_bq)
 
}
function upload_to_BigQ(range,projectId,datasetId,tableId,writeDisposition,has_header,schema_bq) {
  
   if(typeof writeDisposition == "undefined"){
    writeDisposition = 'WRITE_EMPTY'
  }
    
  if(typeof has_header == "undefined"||has_header == true){
    has_header = 1;
  }else{
    has_header = 0;
  }
 
 
   var data = range.getValues();
   var csvFile = undefined;
  
      
        if (data.length > 1) {
      var csv = "";
      for (var row = 0; row < data.length; row++) {
        for (var col = 0; col < data[row].length; col++) {
          if (data[row][col].toString().indexOf(",") != -1) {
            data[row][col] = "\"" + data[row][col] + "\"";
          }
        }

        // join each row's columns
        // add a carriage return to end of each row, except for the last one
        if (row < data.length-1) {
          csv += data[row].join(",") + "\r\n";
        }
        else {
          csv += data[row];
        }
      }
      csvFile = csv;
    }
  
   //   return csvFile;
  
  var csv_name = 'temp_' + new Date().getTime()+'.csv'
  
  DriveApp.createFile(csv_name, csvFile)
  
  
   var files = DriveApp.getFilesByName(csv_name);
 while (files.hasNext()) {
   var file = files.next();


  
   var table = {
    tableReference: {
      projectId: projectId,
      datasetId: datasetId,
      tableId: tableId
    },
  };
  

  

   try {
     table = BigQuery.Tables.insert(table, projectId, datasetId)}
   catch(e) {
   }
  
  
   var data = file.getBlob().setContentType('application/octet-stream');

   if(typeof schema_bq == "undefined"|| schema_bq == false || schema_bq == 'automatic'){
   
  // Create the data upload job.
  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        skipLeadingRows: has_header,
        autodetect: true,
        writeDisposition: writeDisposition
      }
    }
  };
}else{
  
    // Create the data upload job.
  var job = {
    configuration: {
      load: {
        destinationTable: {
          projectId: projectId,
          datasetId: datasetId,
          tableId: tableId
        },
        skipLeadingRows: has_header,
        schema:schema_bq,
        writeDisposition: writeDisposition
      }
    }
  };
  
}
  job = BigQuery.Jobs.insert(job, projectId, data);
     
     file.setTrashed(true);

   
      }
 }