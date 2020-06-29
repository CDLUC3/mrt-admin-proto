$(document).ready(
  function(){
    if ($("table.sortable")){
      var table = $("table.sortable")[0];
      sorttable.makeSortable(table);
    }
    $("#exportTable").on('click', function(){
      exportTable($('table tbody tr:visible'));
    });
  }
);

var Report = function() {
    var self = this;
    this.makeCsv = function(rows) {
        var itemdata = "";
        rows.each(function(rownum, row){
            itemdata += (rownum == 0) ? "" : "\r\n";
            $(row).find("td,th").each(function(colnum, col){
                itemdata += self.exportCol(colnum, col);
            });
        });
        return itemdata;
    }

    this.export = function(rows) {
    var itemdata = "data:text/csv;charset=utf-8," + this.makeCsv(rows);
        var encodedUri = encodeURI(itemdata);
        window.open(encodedUri);
    }

    //this is meant to be overridden for each report
    this.exportCol = function(colnum, col) {
        var data = "";
        data += (colnum == 0) ? "" : ",";
        data += self.exportCell(col);
        return data;
    }

    this.exportCell = function(col) {
        data = "\"";
        $(col).contents().each(function(i, node){
            if ($(node).is("hr")) {
                data += "||";
            } else {
                data += $(node).text().replace(/\n/g," ").replace(/"/g,"\"\"").replace(/\s/g," ");
                if ($(node).is("div:not(:last-child)")) {
                    data += "||";
                }
            }
        });
        data += "\"";
        return data;
    }
}

function exportTable(rows) {
  var ReportObj = new Report();
  ReportObj.export(rows);
}
