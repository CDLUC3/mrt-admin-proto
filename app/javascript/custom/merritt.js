$(document).ready(
  function(){
    if ($("table.sortable")){
      var table = $("table.sortable")[0];
      sorttable.makeSortable(table);
    }
  }
);
