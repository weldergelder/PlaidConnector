var AzureStorage = require('azure-storage');

FLATTEN_OBJECT = function(_input) {
	var toReturn = {};
	
	for (var i in _input) {
		if (!_input.hasOwnProperty(i)) continue;
		
		if ((typeof _input[i]) == 'object') {
			var flatObject = FLATTEN_OBJECT(_input[i]);
			for (var x in flatObject) {
				if (!flatObject.hasOwnProperty(x)) continue;
				
				toReturn[i + '_' + x] = flatObject[x];
			}
		} else {
			toReturn[i] = _input[i];
		}
	}
	return toReturn;
};

ADD_TO_AZURE_TABLE = function(_items, _table, _rowkey, _partitionkey){
    var items = [];
    var entGen = AzureStorage.TableUtilities.entityGenerator;
    if(_items){
        Array.prototype.forEach.call(_items, item => {
            var PartitionKey = entGen.String(item[_partitionkey]);
            var RowKey = entGen.String(item[_rowkey]);
            item = FLATTEN_OBJECT(item);
            Object.keys(item).forEach((property) => { item[property] = entGen.String(item[property]); });
            item.PartitionKey = PartitionKey;
            item.RowKey = RowKey;
            items.push(item);
        });
    }
    var size = items.length;
    console.log('About to transmit ' + size + ' records to Azure Tables');
    Array.prototype.forEach.call(items, item => {
        ADD_RECORD(item, _table);  
    });
    
};

ADD_RECORD = function(_payload, _table) {
    var tableService = AzureStorage.createTableService('plaidstore', 'r0cJk7eHwyPjp1t0uOVgmr3zyFzJG2R5rliYyO41lNph1ySoL/08GwHeusf20R60H9bWDj5WftnejMHYRaC+jQ==');
    tableService.createTableIfNotExists(_table, function(error, result, response){
        if(!error){
            tableService.insertEntity(_table, _payload, function(error, result, response) {
                if(!error){
                    console.log('entity added successfully');
                    console.log(result);
                } else {
                    console.log(error);
                }
            });
        };
    });
}    