var AzureStorage = require('azure-storage');

CONNECT_TO_AZURE_TABLES = function() {
    var tableService = AzureStorage.createTableService('plaidstore', 'r0cJk7eHwyPjp1t0uOVgmr3zyFzJG2R5rliYyO41lNph1ySoL/08GwHeusf20R60H9bWDj5WftnejMHYRaC+jQ==');
    console.log('Tracing 1: Table Service Connection Created');
    tableService.createTableIfNotExists('PlaidTransactionDataDump', function(error, result, response){
        if(!error){
            console.log('Tracing 2: accounts table is now available');
            console.log(result.created);
            console.log(response);
        } else {
            console.log('Tracing 2: error while verifying table creation');
            console.log(response);
        }
    });
};

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

ADD_TO_AZURE_TABLE = function(_items){
    var items = [];
    var entGen = AzureStorage.TableUtilities.entityGenerator;
    console.log(_items.length);
    if(_items){
        Array.prototype.forEach.call(_items, item => {
            var PartitionKey = entGen.String(item.account_id);
            if(item.transaction_id){
                var RowKey = entGen.String(item.transaction_id);
            } else {
                var RowKey = entGen.String(item.account_id);
            }
            console.log('BEFORE\n');
            console.log(item);
            item = FLATTEN_OBJECT(item);
            Object.keys(item).forEach((property) => {
                item[property] = entGen.String(item[property]);
            });
            item.PartitionKey = PartitionKey;
            item.RowKey = RowKey;
            items.push(item);
            console.log(item);
        });
    }
    var size = items.length;
    console.log('About to transmit ' + size + ' records to Azure Tables');
    Array.prototype.forEach.call(items, item => {
        ADD_RECORD(item, 'PlaidTransactionDataDump');  
    });
    
};

ADD_RECORD = function(_payload, _table) {
    console.log(_payload.PartitionKey);
    console.log(_payload.RowKey);
    var tableService = AzureStorage.createTableService('plaidstore', 'r0cJk7eHwyPjp1t0uOVgmr3zyFzJG2R5rliYyO41lNph1ySoL/08GwHeusf20R60H9bWDj5WftnejMHYRaC+jQ==');
    tableService.insertEntity(_table, _payload, function(error, result, response) {
        if(!error){
            console.log('entity added successfully');
            console.log(result);
        } else {
            console.log(error);
            
        }
    });
};