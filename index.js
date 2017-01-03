
var mysql      = require('mysql');
var queries      = require('queries');

module.exports = function(config) {

	config = config || {};
	config.connectionLimit = config.connectionLimit || 10;
    //
	//config.queryFormat = function (query, values) {
	//  	if (!values) return query;
	//  	return query.replace(/(\?)?\?(\w+)/g, function (match, mark, key) {
	//    		if (values.hasOwnProperty(key)) {
	//    		        var value = values[key];
	//      			return mark ? this.escapeId(value) : this.escape(value);
	//    		}
	//    		return match;
	//  	}.bind(this));
	//};

	var pool  = mysql.createPool(config);



	return {
		get : get,
		config : config
	};

	function get(options, id){
		return processQuery(function () {
			var errors;
			if(!options) {
				errors = 'Invalid args';
			} else {
				if(typeof options === 'function'){
					options = options();
				}
			}

			return {
				init:init,
				query: queries.get,
				params: [options.objectFields, options.table, options.keyFiled, id]
			};

			function init(){
				return error;
			}
			
		});
	}

	function processQuery(queryOptions){
                   return new Promise(function(resolve, reject){

			if(typeof queryOptions === 'function'){
				queryOptions = queryOptions();
			}

			var error = queryOptions.init && queryOptions.init();
			if(error){
				reject(err);
				return;	
			}
			pool.getConnection(function(err, connection) {
				if (err) {
					reject(err);
					connection.release();
					return;
				}
                                var queryFunction = function(err, result) {
                                        if (err) {
						reject(err);
						connection.release();
						return;
					}
					if(queryOptions.processResult) {
	                                        result = queryOptions.processResult(result, reject);
					}
					connection.release();
					resolve(rows);
				}
				if(queryOptions.params) {
					connection.query(queryOptions.query, queryOptions.params, queryFunction);
				} else {
                                 	connection.query(queryOptions.query, queryFunction);
				}
			});
		});
	}

};

function User(){
	return {
		table: 'users',
		keyFiled:'Id',
		listFields: ['','','',''],
		objectFields: ['','','','']
	}
}
