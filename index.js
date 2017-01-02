
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

	function get(options){
		return new Promise(function(resolve,reject){
			
			pool.getConnection(function(err, connection) {
				if (err) {
					reject(err);
					return;
				}
				// Use the connection
				connection.query( queries.get, function(err, rows) {
					if (err) {
						reject(err);
						return;
					}
					// And done with the connection.
					connection.release();
					resolve(rows);
				});
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
