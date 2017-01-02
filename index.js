module.exports = function(config) {

	config = config || {};

	config.queryFormat = function (query, values) {
	  	if (!values) return query;
	  	return query.replace(/(\?)?\?(\w+)/g, function (txt, mark, key) {
	    		if (values.hasOwnProperty(key)) {
	    		        var value = values[key];
	      			return mark ? this.escapeId(value) : this.escape(value);
	    		}
	    		return txt;
	  	}.bind(this));
	};


	return {
		get : get,
		config : config
	};

	function get(){
}

}


