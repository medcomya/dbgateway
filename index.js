var mysql = require('mysql');
var queries = require('./queries');

module.exports = function (config) {

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

    var pool = mysql.createPool(config);

    function close() {
        pool.end(function (err) {
        });
    }

    function expressionArg(val, prefix, postfix){
        var expression = '?? ' + val + '?';
        return function(arg){
            if(prefix){
                arg = prefix + arg;
            }
            if(postfix){
                arg = arg + postfix;
            }

            return function(key){
                return mysql.format(expression, [key, arg]);
            }
        };
    }

    var expressions = {
        equal: expressionArg('='),
        notEqual: expressionArg('<>'),
        above: expressionArg('>'),
        aboveAndEqual: expressionArg('>='),
        below: expressionArg('<'),
        belowAndEqual: expressionArg('<='),

        startWith: expressionArg('like', '', '%'),
        endWith: expressionArg('like', '%'),
        contains: expressionArg('like', '%', '%'),

        and: 0,
        or: 1
    };

    return {
        get: get,
        getList: getList,
        close: close,
        config: config,
        expressions: expressions
    };



    function init(options) {

        if (!options) {
            return {
                error : 'Invalid args'
            };
        }

        if (typeof options === 'function') {
            return new options();
        }

        return options;
    }

    function processOptions(options){
        options = options || {
            limit: 10
        };

        var operation = 0;
        var where;

        if(options.where){
            var aWhere = [];
            for(var key in options.where){
                if(options.where.hasOwnProperty(key)) {
                    if(key === 'operation') {
                        operation = options.where[key];
                    } else {
                        aWhere.push(options.where[key](key));
                    }
                    //console.log("obj." + prop + " = " + obj[prop]);
                }
            }

            where = aWhere.join(operation ? ' or ': ' and ');

        }

        if(!where){
            where = '1=1';
        }


        return {
            options : options,
            where : where
        };
    }

    function getList(type, options) {
        return processQuery(function () {
            type = init(type);
            var ops = processOptions(options);
            type.query = queries.getList + ops.where + ' limit 10';
            type.params = [type.listFields, type.table];
            return type;
        });
    }

    function get(type, id) {
        return processQuery(function () {
            type = init(type);
            type.query = queries.get;
            type.params = [type.objectFields, type.table, type.keyFiled, id];
            return type;
        });
    }

    function processQuery(queryOptions) {
        return new Promise(function (resolve, reject) {

            if (typeof queryOptions === 'function') {
                queryOptions = queryOptions();
            }

            var error = queryOptions.error;
            if (error) {
                reject(err);
                return;
            }
            pool.getConnection(function (err, connection) {
                if (err) {
                    reject(err);
                    connection.release();
                    return;
                }
                var queryFunction = function (err, result) {
                    if (err) {
                        reject(err);
                        connection.release();
                        return;
                    }
                    if (queryOptions.processResult) {
                        result = queryOptions.processResult(result, reject);
                    }
                    connection.release();
                    resolve(result);
                };
                if (queryOptions.params) {
                    connection.query(queryOptions.query, queryOptions.params, queryFunction);
                } else {
                    connection.query(queryOptions.query, queryFunction);
                }
            });
        });
    }
};
