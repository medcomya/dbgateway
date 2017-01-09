var mysql = require('mysql');
var queries = require('./queries');
var endOfLine = require('os').EOL;

module.exports = function (config) {

    config = config || {};
    config.connectionLimit = config.connectionLimit || 10;

    var pool = mysql.createPool(config);

    var expressions = {
        equal: expressionArg('='),
        equalField: expressionField('='),
        notEqual: expressionArg('<>'),
        above: expressionArg('>'),
        aboveAndEqual: expressionArg('>='),
        below: expressionArg('<'),
        belowAndEqual: expressionArg('<='),

        startWith: expressionArg('LIKE', '', '%'),
        endWith: expressionArg('LIKE', '%'),
        contains: expressionArg('LIKE', '%', '%'),

        and: 0,
        or: 1
    };

    var contract = {
        query: query,
        get: get,
        insert: insert,
        update: update,
        createRepository: createRepository,
        close: close,
        createSchema: createSchema,
        config: config,
        expressions: expressions,
        order: orderFunc(),
        orderDesc: orderFunc(1)
    };
    contract['delete'] = _delete;
    return contract;

    function createRepository(type){
        type = init(type);
        var repository = {
            get     : function(options){return get(type, options);},
            insert  : function(obj){return insert(type, obj);},
            update  : function(obj, options){return update(type, obj, options);},
            query   : function(){return query(type);},
            type    : type,
            expressions : expressions,
            order       : orderFunc(),
            orderDesc   : orderFunc(1)
        };
        repository['delete'] = function(options){return _delete(type, options);};
        return repository;
    }

    function query(type){
        type = init(type);
        var options = {orderBy : {}, joins: []};
        var queryContract = {
            get     : function(){return get(type, options);},
            update  : function(obj){return update(type, obj, options);},
            where   : function(obj){options.where = obj; return queryContract;},
            join    : addJoin,
            limit   : function(l, o){
                options.limit = l;
                if(o){
                    options.offset = o;
                }
                return queryContract;
            },
            offset    : function(o){options.offset = o; return queryContract;},
            orderBy   : function(field, table){options.orderBy[field] = orderFunc()(table); return queryContract;},
            orderByDesc  : function(field, table){options.orderBy[field] = orderFunc(1)(table); return queryContract;},
            processRow   : function(pr){options.processRow = pr; return queryContract;}
        };
        queryContract['delete'] = function(options){return _delete(type, options);};
        return queryContract;

        function addJoin(typeOrJoinParams, joinType, on, alias, fields){
            if(typeof typeOrJoinParams ===  'function' || (
                typeof typeOrJoinParams === 'object' &&
                typeOrJoinParams.table && typeOrJoinParams.keys && typeOrJoinParams.properties
                )){
                options.joins.push({
                    joinType : joinType,
                    type : typeOrJoinParams,
                    fields : fields,
                    on : on,
                    as : alias
                })
            } else if(typeof typeOrJoinParams === 'object'){
                options.push(typeOrJoinParams);
            }
            return queryContract;
        }
    }



    function close() {
        pool.end(function (err) { });
    }

    function expressionArg(val, prefix, postfix){
        var expression = '?? ' + val + ' ?';
        return function(arg, table){
            if(prefix){
                arg = prefix + arg;
            }
            if(postfix){
                arg = arg + postfix;
            }

            return function(key, mainTable){
                if(mainTable){
                    return (table ? table : mainTable) + '.' + key + ' ' + val + ' ' + mysql.escape(arg);
                }
                return mysql.format(expression, [key, arg]);
            }
        };
    }

    function expressionField(val){
        var expression = '?? ' + val + ' ??';
        return function(arg, table){

            return function(key, mainTable){
                if(mainTable){
                    return mainTable + '.' + key + ' ' + val + ' ' + (table ? table : mainTable) + '.' + arg;
                }
                return mysql.format(expression, [key, arg]);
            }
        };
    }

    function init(type) {
        if (!type) {
            return {error : 'Invalid args'};
        }
        if (typeof type === 'function') {
            return new type();
        }
        return type;
    }

    function processWhere(where, table) {
        var aWhere = [];
        var operation = 0;
        for (var key in where) {
            if (where.hasOwnProperty(key)) {
                if (key === '$group') {
                    var group = processWhere(where[key], table);
                    if(group){
                        aWhere.push('(' + group + ')');
                    }
                } else if (key === '$operation') {
                    operation = where[key];
                } else {
                    aWhere.push(where[key](key, table));
                }
            }
        }
        return aWhere.join(operation ? ' OR ' : ' AND ');
    }

    function orderFunc(isDesc){
        return function(table){
            return {desc : isDesc, table : table};
        };
    }

    function makeField(key, mainTable, table){
        table = table || mainTable;
        return mysql.format('??', [mainTable ? table + '.' + key : key]);
    }

    function makeDirection(isString, direction){
        if(isString){
            return direction ? ' ' + direction : '';
        }
        return (direction === 1 ? ' DESC' : '');
    }

    function processOrderBy(orderBy, mainTable){
        var orders = [];
        for (var key in orderBy){
            if (orderBy.hasOwnProperty(key)) {
                var order = orderBy[key];
                var type = typeof order;
                var field = makeField(key, mainTable, type === 'object' ? order.table : undefined);
                orders.push(field + makeDirection(type === 'string', order.desc || order));
            }
        }
        if(orders.length === 0){
            return '';
        }
        return ' ORDER BY ' + orders.join(', ') + endOfLine;
    }

    function prepareFields(type, ops, isJoin){
        var table = ops.as ? ops.as : type.table;
        var fields = ops.fields;
        return (fields ? fields : type.listFields).map(function(field){
            return mysql.format('??.?? AS ??', [table, field, isJoin ? table + '_' + field : field]);
        });
    }

    function processOptions(options, type, flds){
        options = options || {
            limit: 100
        };
        var joins = options.joins || [];
        var where;
        var mainTable = joins.length > 0 ? type.table : undefined;
        if (options.where){
            where = processWhere(options.where, mainTable);
        }

        var ops = {
            options : options,
            where : where ? ' WHERE ' + where + endOfLine : '',
            joins : '',
            orderBy : '',
            limit: '',
            offset: ''
        };

        if (joins.length > 0){
            var sJoins = [];
            ops.fields = flds || prepareFields(type, options);
            joins.forEach(function(join){
                join.as = join.as || join.alias;
                var type = init(join.type);
                if(!type.error){
                    join.fields = flds || join.fields || [];
                    ops.fields = ops.fields.concat(prepareFields(type, join, 1));
                    var table = join.as ? type.table + ' AS ' + join.as : type.table;
                    sJoins.push(' ' + join.joinType +' JOIN ' + table +  ' ON '+ processWhere(join.on, join.as ? join.as : type.table))
                }
            });
            if(sJoins.length > 0){
                ops.joins = sJoins.join(endOfLine) + endOfLine;
            }
        } else {
            ops.fields = flds || (options.fields ? options.fields : type.listFields);
        }
        ops.fields = flds || ops.fields.join(', ');
        if (options.orderBy){
            ops.orderBy = processOrderBy(options.orderBy, mainTable);
        }
        if (options.limit){
            ops.limit = ' LIMIT ' + options.limit + endOfLine;
        }
        if (options.offset){
            ops.offset = ' OFFSET ' + options.offset + endOfLine ;
        }
        return ops;
    }

    function getList(type, options) {
        var promise = processQuery(function () {
            type = init(type);
            var ops = processOptions(options, type);
            type.eventMode = !!options.processRow;
            type.query = ' SELECT ' + ops.fields + ' FROM ' + type.table + ops.joins +
            ops.where + ops.orderBy + ops.limit + ops.offset;
            console.log(type.query);
            return type;
        });

        if(options.processRow){
            promise = promise.then(function(queryParams){
                return new Promise(function(resolve, reject){
                    var connection = queryParams.connection;
                    connection.resume();
                    queryParams.query.on('result', function(row) {
                        connection.pause();
                        options.processRow(row);
                        connection.resume();
                    }).on('end', function() {
                        connection.release();
                        resolve();
                    }).on('error', function(err) {
                        reject(err);
                    });
                });
            });
        }
        return promise;
    }

    function getObject(type, id) {
        return processQuery(function () {
            type = init(type);
            type.query = queries.get;
            type.params = [type.objectFields, type.table, type.primaryFiled, id];
            return type;
        });
    }

    function get(type, options) {
        if(typeof options === 'number'){
            return getObject(type, options);
        } else {
            return getList(type, options);
        }
    }

    function insert(type, obj){
        return processQuery(function () {
            type = init(type);
            type.query = queries.insert;
            type.params = [type.table, obj];
            return type;
        });
    }

    function update(type, obj, options){
        return processQuery(function () {
            type = init(type);
            var ops;
            if(typeof options === 'number'){
                ops = {where : mysql.format(' WHERE ?? = ?', [type.primaryFiled, options])};
            } else {
                ops = processOptions(options, type, 1);
            }
            type.query = queries.update + ops.where;
            type.params = [type.table, obj];
            return type;
        });
    }

    function _delete(type, options){
        return processQuery(function () {
            type = init(type);
            var ops;
            if(typeof options === 'number'){
                ops = {where : mysql.format(' WHERE ?? = ?', [type.primaryFiled, options])};
            } else {
                ops = processOptions(options, type, 1);
            }
            type.query = queries._delete + ops.where;
            type.params = [type.table];
            return type;
        });
    }

    function createSchema() {
        return processQuery(function () {
            return {
                query: queries.showTables
            };
        }).then(function(result){
            var fieldName = result.fields[0].name;
            var tables = result.results.map(function(obj){
                return obj[fieldName];
            });
            var query = tables.map(function(table){
                return mysql.format(queries.showTableDetails, [table, table]);//'SHOW COLUMNS FROM `' + table[fieldName] + '`';
            }).join(';');
            return processQuery(function () {
                return {
                    query: query
                };
            }).then(function(result){
                result.tables = tables;
                return result;
            });
        }).then(function(result){
            var classes = [];
            var tables = [];
            result.tables.forEach(function(table, idx){

                var className = capitalize(manyToOne(table));
                tables.push(className);

                var columns = result.results[2 * idx];
                var dbKeys = result.results[2 * idx + 1];

                var listFields = columns.filter(function(column){
                    return column.Key;
                }).map(function(column){
                    return column.Field;
                });

                var objectFields = columns.map(function(column){
                    return column.Field;
                });

                var keys = {};
                var properties = {};
                columns.forEach(function(column){

                    properties[column.Field] = {
                        _type : column.Type,
                        _null : column.Null
                    };
                    if(column.Default !== null){
                        properties[column.Field]._default = column.Default;
                    }
                    if(column.Extra){
                        properties[column.Field]._extra = column.Extra;
                    }
                    if(column.Key){
                        var filteredKeys = dbKeys.filter(function(key){ return key.Column_name === column.Field;});
                        filteredKeys.forEach(function(key){
                            if(keys[key.Key_name]){
                                var k = keys[key.Key_name];
                                k.fields.push({
                                    name: key.Column_name,
                                    seq: key.Seq_in_index
                                });
                            } else {
                                keys[key.Key_name] = {
                                    fields : [{
                                        name: key.Column_name,
                                        seq: key.Seq_in_index
                                    }],
                                    unique: !key.Non_unique
                                };
                            }
                        });

                        properties[column.Field]._key = filteredKeys[0].Key_name;
                    }
                });

                for (var key in keys){
                    if (keys.hasOwnProperty(key)) {
                        if(keys[key].fields.length > 1){
                            keys[key].fields = keys[key].fields.sort(function(f1, f2){
                                return f1.seq - f2.seq;
                            });
                        }
                        keys[key].fields = keys[key].fields.map(function(field){
                            return field.name;
                        });
                    }
                }

                var cl = 'function ' +className + '(){' + endOfLine +
                '\tthis.table = \'' + table + '\';' + endOfLine +
                '\tthis.primaryFiled = \'Id\';' + endOfLine +
                '\tthis.keys = ' + makeProperties(keys) + //JSON.stringify(keys).replace(/"/gi,'\'') + ';' + endOfLine +
                '\tthis.listFields = ' + JSON.stringify(listFields).replace(/"/gi,'\'') + ';' + endOfLine +
                '\tthis.objectFields = ' + JSON.stringify(objectFields).replace(/"/gi,'\'') + ';' + endOfLine +
                '\tthis.properties = ' + makeProperties(properties) +
                '}';
                classes.push(cl);
            });

            var sClasses = classes.join(endOfLine + endOfLine);
            if(process.argv.some(function (val) {
                return val === 'module';
            })){
                return 'module.exports = {' + endOfLine + tables.map(function(table){
                        return '\t' + table + ' : ' + table;
                    }).join(',' + endOfLine) + endOfLine + '};' + endOfLine + endOfLine + sClasses;
            }
            return sClasses;
        });
    }

    function makeProperties(properties){
        return (JSON.
            stringify(properties).
            replace(/"/gi,'\'').
            replace(/},'/gi,'},' + endOfLine + '\t\t\'').
            replace('{','{' + endOfLine + '\t\t') + ';' + endOfLine).replace('}};','}' + endOfLine + '\t};');
    }

    function manyToOne(string) {
        var lastPos = string.length - 1;
        if(string.charAt(lastPos) !== 's'){
            return string;
        }
        if(string.charAt(lastPos - 1) === 'e' && string.charAt(lastPos - 2) === 'i'){
            return string.slice(0, lastPos-2) + 'y';
        }
        if(string.charAt(lastPos - 1) === 'e' && string.charAt(lastPos - 2) === 'o'){
            return string.slice(0, lastPos-1);
        }
        if(string.charAt(lastPos - 1) === 'e' && string.charAt(lastPos - 2) === 'v'){
            var part = string.slice(0, lastPos-2);
            return part.length < 3 ? part + 'fe' : part + 'f';
        }
        return string.slice(0, lastPos);
    }

    function capitalize(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
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
                    return;
                }
                var queryFunction = !queryOptions.eventMode ? function (err, results, fields) {
                    if (err) {
                        reject(err);
                        connection.release();
                        return;
                    }
                    var result = { results:results, fields:fields};
                    if (queryOptions.processResult) {
                        result = queryOptions.processResult(result, reject);
                    }
                    connection.release();
                    resolve(result);
                } : undefined;

                var query = queryOptions.params ?
                    connection.query(queryOptions.query, queryOptions.params, queryFunction) :
                    connection.query(queryOptions.query, queryFunction);

                console.log(query.sql);

                if(queryOptions.eventMode){
                    connection.pause();
                    resolve({
                        query : query,
                        connection : connection
                    });
                }
            });
        });
    }
};
