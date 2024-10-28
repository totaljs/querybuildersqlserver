const MSSQL = require('mssql');
const CANSTATS = global.F ? (global.F.stats && global.F.stats.performance && global.F.stats.performance.dbrm != null) : false;
const REG_MSSQL_ESCAPE = /'/g;
const REG_LANGUAGE = /[a-z0-9]+§/gi;
const REG_WRITE = /(INSERT|UPDATE|DELETE|DROP)\s/i;
const REG_COL_TEST = /"|\s|:|\./;
const REG_UPDATING_CHARS = /^[-+*/><!=#]/;
const LOGGER = ' -- MSSQL -->';

const POOLS = {};
var FieldsCache = {};

function exec(client, filter, callback, done, errorhandling) {
	var cmd;

	if (filter.exec === 'list') {
		try {
			cmd = makesql(filter);
		} catch (e) {
			done();
			callback(e);
			return;
		}

		if (filter.debug)
			console.log(LOGGER, cmd.query, cmd.params);

		client.query(cmd.query, function(err, response) {
			if (err) {
				done();
				errorhandling && errorhandling(err, cmd);
				callback(err);
			} else {
				cmd = makesql(filter, 'count');

				if (filter.debug)
					console.log(LOGGER, cmd.query, cmd.params);

				client.query(cmd.query, function(err, counter) {
					done();
					err && errorhandling && errorhandling(err, cmd);
					callback(err, err ? null : { items: response.recordset, count: +counter.recordset[0].count });
				});
			}
		});
		return;
	}

	try {
		cmd = makesql(filter);
	} catch (e) {
		done();
		callback(e);
		return;
	}

	if (filter.debug)
		console.log(LOGGER, cmd.query, cmd.params);

	var output;

	if (filter.exec === 'insert' || filter.exec === 'update') {
		let request = client.request();

		for (let i = 0; i < Object.keys(filter.payload).length; i++) {
			let param = Object.keys(filter.payload)[i];

			if (REG_UPDATING_CHARS.test(param))
				param = param.substring(1);
			
			request.input(param, cmd.params[i]);
		}

		request.query(cmd.query, function(err, response) {
			if (err) {
				done();
				errorhandling && errorhandling(err, cmd);
				callback(err);
				return;
			}

			done();

			if (filter.exec === 'insert') {
				if (filter.returning)
					output = response.recordset.length && response.recordset[0];
				else if (filter.primarykey)
					output = response.recordset.length && response.recordset[0][filter.primarykey];
				else
					output = response.rowsAffected[0];
			} else if (filter.exec === 'update') {
				if (filter.returning)
					output = filter.first ? (response.recordset.length && response.recordset[0]) : response.recordset;
				else
					output = (response.rowsAffected.length && response.rowsAffected[0]) || 0;
			}

			callback(null, output);
		});	
		return;
	}
	
	client.query(cmd.query, function(err, response) {
		done();

		if (err) {
			errorhandling && errorhandling(err, cmd);
			callback(err);
			return;
		}

		switch (filter.exec) {
			
			case 'remove':
				if (filter.returning)
					output = filter.first ? (response.recordset.length && response.recordset[0]) : response.recordset;
				else if (filter.primarykey)
					output = response.recordset.length && response.recordset[0][filter.primarykey];
				else
					output = response.rowsAffected[0];

				callback(null, output);
				break;
			case 'check':
				output = response.recordset[0] ? response.recordset[0].count > 0 : false;
				callback(null, output);
				break;
			case 'count':
				output = response.recordset[0] ? response.recordset[0].count : null;
				callback(null, output);
				break;
			case 'scalar':
				output = filter.scalar.type === 'group' ? response.recordset : (response.recordset[0] ? response.recordset[0].value : null);
				callback(null, output);
				break;
			default:
				output = response.recordset;
				callback(err, output);
				break;
		}
	});
};

function mssql_where(where, opt, filter, operator) {
	var tmp;

	for (var item of filter) {

		var name = '';

		if (item.name) {

			let key = 'where_' + (opt.language || '') + '_' + item.name;
			
			name = FieldsCache[key];

			if (!name) {
				name = item.name;
				if (name[name.length - 1] === '§')
					name = replacelanguage(item.name, opt.language, true);
				else
					name = REG_COL_TEST.test(item.name) ? item.name : ('"' + item.name + '"');
				FieldsCache[key] = name;
			}

		}

		switch (item.type) {
			case 'or':
				tmp = [];
				mssql_where(tmp, opt, item.value, 'OR');
				where.length && where.push(operator);
				where.push('(' + tmp.join(' ') + ')');
				break;
			case 'in':
			case 'notin':
				where.length && where.push(operator);
				tmp = [];
				if (item.value instanceof Array) {
					for (var val of item.value) {
						if (val != null)
							tmp.push(MSSQL_ESCAPE(val));
					}
				} else if (item.value != null)
					tmp = [MSSQL_ESCAPE(item.value)];
				if (!tmp.length)
					tmp.push('null');
				where.push(name + (item.type === 'in' ? ' IN ' : ' NOT IN ') + '(' + tmp.join(',') + ')');
				break;
			case 'query':
				where.length && where.push(operator);
				where.push('(' + item.value + ')');
				break;
			case 'where':
				where.length && where.push(operator);
				if (item.value == null)
					where.push(name + (item.comparer === '=' ? ' IS NULL' : ' IS NOT NULL'));
				else
					where.push(name + item.comparer + MSSQL_ESCAPE(item.value));
				break;
			case 'contains':
				where.length && where.push(operator);
				where.push('LEN(CAST(' + name +' AS VARCHAR))>0');
				break;
			case 'search':
				where.length && where.push(operator);
				tmp = item.value ? item.value.replace(/%/g, '') : '';

				if (item.operator === 'beg')
					where.push(name + ' LIKE ' + MSSQL_ESCAPE('%' + tmp));
				else if (item.operator === 'end')
					where.push(name + ' LIKE ' + MSSQL_ESCAPE(tmp + '%'));
				else
					where.push('CAST(' + name + ' AS VARCHAR) LIKE ' + MSSQL_ESCAPE('%' + tmp + '%'));
				break;
			case 'month':
			case 'year':
			case 'day':
				where.length && where.push(operator);
				where.push(item.type.toUpperCase() + '(' + name + ')' + item.comparer + MSSQL_ESCAPE(item.value));
				break;
			case 'hour':
			case 'minute':
				where.length && where.push(operator);
				where.push('DATEPART(' + item.type.toUpperCase() + ', ' + name + ')' + item.comparer + MSSQL_ESCAPE(item.value));
				break;
			case 'empty':
				where.length && where.push(operator);
				where.push('(' + name + ' IS NULL OR LEN(CAST(' + name +' AS VARCHAR))=0)');
				break;
			case 'between':
				where.length && where.push(operator);
				where.push('(' + name + ' BETWEEN ' + MSSQL_ESCAPE(item.a) + ' AND ' + MSSQL_ESCAPE(item.b) + ')');
				break;
		}
	}
};

function mssql_insertupdate(filter, insert) {

	var query = [];
	var fields = insert ? [] : null;
	var params = [];

	for (var key in filter.payload) {
		var val = filter.payload[key];

		if (val === undefined)
			continue;

		var c = key[0];	
		switch (c) {
			case '-':
			case '+':
			case '*':
			case '/':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('@' + key);
				} else {
					query.push('"' + key + '"=COALESCE("' + key + '",0)' + c + '@' + key);
				}
				break;
			case '>':
			case '<':
				key = key.substring(1);
				params.push(val ? val : 0);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('@' + key);
				} else
					query.push('"' + key + '"=' + (c === '>' ? 'GREATEST' : 'LEAST') + '("' + key + '",@' + key + ')');
				break;
			case '!':
				// toggle
				key = key.substring(1);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('0');
				} else
					query.push('"' + key + '" = CASE WHEN ' + key + ' = 1 THEN 0 ELSE 1 END');
				break;
			case '=':
			case '#':
				// raw
				key = key.substring(1);
				if (insert) {
					if (c === '=') {
						fields.push('"' + key + '"');
						query.push(val);
					}
				} else
					query.push('"' + key + '"=' + val);
				break;
			default:
				params.push(val);
				if (insert) {
					fields.push('"' + key + '"');
					query.push('@' + key);
				} else 
					query.push('"' + key + '"=@' + key);
				break;
		}
	}
	
	return { fields, query, params };
};

function replacelanguage(fields, language, noas) {
	return fields.replace(REG_LANGUAGE, function(val) {
		val = val.substring(0, val.length - 1);
		return '"' + val + '' + (noas ? ((language || '') + '"') : language ? (language + '" AS "' + val + '"') : '"');
	});
};

function makesql(opt, exec) {

	var query = '';
	var where = [];
	var model = {};
	var isread = false;
	var params;
	var returning;
	var tmp;

	if (!exec)
		exec = opt.exec;

	mssql_where(where, opt, opt.filter, 'AND');

	var language = opt.language || '';
	var fields;
	var sort;
	
	if (opt.fields) {
		let key = 'fields_' + language + '_' + opt.fields.join(',');
		fields = FieldsCache[key] || '';
		if (!fields) {
			for (let i = 0; i < opt.fields.length; i++) {
				let m = opt.fields[i];
				if (m[m.length - 1] === '§')
					fields += (fields ? ',' : '') + replacelanguage(m, opt.language);
				else
					fields += (fields ? ',' : '') + (REG_COL_TEST.test(m) ? m : ('"' + m + '"'));
			}
			FieldsCache[key] = fields;
		}
	}

	switch (exec) {
		case 'find':
		case 'read':
			query = 'SELECT ' + (fields || '*') + ' FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'list':
			query = 'SELECT ' + (fields || '*') + ' FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'count':
			opt.first = true;
			query = 'SELECT CAST(COUNT(1) AS INT) as count FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'insert':
			returning = opt.returning ? opt.returning.join(',inserted.') : opt.primarykey ? opt.primarykey : '';
			tmp = mssql_insertupdate(opt, true);

			query = 'INSERT INTO ' + opt.table2 + ' (' + tmp.fields.join(',') + ')' + (returning ? ' OUTPUT inserted.' + returning : '') + ' VALUES(' + tmp.query.join(',') + ')';
			params = tmp.params;
			break;
		case 'remove':
			returning = opt.returning ? opt.returning.join(',deleted.') : opt.primarykey ? opt.primarykey : '';

			query = 'DELETE FROM ' + opt.table2 + (returning ? ' OUTPUT deleted.' + returning : '') + (where.length ? (' WHERE ' + where.join(' ')) : '');
			break;
		case 'update':
			returning = opt.returning ? opt.returning.join(',inserted.') : '';

			tmp = mssql_insertupdate(opt);
			query = 'UPDATE ' + opt.table2 + ' SET ' + tmp.query.join(',') + (returning ? ' OUTPUT inserted.' + returning : '') + (where.length ? (' WHERE ' + where.join(' ')) : '');

			params = tmp.params;
			break;
		case 'check':
			query = 'SELECT 1 as count FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
			isread = true;
			break;
		case 'drop':
			query = 'DROP TABLE ' + opt.table2;
			break;
		case 'truncate':
			query = 'TRUNCATE TABLE ' + opt.table2;
			break;
		case 'command':
			break;
		case 'scalar':
			switch (opt.scalar.type) {
				case 'avg':
				case 'min':
				case 'sum':
				case 'max':
				case 'count':
					opt.first = true;
					var val = opt.scalar.key === '*' ? 1 : opt.scalar.key;
					query = 'SELECT CAST(' + opt.scalar.type.toUpperCase() + (opt.scalar.type !== 'count' ? ('(' + val + ')') : '(1)') + ' AS NUMERIC) as value FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '');
					break;
				case 'group':
					query = 'SELECT ' + opt.scalar.key + ', ' + (opt.scalar.key2 ? ('CAST(SUM(' + opt.scalar.key2 + ') AS NUMERIC)') : 'CAST(COUNT(1) AS INT)') + ' as value FROM ' + opt.table2 + (where.length ? (' WHERE ' + where.join(' ')) : '') + ' GROUP BY ' + opt.scalar.key;
					break;
			}
			isread = true;
			break;
		case 'query':
			if (where.length) {
				let wherem = opt.query.match(/\{where\}/ig);
				let wherec = 'WHERE ' + where.join(' ');
				
				query = wherem ? opt.query.replace(wherem, wherec) : (opt.query + ' ' + wherec);
			} else {
				query = opt.query;
			}

			params = opt.params;
			isread = REG_WRITE.test(query) ? false : true;
			break;
	}

	if (exec === 'find' || exec === 'read' || exec === 'list' || exec === 'query' || exec === 'check') {

		if (opt.sort) {
			let key = 'sort_' + language + '_' + opt.sort.join(',');

			sort = FieldsCache[key] || '';
			if (!sort) {
				for (let i = 0; i < opt.sort.length; i++) {
					let m = opt.sort[i];
					let index = m.lastIndexOf('_');
					let name = m.substring(0, index);
					
					let value = (REG_COL_TEST.test(name) ? name : ('"' + name + '"')).replace(/§/, language);
					sort += (sort ? ',' : '') + value + ' ' + (m.substring(index + 1).toLowerCase() === 'desc' ? 'DESC' : 'ASC');
				}
				FieldsCache[key] = sort;
			}
			query += ' ORDER BY ' + sort;
		}

		if (opt.take && opt.skip)
			query += ' OFFSET ' + opt.skip + ' ROWS FETCH NEXT ' + opt.take + ' ROWS ONLY';
		else if (opt.take){
			var index = query.search(/select/i);
			var start = query.substring(0, index + 'select'.length);
			var end = query.substring(index + 'select'.length);

			query = start + ' TOP ' + opt.take + end;
		}
		else if (opt.skip)
			query += ' OFFSET ' + opt.skip + ' ROWS';
	}

	model.query = query;
	model.params = params;

	if (CANSTATS) {
		if (isread)
			F.stats.performance.dbrm++;
		else
			F.stats.performance.dbwm++;
	}
	
	return model;
}

function MSSQL_ESCAPE(value) {

	if (value == null)
		return 'null';

	if (value instanceof Array) {
		var builder = [];

		if (value.length) {
			for (var m of value)
				builder.push(MSSQL_ESCAPE(m));
			return '\'' + builder.join(',') + '\'';
		} else
			return 'null';
	}

	var type = typeof(value);

	if (type === 'function') {
		value = value();
		if (value == null)
			return 'null';
		type = typeof(value);
	}

	if (type === 'boolean')
		return value === true ? '1' : '0';

	if (type === 'number')
		return value + '';

	if (type === 'string')
		return mssql_escape(value);

	if (value instanceof Date)
		return mssql_escape(dateToString(value));

	if (type === 'object')
		return mssql_escape(JSON.stringify(value));

	return mssql_escape(value.toString());
};

function mssql_escape(val) {

	if (val == null)
		return 'NULL';

	val = val.replace(REG_MSSQL_ESCAPE, "''");
	return '\'' + val + '\'';
};

function dateToString(dt) {

	var arr = [];

	arr.push(dt.getFullYear().toString());
	arr.push((dt.getMonth() + 1).toString());
	arr.push(dt.getDate().toString());
	arr.push(dt.getHours().toString());
	arr.push(dt.getMinutes().toString());
	arr.push(dt.getSeconds().toString());

	for (var i = 1; i < arr.length; i++) {
		if (arr[i].length === 1)
			arr[i] = '0' + arr[i];
	}

	return arr[0] + '-' + arr[1] + '-' + arr[2] + ' ' + arr[3] + ':' + arr[4] + ':' + arr[5];
};

global.MSSQL_ESCAPE = MSSQL_ESCAPE;

exports.init = function(name, connstring, pooling, errorhandling) {
	if (!name)
		name = 'default';

	if (POOLS[name]) {
		POOLS[name].close();
		delete POOLS[name];
	}

	if (!connstring) {
		// Removes instance
		NEWDB(name, null);
		return;
	}
	var config = {};

	switch (typeof(connstring)) {
		case 'string':
			var url = F.Url.parse(connstring);
			var auth = url.auth.split(':');

			config.user = auth[0];
			config.password = auth[1];
			config.server = url.hostname;
			config.database = url.pathname.split('/').trim()[0];

			var queries = {};
			if (url.query) {
				var allqueries = url.query.split('&');

				for (let q of allqueries) {
					let item = q.split('=');
					queries[decodeURIComponent(item[0])] = decodeURIComponent(item[1]);
				}
			}

			config.options = {};
			config.options.encrypt = queries.encrypt ? queries.encrypt === 'false' ? false : true : false;
			config.options.trustServerCertificate = queries.trustServerCertificate ? queries.trustServerCertificate === 'false' ? false : true : false;
			break;
		case 'object':
			config = connstring;
			break;
		default:
			NEWDB(name, null);
			return;
	}
	
	var onerror = null;

	if (errorhandling)
		onerror = (err, cmd) => errorhandling(err + ' - ' + cmd.query.substring(0, 100));

	if (config.pool && config.pool.max) 
		pooling = config.pool.max;

	if (pooling) {
		if (!config.pool) 
			config.pool = {};

		config.pool.min = 0;
		config.pool.max = +pooling;
		config.idleTimeoutMillis = 30000;
	}

	NEWDB(name, function(filter, callback) {
		if (filter.schema == null && config.schema) 
			filter.schema = config.schema;

		filter.table2 = filter.schema ? (filter.schema + '.' + filter.table) : filter.table;

		if (pooling) {
			var pool = POOLS[name] || (POOLS[name] = new MSSQL.ConnectionPool(config));
			pool.connect(function(err) {
				if (err)
					callback(err);
				else {
					exec(pool, filter, callback, () => pool.close(), onerror);
				}
			})
		} else {
			MSSQL.connect(config).then(function(client) {
				exec(client, filter, callback, () => client.close(), onerror);
			}).catch(function(err) {
				callback(err);
			})
		}
	});
};

ON('service', function(counter) {
	if (counter % 10 === 0)
		FieldsCache = {};
});
