# Total.js QueryBuilder: mssql

A simple QueryBuilder integrator for mssql database.

- [Documentation](https://docs.totaljs.com/total4/pzbr001pr41d/)
- `$ npm install querybuildersqlserver`

## Initialization

- Example: `microsoftsqlserver://user:password@localhost:5432/database`

```js
// require('querybuilderpg').init(name, connectionstring, pooling, [errorhandling]);
// name {String} a name of DB (default: "default")
// connectionstring {String} a connection to the mssql
// pooling {Number} max. clients (default: "0" (disabled))
// errorhandling {Function(err, cmd)}

require('querybuilderpg').init('default', CONF.database);
// require('querybuilderpg').init('default', CONF.database, 10);
```

**Usage**:

```js
DATA.find('tbl_user').where('id', 1234).callback(console.log);
```

## Autoquery for JSON columns

`QueryBuilder.autoqueryjson($, column, schema, [defsort], [maxlimit])` works like `autoquery()` but the properties are read from a JSON column. It supports the filtering, sorting, fields and pagination of `autoquery()`.

```js
// $ {Controller|Object} uses $.query (or the object itself)
// column {String} a name of the JSON column
// schema {String} properties and types, e.g. 'id,name,age:Number'
// defsort {String} default sorting, e.g. 'age_desc' (optional)
// maxlimit {Number} max. items per page, it enables the pagination (optional)

DATA.list('tbl_user').autoqueryjson($, 'data', 'id,name,email,age:Number,ok:Boolean,dtcreated:Date,tags:[string]', 'dtcreated_desc', 100).callback($.callback);
```

- Query string: `?age=>18&name=john&tags=a,b&sort=age_desc&fields=name,age&page=2&limit=20`
- Filters use the same rules as `autoquery()` (`a - b` range, `a,b` list, `>` and `<`, `=` exact, `today`...)
- Only properties declared in the `schema` can be used in fields, filters and sorting
- A property without a type is `String`
- The names of the column and properties accept only `a-z`, `0-9` and `_`
- Requires SQL Server 2016+ (`JSON_VALUE`, `OPENJSON`, `TRY_CAST`) with the database compatibility level 130+
- `Date` is returned as `datetimeoffset` and `[string]` is parsed into an array
- `JSON_VALUE` returns scalars only: an object or an array stored in a `String`, `Number`, `Boolean` or `Date` property is `null`

__Types and values__:

| Type | Missing property | Value in a wrong format |
|------|------------------|-------------------------|
| `String` | `null` | the value as text |
| `Number` | `null` | `0` |
| `Boolean` | `null` | `false` (`true`, `1`, `on`, `yes` are `true`) |
| `Date` | `null` | `null` |
| `[string]` | `null` | `[]` |

A value in a wrong format never fails the query (e.g. `"age": "abc"` is `0`), the same way as in `autoquery()`. This applies to filtering, sorting and to the returned values.

The QueryBuilder doesn't generate SQL for JSON columns. It uses a neutral name `json:column:property:type` and this adapter translates it into `JSON_VALUE(...)` expressions.

## Connection string attributes

- Connection string example: `microsoftsqlserver://user:password@localhost:5432/database`
