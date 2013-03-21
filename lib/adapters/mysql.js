var safeRequire = require('../utils').safeRequire;

/**
 * Module dependencies
 */
var mysql = safeRequire('mysql');
var BaseSQL = require('../sql');

var utils  = require('bc-utils').inflection;
var log = require('bc-log').log;
var $ = require('bc-log').$;

var pluralize = utils.pluralize;
var casingFunc = utils.underscore; // camelize;
var _ = require('underscore');

exports.initialize = function initializeSchema(schema, callback) {
    if (!mysql) return;

    var s = schema.settings;

    var config = {
        host: s.host || 'localhost',
        port: s.port || 3306,
        user: s.username,
        database: s.database,
        password: s.password,
        debug: s.debug
    };

    console.log(config);

    schema.client = mysql.createConnection(config);

    function handleDisconnect(connection) {
      connection.on('error', function(err) {

        if (!err.fatal) {
          log('%s %s', $('! non-fatal Error').red, $(err.code).blue);
          return;
        }

        log('\n\n\n%s %s\n\n\n', $('! Fatal Error').red, $(err.code).blue);

        if (err.code !== 'PROTOCOL_CONNECTION_LOST') {
          throw err;
        }

        log('%s:\n%s', $('Re-connecting lost connection').green, $(err.stack).red);
        
        process.exit(1);
    
        /*connection = mysql.createConnection(config);
        handleDisconnect(connection);
        connection.connect(function(error) {
            if (error) {
                log($('\n\nunsuccessful reconnection\n\n').red);
                process.exit(1);
            }
            log($('\n\nsuccessful reconnection\n\n').green);

        });
        */
      });
    }
    handleDisconnect(schema.client);


    schema.adapter = new MySQL(schema.client);
    schema.adapter.schema = schema;

    schema.client.query('SELECT 1', function(err) {
        if (err) {
            log('%s:\n%s', $('Error on initialization').blue, $(err.stack).red);
            process.exit(1);
        } else {
            callback(null);
        }
    });
};

/**
 * MySQL adapter
 */
function MySQL(client) {
    this._models = {};
    this.client = client;
}

require('util').inherits(MySQL, BaseSQL);

MySQL.prototype.query = function (sql, callback) {
    var client = this.client;
    var time = Date.now();

    if (!this.schema.connected) {
        return this.schema.on('connected', function () {
            this.query(sql, callback);
        }.bind(this));
    }
    if (typeof callback !== 'function') throw new Error('callback should be a function');
    console.log(sql);
    this.client.query(sql, callback);
};

/**
 * Must invoke callback(err, id)
 */
MySQL.prototype.create = function (model, data, callback) {
    var fields = this.toFields(model, data);
    var sql = 'INSERT INTO ' + this.tableEscaped(model);
    if (fields) {
        sql += ' SET ' + fields;
    } else {
        sql += ' VALUES ()';
    }
    this.query(sql, function (err, info) {
        callback(err, info && info.insertId);
    });
};

MySQL.prototype.updateOrCreate = function (model, data, callback) {
    var mysql = this;
    var fieldsNames = [];
    var fieldValues = [];
    var combined = [];
    var props = this._models[model].properties;
    Object.keys(data).forEach(function (key) {
        if (props[key] || key === 'id') {
            var k = '`' + key + '`';
            var v;
            if (key !== 'id') {
                v = mysql.toDatabase(props[key], data[key]);
            } else {
                v = data[key];
            }
            fieldsNames.push(k);
            fieldValues.push(v);
            if (key !== 'id') combined.push(k + ' = ' + v);
        }
    });

    var sql = 'INSERT INTO ' + this.tableEscaped(model);
    sql += ' (' + fieldsNames.join(', ') + ')';
    sql += ' VALUES (' + fieldValues.join(', ') + ')';
    sql += ' ON DUPLICATE KEY UPDATE ' + combined.join(', ');

    this.query(sql, function (err, info) {
        if (!err && info && info.insertId) {
            data.id = info.insertId;
        }
        callback(err, data);
    });
};

MySQL.prototype.toFields = function (model, data) {
    var fields = [];
    var props = this._models[model].properties;
    // var settings = this._models[model].settings;
    Object.keys(data).forEach(function (key) {
        if (props[key]) {
            if (key.match(/^id$/)) {
                // nop
                // you actually can't set HEX('id') = something, it doesnt make sense
                // this is used for writes!
                // leave it blank to you can remove id=311 from UPDATES
                // fields.push('HEX(`' + key.replace(/\./g, '`.`') + '`) = ' + this.toDatabase(props[key], data[key]));
            } else {
                fields.push('`' + key.replace(/\./g, '`.`') + '` = ' + this.toDatabase(props[key], data[key]));
            }
        
        }
    }.bind(this));
    return fields.join(',');
};

function dateToMysql(val) {
    return val.getUTCFullYear() + '-' +
        fillZeros(val.getUTCMonth() + 1) + '-' +
        fillZeros(val.getUTCDate()) + ' ' +
        fillZeros(val.getUTCHours()) + ':' +
        fillZeros(val.getUTCMinutes()) + ':' +
        fillZeros(val.getUTCSeconds());

    function fillZeros(v) {
        return v < 10 ? '0' + v : v;
    }
}

MySQL.prototype.toDatabase = function (prop, val) {
    if (val === null || val === undefined || prop === undefined) {
        if (!prop && val) {
            console.log('you probably are trying to look for prop that doesnt exist!')
        }
        return 'NULL';
    }
    if (val.constructor.name === 'Object') {
        var operator = Object.keys(val)[0]
        val = val[operator];
        if (operator === 'between') {
            return  this.toDatabase(prop, val[0]) +
                    ' AND ' +
                    this.toDatabase(prop, val[1]);
        } else if (operator == 'inq' || operator == 'nin') {
            if (!(val.propertyIsEnumerable('length')) && typeof val === 'object' && typeof val.length === 'number') { //if value is array
                return val.join(',');
            } else {
                return val;
            }
        }
    }
    if (!prop) return val;
    if (prop.type.name === 'Number') return val;
    if (prop.type.name === 'Date') {
        if (!val) return 'NULL';
        if (!val.toUTCString) {
            val = new Date(val);
        }
        return '"' + dateToMysql(val) + '"';
    }
    if (prop.type.name == "Boolean") return val ? 1 : 0;
    return this.client.escape(val.toString());
};

MySQL.prototype.fromDatabase = function (model, data) {
    if (!data) return null;
    var props = this._models[model].properties;
    Object.keys(data).forEach(function (key) {
        var val = data[key];
        if (props[key]) {
            if (props[key].type.name === 'Date' && val !== null) {
                val = new Date(val.toString().replace(/GMT.*$/, 'GMT'));
            }
        }
        data[key] = val;
    });
    return data;
};

MySQL.prototype.escapeName = function (name) {
    return '`' + name.replace(/\./g, '`.`') + '`';
};

MySQL.prototype.all = function all(model, filter, callback) {

    var sql = 'SELECT ';
    sql += this.getModelEscapedSelectFields(model);
    sql += ' FROM ' + this.tableEscaped(model);

    var self = this;
    var props = this._models[model].properties;

    if (filter) {

        if (filter.join) {


            if (typeof filter.join === 'object') {

                /*


                EXAMPLE QUERY TO EMULATE:

                SELECT
                Image.*, File.url, ImageAssoc.weight
                FROM Image
                INNER JOIN File ON File.id = Image.file_id
                INNER JOIN ImageAssoc ON ImageAssoc.image_id = Image.id
                WHERE ImageAssoc.image_gallery_id = 1;

                */

                var thisTable = this.table(model);



                // SELECT
                sql = 'SELECT ';


                // Image.*, File.url, ImageAssoc.weight
                var TableData = [thisTable +   '.*'];
                _.each(filter.join, function(info, Model) {
                    _.each(info.include, function(attr) {
                        TableData.push(Model + '.' + attr);
                    });
                });
                sql += TableData.join(', ');

                // FROM Image
                sql += ' FROM ' + thisTable;

                // INNER JOIN File ON File.id = Image.file_id
                // INNER JOIN ImageAssoc ON ImageAssoc.image_id = Image.id
                var joins = [];
                _.each(filter.join, function(info, Model) {

                    var key = null;
                    if (info.foreignKey) {
                        key = info.foreignKey;
                    }

                    if (info.assoc) {
                        joins.push(buildJoin(thisTable, Model, key));
                    } else {
                        joins.push(buildInverseJoin(thisTable, Model, key));
                    }
                });
                sql += ' ' + joins.join(' ');

                // WHERE ImageAssoc.image_gallery_id = 1;
                var wheres = [];
                _.each(filter.join, function(info, Model) {
                    if (info.where) {
                        wheres.push(buildWhere(info.where, Model));
                    }
                });

                if (filter.where) {
                    wheres.push(buildWhere(filter.where, thisTable));
                }

                var w = _.flatten(wheres);
                if (w.length) sql +=  ' WHERE ' + w.join(' AND ');


            } else {

                if (filter.include) {

                    var T2 = '`' + filter.join.modelName + '`';
                    var fields = [];
                    filter.include.forEach(function(field) {
                        fields.push( T2 + '.' + field);
                    });

                    sql = 'SELECT ' + this.tableEscaped(model) +   '.*, ' + fields.join(',') + ' FROM ' + this.tableEscaped(model);

                } else {

                    sql = 'SELECT ' + this.tableEscaped(model) +   '.* FROM ' + this.tableEscaped(model);

                }

                sql += ' ' + buildJoin(this.table(model), filter.join.modelName);
                if (filter.where) {
                    var k =  Object.keys(filter.where)[0],
                    f = filter.join.modelName + '.' + k + ' = ' + filter.where[k];
                    sql += ' WHERE ' + f;
                }

            }


        } else if (filter.where) {
            var w = buildWhere(filter.where);
            if (w.length) sql +=  ' WHERE ' + w.join(' AND ');
        }

        if (filter.order) {
            sql += ' ' + buildOrderBy(filter.order);
        }

        if (filter.limit) {
            sql += ' ' + buildLimit(filter.limit, filter.offset || 0);
        }

    }

    this.query(sql, function (err, data) {
        if (err) {
            return callback(err, []);
        }
        callback(null, data.map(function (obj) {
            return self.fromDatabase(model, obj);
        }));
    }.bind(this));

    return sql;

    function buildWhere(conds, Table) {
        var cs = [];
        Object.keys(conds).forEach(function (key) {
            var keyEscaped = Table ? Table + '.' : '';
            keyEscaped +=  '`' + key.replace(/\./g, '`.`') + '`';

            var val = null;
            if (Table) {
                var ps = self._models[Table].properties;
                val = self.toDatabase(ps[key], conds[key]);
            } else {
                val = self.toDatabase(props[key], conds[key]);
            }


            if (conds[key] === null || conds[key] === undefined) {
                cs.push(keyEscaped + ' IS NULL');
            } else if (conds[key].constructor.name === 'Object') {
                var condType = Object.keys(conds[key])[0];
                var sqlCond = keyEscaped;
                switch (condType) {
                    case 'gt':
                        sqlCond += ' > ';
                        break;
                    case 'gte':
                        sqlCond += ' >= ';
                        break;
                    case 'lt':
                        sqlCond += ' < ';
                        break;
                    case 'lte':
                        sqlCond += ' <= ';
                        break;
                    case 'between':
                        sqlCond += ' BETWEEN ';
                        break;
                    case 'inq':
                        sqlCond += ' IN ';
                        break;
                    case 'nin':
                        sqlCond += ' NOT IN ';
                        break;
                    case 'neq':
                        sqlCond += ' != ';
                        break;
                }
                sqlCond += (condType == 'inq' || condType == 'nin') ? '(' + val + ')' : val;
                cs.push(sqlCond);
            } else {
                cs.push(keyEscaped + ' = ' + val);
            }
        });
        return cs;
        // if (cs.length === 0) {
        //   return '';
        // }
        // return 'WHERE ' + cs.join(' AND ');
    }

    function buildJoin(thisClass, relationClass, key) {
        var rel = relationClass,
        qry = thisClass,
        f1 = rel + '.' +  (key || casingFunc( qry + '_id')),
        f2 = qry + '.id';
        return 'INNER JOIN ' + rel + ' ON ' + f1 + ' = ' + f2;
    }

    function buildInverseJoin(thisClass, relationClass, key) {
        var rel = relationClass,
        qry = thisClass,
        f1 = qry + '.' +  (key || casingFunc( rel + '_id')),
        f2 = rel + '.id';
        return 'INNER JOIN ' + rel + ' ON ' + f2 + ' = ' + f1;
    }

    function buildOrderBy(order) {

        if (_.isArray(order)) {

            order = _.map(order, function(o) {
                return [o, 'ASC'];
            });

        } else if (_.isObject(order)) {

            order = _.map(order, function(dir, field) {
                return [field, dir];
            });

        } else if (typeof order === 'string') {

            order = [[order, 'ASC']];

        }

        order = _.map(order, function(orderPair) {
            return orderPair[0] + ' ' + orderPair[1];
        });

        return 'ORDER BY ' + order.join(', ');
    }

    function buildLimit(limit, offset) {
        return 'LIMIT ' + (offset ? (offset + ', ' + limit) : limit);
    }

};

MySQL.prototype.autoupdate = function (cb) {
    var self = this;
    var wait = 0;
    Object.keys(this._models).forEach(function (model) {
        wait += 1;
        self.query('SHOW FIELDS FROM ' + self.tableEscaped(model), function (err, fields) {
            self.query('SHOW INDEXES FROM ' + self.tableEscaped(model), function (err, indexes) {
                if (!err && fields.length) {
                    self.alterTable(model, fields, indexes, done);
                } else {
                    self.createTable(model, done);
                }
            });
        });
    });

    function done(err) {
        if (err) {
            console.log(err);
        }
        if (--wait === 0 && cb) {
            cb();
        }
    }
};

MySQL.prototype.isActual = function (cb) {
    var ok = false;
    var self = this;
    var wait = 0;
    Object.keys(this._models).forEach(function (model) {
        wait += 1;
        self.query('SHOW FIELDS FROM ' + model, function (err, fields) {
            self.query('SHOW INDEXES FROM ' + model, function (err, indexes) {
                self.alterTable(model, fields, indexes, done, true);
            });
        });
    });

    function done(err, needAlter) {
        if (err) {
            console.log(err);
        }
        ok = ok || needAlter;
        if (--wait === 0 && cb) {
            cb(null, !ok);
        }
    }
};

MySQL.prototype.alterTable = function (model, actualFields, actualIndexes, done, checkOnly) {
    var self = this;
    var m = this._models[model];
    var propNames = Object.keys(m.properties).filter(function (name) {
        return !!m.properties[name];
    });
    var indexNames = m.settings.indexes ? Object.keys(m.settings.indexes).filter(function (name) {
        return !!m.settings.indexes[name];
    }) : [];
    var sql = [];
    var ai = {};

    if (actualIndexes) {
        actualIndexes.forEach(function (i) {
            var name = i.Key_name;
            if (!ai[name]) {
                ai[name] = {
                    info: i,
                    columns: []
                };
            }
            ai[name].columns[i.Seq_in_index - 1] = i.Column_name;
        });
    }
    var aiNames = Object.keys(ai);

    // change/add new fields
    propNames.forEach(function (propName) {
        if (propName === 'id') return;
        var found;
        actualFields.forEach(function (f) {
            if (f.Field === propName) {
                found = f;
            }
        });

        if (found) {
            actualize(propName, found);
        } else {
            sql.push('ADD COLUMN `' + propName + '` ' + self.propertySettingsSQL(model, propName));
        }
    });

    // drop columns
    actualFields.forEach(function (f) {
        var notFound = !~propNames.indexOf(f.Field);
        if (f.Field === 'id') return;
        if (notFound || !m.properties[f.Field]) {
            sql.push('DROP COLUMN `' + f.Field + '`');
        }
    });

    // remove indexes
    aiNames.forEach(function (indexName) {
        if (indexName === 'id' || indexName === 'PRIMARY') return;
        if (indexNames.indexOf(indexName) === -1 && !m.properties[indexName] || m.properties[indexName] && !m.properties[indexName].index) {
            sql.push('DROP INDEX `' + indexName + '`');
        } else {
            // first: check single (only type and kind)
            if (m.properties[indexName] && !m.properties[indexName].index) {
                // TODO
                return;
            }
            // second: check multiple indexes
            var orderMatched = true;
            if (indexNames.indexOf(indexName) !== -1) {
                m.settings.indexes[indexName].columns.split(/,\s*/).forEach(function (columnName, i) {
                    if (ai[indexName].columns[i] !== columnName) orderMatched = false;
                });
            }
            if (!orderMatched) {
                sql.push('DROP INDEX `' + indexName + '`');
                delete ai[indexName];
            }
        }
    });

    // add single-column indexes
    propNames.forEach(function (propName) {
        var i = m.properties[propName].index;
        if (!i) {
            return;
        }
        var found = ai[propName] && ai[propName].info;
        if (!found) {
            var type = '';
            var kind = '';
            if (i.type) {
                type = 'USING ' + i.type;
            }
            if (i.kind) {
                // kind = i.kind;
            }
            if (kind && type) {
                sql.push('ADD ' + kind + ' INDEX `' + propName + '` (`' + propName + '`) ' + type);
            } else {
                sql.push('ADD ' + kind + ' INDEX `' + propName + '` ' + type + ' (`' + propName + '`) ');
            }
        }
    });

    // add multi-column indexes
    indexNames.forEach(function (indexName) {
        var i = m.settings.indexes[indexName];
        var found = ai[indexName] && ai[indexName].info;
        if (!found) {
            var type = '';
            var kind = '';
            if (i.type) {
                type = 'USING ' + i.kind;
            }
            if (i.kind) {
                kind = i.kind;
            }
            if (kind && type) {
                sql.push('ADD ' + kind + ' INDEX `' + indexName + '` (' + i.columns + ') ' + type);
            } else {
                sql.push('ADD ' + kind + ' INDEX ' + type + ' `' + indexName + '` (' + i.columns + ')');
            }
        }
    });

    if (sql.length) {
        var query = 'ALTER TABLE `' + model + '` ' + sql.join(',\n');
        if (checkOnly) {
            done(null, true, {statements: sql, query: query});
        } else {
            this.query(query, done);
        }
    } else {
        done();
    }

    function actualize(propName, oldSettings) {
        var newSettings = m.properties[propName];
        if (newSettings && changed(newSettings, oldSettings)) {
            sql.push('CHANGE COLUMN `' + propName + '` `' + propName + '` ' + self.propertySettingsSQL(model, propName));
        }
    }

    function changed(newSettings, oldSettings) {
        if (oldSettings.Null === 'YES' && (newSettings.allowNull === false || newSettings.null === false)) return true;
        if (oldSettings.Null === 'NO' && !(newSettings.allowNull === false || newSettings.null === false)) return true;
        if (oldSettings.Type.toUpperCase() !== datatype(newSettings)) return true;
        return false;
    }
};

MySQL.prototype.propertiesSQL = function (model) {
    var self = this;
    var sql = null;
    if (this._models[model].settings.uuid) {
        sql = ['`id` BINARY(16) NOT NULL UNIQUE PRIMARY KEY'];
    } else {
        sql = ['`id` INT(11) NOT NULL AUTO_INCREMENT UNIQUE PRIMARY KEY'];
    }
    Object.keys(this._models[model].properties).forEach(function (prop) {
        if (prop === 'id') return;
        sql.push('`' + prop + '` ' + self.propertySettingsSQL(model, prop));
    });
    return sql.join(',\n  ');

};

MySQL.prototype.propertySettingsSQL = function (model, prop) {
    var p = this._models[model].properties[prop];
    return datatype(p) + ' ' +
    (p.allowNull === false || p['null'] === false ? 'NOT NULL' : 'NULL');
};

function datatype(p) {
    var dt = '';
    switch (p.type.name) {
        default:
        case 'String':
        case 'JSON':
        dt = 'VARCHAR(' + (p.limit || 255) + ')';
        break;
        case 'Binary':
        dt = 'BINARY(16)';
        break;
        case 'Text':
        dt = 'TEXT';
        break;
        case 'Number':
        dt = 'INT(' + (p.limit || 11) + ')';
        break;
        case 'Date':
        dt = 'DATETIME';
        break;
        case 'Boolean':
        dt = 'TINYINT(1)';
        break;
    }
    return dt;
}

