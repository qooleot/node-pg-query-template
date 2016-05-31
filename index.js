"use strict";

const Promise = require('bluebird');
const _ = require('lodash');

module.exports = function(pg, app) {

  function override(object, methodName, callback) {
    object[methodName] = callback(object[methodName])
  }

  pg.on('error', function(error) {
    console.log('pg error', error);
  })

  // attaching method to pg.Client so all the APIs that use app.pgClient.queryAsync will use connection pooling
  pg.Client.prototype.queryAsync = Promise.promisify(function(query, bindVars, queryCB) {

    var client = this;

    // if no bind vars
    if (queryCB == undefined) {
      queryCB = bindVars;
      bindVars = [];
    }

    if (this.inTransaction) {
     client.query(query, bindVars, queryCB);
    } else {  
      pg.connect(app.config.pg.business.pg_conn_string, function(connectErr, pgClient, connectFinishFn) {
        pgClient.query(query, bindVars, function(err, queryRes) {
          connectFinishFn();
          queryCB(err, queryRes);
        });
      });
    }   
  });

  // grab client from pool, then create begin transaction
  pg.Client.prototype.transactionStart = Promise.promisify(function(cb) {
    pg.connect(app.config.pg.business.pg_conn_string, function(connectErr, pgClient, connectFinishFn) {
      pgClient.returnClientToPool = connectFinishFn;
      pgClient.inTransaction = true;
      pgClient.query('BEGIN', function(err, beginResult) {
        cb(connectErr, pgClient);
      });
    });
  });
 
  // commit + return client to connection pool
  pg.Client.prototype.commit = Promise.promisify(function(cb) {
    var client = this;
    client.inTransaction = false;
    client.query('COMMIT', function(err, commitResult) {
      client.returnClientToPool();
      cb(err, commitResult); 
    });
  });

  // rollback + return client to connection pool
  pg.Client.prototype.rollback = Promise.promisify(function(cb) {
    var client = this;
    client.inTransaction = false;
    client.query('ROLLBACK', function(err, rollbackResult) {
      client.returnClientToPool();
      cb(err, rollbackResult); 
    });
  });

  pg.Client.prototype.queryTmpl = function(obj, substVals) {
  
    /* lodash template handling
     *   since template strings do not allow templates-within-templates
     *   and a shortcut way to just inject blocks of sql that is trusted (i.e. not input from ajax call)
     */
    if (substVals) {
      var numSubstTries = 0;
      while (numSubstTries < 5 && obj.query.match(/{{/)) {
        _.templateSettings.interpolate = /{{([\s\S]+?)}}/g;
        var compiled = _.template(obj.query);
        obj.query = compiled(substVals);
        numSubstTries++;
      }
    }

    var me = this;
    var queryWrapper = new Promise(function(resolve, reject) {
      pg.Client.prototype.queryAsync.call(me, obj.query, obj.values)
        .then(function(queryResult) {
          queryResult = _.pick(queryResult, ['rowCount', 'rows']);
          resolve(queryResult);
        })
        .catch(function(e) {
          console.log('query error in promise', e)
          reject(e);
        });
    });

    return queryWrapper;
  };

  exports.sqlTemplate = pg.Client.prototype.sqlTmpl = function(pieces) {
    var result = '';
    var vals = [];
    var substitutions = [].slice.call(arguments, 1);
    for (var i = 0; i < substitutions.length; ++i) {
      result += pieces[i] + '$' + (i + 1);
      vals.push(substitutions[i]);
    }

    result += pieces[substitutions.length];

    return {query: result, values: vals};
  };

  return exports;
}

