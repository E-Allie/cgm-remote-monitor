'use strict';

const utils = require('./utils');

/**
 * Insert single document
 * @param {Object} col
 * @param {Object} doc
 * @param {Object} options
 */
function insertOne(col, doc, options) {

  return new Promise(function(resolve, reject) {

    col.insertOne(doc, function mongoDone(err, result) {

      if (err) {
        reject(err);
      } else {
        const identifier = doc.identifier || result.insertedId.toString();

        if (!options || options.normalize !== false) {
          delete doc._id;
        }
        resolve(identifier);
      }
    });
  });
}


/**
 * Replace single document
 * @param {Object} col
 * @param {string} identifier
 * @param {Object} doc
 */
function replaceOne(col, identifier, doc) {

  return new Promise(function(resolve, reject) {

    const filter = utils.filterForOne(identifier);

    col.replaceOne(filter, doc, {
      upsert: true
    }, function mongoDone(err, result) {
      if (err) {
        reject(err);
      } else {
        resolve(result.matchedCount);
      }
    });
  });
}


/**
 * Update single document by identifier
 * @param {Object} col
 * @param {string} identifier
 * @param {object} setFields
 */
function updateOne(col, identifier, setFields) {

  return new Promise(function(resolve, reject) {

    const filter = utils.filterForOne(identifier);

    col.updateOne(filter, {
      $set: setFields
    }, function mongoDone(err, result) {
      if (err) {
        reject(err);
      } else {
        resolve({
          updated: result.result.nModified
        });
      }
    });
  });
}


/**
 * Permanently remove single document by identifier
 * @param {Object} col
 * @param {string} identifier
 */
function deleteOne(col, identifier) {

  return new Promise(function(resolve, reject) {

    const filter = utils.filterForOne(identifier);

    col.deleteOne(filter, function mongoDone(err, result) {
      if (err) {
        reject(err);
      } else {
        resolve({
          deleted: result.result.n
        });
      }
    });
  });
}


/**
 * Permanently remove many documents matching any of filtering criteria
 */
function deleteManyOr(col, filterDef) {

  return new Promise(function(resolve, reject) {

    const filter = utils.parseFilter(filterDef, 'or');

    col.deleteMany(filter, function mongoDone(err, result) {
      if (err) {
        reject(err);
      } else {
        resolve({
          deleted: result.deletedCount
        });
      }
    });
  });
}

/**
 * Execute multiple write operations
 * @param {Object} col
 * @param {Array<Object>} operations - Array of write operations (insertOne, updateOne, replaceOne, deleteOne, etc.)
 * @param {Object} options - Options for bulkWrite (e.g., { ordered: false })
 */
function bulkWrite(col, operations, options) {

  // Ensure operations is an array and not empty
  if (!Array.isArray(operations) || operations.length === 0) {
    // Or return a specific result indicating no operations were performed
    return {
      ok: 1
      , nInserted: 0
      , nMatched: 0
      , nModified: 0
      , nUpserted: 0
      , nRemoved: 0
      , writeErrors: []
    };
  }
  return col.bulkWrite(operations, options);
}


module.exports = {
  insertOne
  , replaceOne
  , updateOne
  , deleteOne
  , deleteManyOr
  , bulkWrite
, };