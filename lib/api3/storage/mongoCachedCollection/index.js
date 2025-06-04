'use strict';

const _ = require('lodash')

/**
 * Storage implementation which wraps mongo baseStorage with caching
 * @param {Object} ctx
 * @param {Object} env
 * @param {string} colName - name of the collection in mongo database
 * @param {Object} baseStorage - wrapped mongo storage implementation
 */
function MongoCachedCollection(ctx, env, colName, baseStorage) {

  const self = this;

  self.colName = colName;

  self.identifyingFilter = baseStorage.identifyingFilter;

  self.findOne = (...args) => baseStorage.findOne(...args);

  self.findOneFilter = (...args) => baseStorage.findOneFilter(...args);

  self.findMany = (...args) => baseStorage.findMany(...args);


  self.insertOne = async (doc) => {
    const result = await baseStorage.insertOne(doc, {
      normalize: false
    });

    if (cacheSupported()) {
      updateInCache([doc]);
    }

    if (doc._id) {
      delete doc._id;
    }
    return result;
  }


  self.replaceOne = async (identifier, doc) => {
    const result = await baseStorage.replaceOne(identifier, doc);

    if (cacheSupported()) {
      const rawDocs = await baseStorage.findOne(identifier, null, {
        normalize: false
      })
      updateInCache([rawDocs[0]])
    }

    return result;
  }


  self.updateOne = async (identifier, setFields) => {
    const result = await baseStorage.updateOne(identifier, setFields);

    if (cacheSupported()) {
      const rawDocs = await baseStorage.findOne(identifier, null, {
        normalize: false
      })

      if (rawDocs[0].isValid === false) {
        deleteInCache(rawDocs)
      } else {
        updateInCache([rawDocs[0]])
      }
    }

    return result;
  }

  self.deleteOne = async (identifier) => {
    let invalidateDocs
    if (cacheSupported()) {
      invalidateDocs = await baseStorage.findOne(identifier, {
        _id: 1
      }, {
        normalize: false
      })
    }

    const result = await baseStorage.deleteOne(identifier);

    if (cacheSupported()) {
      deleteInCache(invalidateDocs)
    }

    return result;
  }

  self.deleteManyOr = async (filter) => {
    let invalidateDocs
    if (cacheSupported()) {
      invalidateDocs = await baseStorage.findMany({
        filter
        , limit: 1000
        , skip: 0
        , projection: {
          _id: 1
        }
        , options: {
          normalize: false
        }
      });
    }

    const result = await baseStorage.deleteManyOr(filter);

    if (cacheSupported()) {
      deleteInCache(invalidateDocs)
    }

    return result;
  }

  /**
   * Executes a bulkWrite operation on the base storage and handles cache invalidation.
   * THIS IS CURRENTLY A PARTIAL IMPLEMENTATION.
   * It is implemented only so far as bulkwrite operations are currently utilized.
   * @param {Array<Object>} operations - Array of bulk write operations.
   * @param {Object} options - Options for the bulkWrite operation.
   * @returns {Promise<Object>} - The result from baseStorage.bulkWrite.
   */
  self.bulkWrite = async (operations, options) => {

    const bulkResult = await baseStorage.bulkWrite(operations, options);

    if (cacheSupported()) {
      const docsToUpdateInCache = [];
      const idsToDeleteFromCache = [];

      // Iterate through the original operations to determine cache impact
      for (let i = 0; i < operations.length; i++) {
        const op = operations[i];
        const isWriteError = bulkResult.hasWriteErrors() && bulkResult.getWriteErrors().some(err => err.index === i);

        if (isWriteError) {
          // If there was a write error for this operation, skip cache modification for it.
          continue;
        }

        // We generally need the documents as they are in the DB

        if (op.insertOne && op.insertOne.document) {

          const insertedInfo = bulkResult.getInsertedIds().find(item => item.index === i);
          if (insertedInfo) {
            const docToCache = {
              ...op.insertOne.document
              , _id: insertedInfo._id
            };
            docsToUpdateInCache.push(docToCache);
          }
        } else if (op.replaceOne && op.replaceOne.replacement) {
          // If a replace operation was successful
          // The replacement document is what should be in the cache, we need its _id. 
          // For now, let's assume `op.replaceOne.replacement` is the state to cache.
          // If `_id` is missing, we might need to fetch.
          let idForCache = op.replaceOne.replacement._id;
          if (!idForCache && op.replaceOne.filter && op.replaceOne.filter._id) {
            idForCache = op.replaceOne.filter._id;
          }
          // If still no _id, this strategy might be insufficient without a fetch.
          // TODO: Consider a debug log here to see if idForCache is EVER undefined here?
          docsToUpdateInCache.push({
            ...op.replaceOne.replacement
            , _id: idForCache
          });

        }


        /* TODO: Handle updateOne and delete operations, which currently are not utilized in bulkWrite.

        else if (op.updateOne && op.updateOne.update) {
          // If an update operation was successful, the document was modified.

          if (op.updateOne.filter) { // Assuming filter can find the doc

          }
        } else if (op.deleteOne || op.deleteMany) {
          // If op.deleteOne.filter._id is present, use it.
          if (op.deleteOne && op.deleteOne.filter && op.deleteOne.filter._id) {
            idsToDeleteFromCache.push(op.deleteOne.filter._id.toString());
          }
          // `deleteMany` is harder as it's a broader filter.
          // Cache invalidation for `deleteMany` might need to be broader or smarter.
        }
        */

        // Upsert operations (op.replaceOne with upsert:true or op.updateOne with upsert:true)
        // would also need handling. `bulkResult.getUpsertedIds()` provides info.
        // NOTE: While creation is now named upsert, it is a 'manual upsert'
        // As a result, this flag is also currently unused in bulkWrite.

        // If an upsert resulted in an insert: use `getUpsertedIds()` to get `_id` and op data to cache.
        // If an upsert resulted in an update: treat like `replaceOne` or `updateOne`.
        const upsertedInfo = bulkResult.getUpsertedIds().find(item => item.index === i);
        if (upsertedInfo) {
          // This was an upsert that resulted in an insert.
          // The document is in op.replaceOne.replacement or op.updateOne.update.$set (or similar)
          let docToCache;
          if (op.replaceOne && op.replaceOne.replacement) {
            docToCache = {
              ...op.replaceOne.replacement
              , _id: upsertedInfo._id
            };
          }

          /*else if (op.updateOne && op.updateOne.update) { 
            //Again, the more complex, but unused currently, codepath
          }*/

          if (docToCache) {
            docsToUpdateInCache.push(docToCache);
          }
        }

      }

      if (docsToUpdateInCache.length > 0) {
        // Filter out any undefined/null before passing to updateInCache
        updateInCache(docsToUpdateInCache.filter(doc => doc && doc._id));
      }
      if (idsToDeleteFromCache.length > 0) {
        // deleteInCache expects an array of objects with _id
        deleteInCache(idsToDeleteFromCache.map(_id => ({
          _id
        })));
      }
    }

    return bulkResult;
  };

  self.version = (...args) => baseStorage.version(...args);

  self.getLastModified = (...args) => baseStorage.getLastModified(...args);

  function cacheSupported() {
    return ctx.cache &&
      ctx.cache[colName] &&
      _.isArray(ctx.cache[colName]);
  }

  function updateInCache(doc) {
    if (doc && doc.isValid === false) {
      deleteInCache([doc._id])
    } else {
      ctx.bus.emit('data-update', {
        type: colName
        , op: 'update'
        , changes: doc
      });
    }
  }

  function deleteInCache(docs) {
    let changes
    if (_.isArray(docs)) {
      if (docs.length === 0) {
        return
      } else if (docs.length === 1 && docs[0]._id) {
        const _id = docs[0]._id.toString()
        changes = [_id]
      }
    }

    ctx.bus.emit('data-update', {
      type: colName
      , op: 'remove'
      , changes
    });
  }
}

module.exports = MongoCachedCollection;