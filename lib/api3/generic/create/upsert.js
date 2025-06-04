'use strict';

const apiConst = require('../../const.json')
  , security = require('../../security.js')
  , validateCreate = require('./validate.js')
  , validateUpdate = require('../update/validate.js')
  , opTools = require('../../shared/operationTools.js');

/**
 * Upserts a [new?] document or documents into the collection
 * @param {Object} opCtx
 * @param {Array<Object>|Object} doc
 */
async function upsert(opCtx, doc) {

  const {
    ctx
    , auth
    , col
    , req
    , res
  } = opCtx;

  await security.demandPermission(opCtx, `api:${col.colName}:create`);

  const docs = (Array.isArray(doc) ? doc : [doc]);

  // For BulkWrite
  const operations = [];
  const successfulInsertsForEvents = [];
  const successfulReplacesForEvents = [];
  const processingErrors = [];
  const now = new Date();
  const currentTime = now.getTime();

  // Highly Parallel document preparation for BulkWrite, the order does not matter
  const preparationTasks = docs.map(async (originalDoc, i) => {
    const docItem = {
      ...originalDoc
    };
    let operationType = 'unknown';

    try {
      col.parseDate(docItem);
      opTools.resolveIdentifier(docItem);

      if (auth && auth.subject && auth.subject.name) {
        docItem.subject = auth.subject.name;
      }

      const identifyingFilter = col.storage.identifyingFilter(docItem.identifier, docItem, col.dedupFallbackFields);
      const existingResults = await col.storage.findOneFilter(identifyingFilter, {});
      const storageDoc = existingResults && existingResults.length > 0 ? existingResults[0] : null;

      if (storageDoc) { // Prepare for Replace
        operationType = 'update';
        await security.demandPermission(opCtx, `api:${col.colName}:update`, docItem.identifier, storageDoc);

        const validationResultUpdate = validateUpdate(opCtx, docItem, storageDoc, {
          isDeduplication: true
        });
        if (validationResultUpdate !== true) {
          const err = new Error(validationResultUpdate.message || 'Update validation failed');
          err.httpStatus = validationResultUpdate.httpStatus || apiConst.HTTP.BAD_REQUEST;
          throw err;
        }

        //Secondary check, 'borrowed' from update operation
        const modifiedDate = col.resolveDates(storageDoc)
          , ifUnmodifiedSince = req.get('If-Unmodified-Since');

        if (ifUnmodifiedSince &&
          dateTools.floorSeconds(modifiedDate) > dateTools.floorSeconds(new Date(ifUnmodifiedSince))) {
          const err = new Error('Document modified since last read');
          err.httpStatus = apiConst.HTTP.PRECONDITION_FAILED;
          throw err;
        }

        docItem.srvModified = currentTime;
        docItem.srvCreated = storageDoc.srvCreated || currentTime;
        if (opCtx.auth && opCtx.auth.subject && opCtx.auth.subject.name) {
          docItem.subject = opCtx.auth.subject.name;
        }
        operations.push({
          replaceOne: {
            filter: identifyingFilter
            , replacement: docItem
            , upsert: false
          , }
        , });
      } else { // Prepare for Insert
        operationType = 'create';
        await security.demandPermission(opCtx, `api:${col.colName}:create`, docItem.identifier);

        const validationResultCreate = validateCreate(opCtx, docItem);
        if (validationResultCreate !== true) {
          const err = new Error(validationResultCreate.message || 'Create validation failed');
          err.httpStatus = validationResultCreate.httpStatus || apiConst.HTTP.BAD_REQUEST;
          throw err; // Caught by the catch block below
        }

        docItem.srvModified = currentTime;
        docItem.srvCreated = currentTime;
        if (opCtx.auth && opCtx.auth.subject && opCtx.auth.subject.name) {
          docItem.subject = opCtx.auth.subject.name;
        }
        operations.push({ // Direct push to operations
          insertOne: {
            document: docItem
          , }
        , });
      }
      // This promise fulfills if successful. Its value isn't strictly necessary
      // if errors are handled by pushing to processingErrors.
      // This can be useful if Promise.allSettled is inspected in the future.
      return {
        status: 'fulfilled_preparation'
        , originalIndex: i
      };
    } catch (err) {
      // Errors from permission/validation/etc. are caught here
      processingErrors.push({
        index: i, // Original index from 'docs' array
        identifier: docItem.identifier || originalDoc.identifier || 'N/A'
        , error: err.message || 'Preparation failed'
        , httpStatus: err.httpStatus || apiConst.HTTP.INTERNAL_ERROR
        , operationType
      , });
      // Indicate that this specific task encountered a handled error.
      // This allows Promise.allSettled to show it as 'fulfilled' because the error was "handled" by us.
      // If we re-threw here, Promise.allSettled would show 'rejected' for this item.
      return {
        status: 'handled_error_in_preparation'
        , originalIndex: i
      };
    }
  });

  // Wait for all preparation tasks to complete.
  const settlementResults = await Promise.allSettled(preparationTasks);


  const responseFields = {
    insertedCount: 0
    , replacedCount: 0
    , matchedCount: 0, // from bulkWrite
    upsertedCount: 0, // from bulkWrite
    deletedCount: 0, // from bulkWrite (though not used here)
    errors: [...processingErrors], // Start with pre-check errors
  };

  if (operations.length > 0) {
    try {
      const bulkResult = await col.storage.bulkWrite(operations, {
        ordered: false
      });

      responseFields.insertedCount = bulkResult.insertedCount || 0;
      responseFields.replacedCount = bulkResult.modifiedCount || 0; // MongoDB bulkWrite uses modifiedCount for replace/update
      responseFields.matchedCount = bulkResult.matchedCount || 0;
      responseFields.upsertedCount = bulkResult.upsertedCount || 0;
      // responseFields.deletedCount = bulkResult.deletedCount || 0;


      // Error processing
      const writeErrorIndices = new Set(); //Using Set for O(1) lookups later on

      // Process write errors from bulkResult
      if (bulkResult.hasWriteErrors()) {
        const allWriteErrors = bulkResult.getWriteErrors(); // Get the errors array once.
        allWriteErrors.forEach(writeError => {
          // Add the index of the failed operation to our Set for quick lookup later.
          writeErrorIndices.add(writeError.index);

          // Populate responseFields.errors
          const opWithError = operations[writeError.index];
          let identifier = 'Unknown';
          let failedOpType = 'unknown';

          // Added a check for opWithError to prevent errors if index is somehow out of bounds, though unlikely.
          if (opWithError) {
            if (opWithError.insertOne && opWithError.insertOne.document) {
              identifier = opWithError.insertOne.document.identifier;
              failedOpType = 'insert';
            } else if (opWithError.replaceOne && opWithError.replaceOne.replacement) {
              identifier = opWithError.replaceOne.replacement.identifier;
              failedOpType = 'replace';
            } else if (opWithError.replaceOne && opWithError.replaceOne.filter) {
              identifier = opWithError.replaceOne.filter.identifier || (opWithError.replaceOne.filter.$and && opWithError.replaceOne.filter.$and[0] && opWithError.replaceOne.filter.$and[0].identifier);
              failedOpType = 'replace';
            }
          }

          responseFields.errors.push({
            operationIndex: writeError.index, // Original index in the 'operations' array
            identifier: identifier
            , code: writeError.code
            , error: writeError.errmsg
            , operationType: failedOpType
          });
        });
      }

      // Refined event emission
      // Iterate through the original operations submitted to bulkWrite.
      for (let i = 0; i < operations.length; i++) {
        const op = operations[i];

        // Check if there's an error for this specific original operation index
        if (writeErrorIndices.has(i)) {
          continue; // Skip if this operation failed at DB level
        }

        if (op.insertOne) {
          successfulInsertsForEvents.push(op.insertOne.document);
        } else if (op.replaceOne) {
          successfulReplacesForEvents.push(op.replaceOne.replacement);
        }
      }


      if (successfulInsertsForEvents.length > 0) {
        ctx.bus.emit('storage-socket-create', {
          colName: col.colName
          , docs: successfulInsertsForEvents
        });
      }
      if (successfulReplacesForEvents.length > 0) {
        ctx.bus.emit('storage-socket-update', {
          colName: col.colName
          , docs: successfulReplacesForEvents
        });
      }

      if (responseFields.insertedCount > 0 || responseFields.replacedCount > 0) {
        col.autoPrune();
        ctx.bus.emit('data-received');
      }

      // Determine overall status
      let httpStatus = apiConst.HTTP.OK;
      if (responseFields.errors.length > 0 && (responseFields.insertedCount > 0 || responseFields.replacedCount > 0)) {
        httpStatus = apiConst.HTTP.MULTI_STATUS; // Partial success
      } else if (responseFields.errors.length > 0) {
        httpStatus = apiConst.HTTP.BAD_REQUEST; // All failed or only pre-check errors
      } else if (responseFields.insertedCount === 0 && responseFields.replacedCount === 0 && operations.length > 0) {
        // No actual writes but operations were attempted (e.g. all matched but no modification needed, though less likely with replaceOne)
        // Or all operations resulted in write errors that were not pre-check errors.
        httpStatus = apiConst.HTTP.OK; // Or BAD_REQUEST if all had write errors
      }

      //The above should cover all cases, but add as needed.


      opTools.sendJSON({
        res
        , status: httpStatus
        , fields: responseFields
      });

    } catch (err) {
      // This catches errors from col.storage.bulkWrite() itself (e.g., connection issue)
      console.error('Bulk write operation failed entirely:', err);
      // Add to existing errors or overwrite if it's a fundamental failure
      responseFields.errors.push({
        error: `Bulk write execution failed: ${err.message}`
      });
      opTools.sendJSONStatus(res, apiConst.HTTP.INTERNAL_ERROR, apiConst.MSG.STORAGE_ERROR, {
        errors: responseFields.errors
      });
    }
  } else if (responseFields.errors.length > 0) {
    opTools.sendJSONStatus(res, apiConst.HTTP.BAD_REQUEST, 'Documents failed pre-processing.', {
      errors: responseFields.errors
    });
  } else if (docs.length > 0 && operations.length === 0 && processingErrors.length === 0) {
    // This case means all documents were filtered out before forming operations, without errors.
    // e.g. if some future check decided to skip all of them silently.
    opTools.sendJSONStatus(res, apiConst.HTTP.OK, 'No operations to perform based on input.');
  } else if (docs.length === 0) { // Should be caught by initial check
    opTools.sendJSONStatus(res, apiConst.HTTP.BAD_REQUEST, apiConst.MSG.HTTP_400_BAD_REQUEST_BODY_EMPTY_ARRAY);
  } else {
    opTools.sendJSONStatus(res, apiConst.HTTP.INTERNAL_ERROR, 'Failed to process documents, no operations generated.');
  }
}


module.exports = upsert;