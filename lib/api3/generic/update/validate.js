'use strict';

const apiConst = require('../../const.json')
  , opTools = require('../../shared/operationTools');


/**
 * Validation of document to update
 * @param {Object} opCtx
 * @param {Object} doc
 * @param {Object} storageDoc
 * @param {Object} options
 * @returns {{error: boolean, message: string, httpStatus: number} | true} - Error object or true for success
 */
function validate(opCtx, doc, storageDoc, options) {

  const {
    isPatching
    , isDeduplication
  } = options || {};

  const immutable = ['identifier', 'date', 'utcOffset', 'eventType', 'device', 'app'
    , 'srvCreated', 'subject', 'srvModified', 'modifiedBy', 'isValid'];

  if (storageDoc.isReadOnly === true || storageDoc.readOnly === true || storageDoc.readonly === true) {
    return {
      error: true
      , message: apiConst.MSG.HTTP_422_READONLY_MODIFICATION
      , httpStatus: apiConst.HTTP.UNPROCESSABLE_ENTITY
    };
  }

  for (const field of immutable) {

    // change of identifier is allowed in deduplication (for APIv1 documents)
    if (field === 'identifier' && isDeduplication)
      continue;

    // changing deleted document is without restrictions
    if (storageDoc.isValid === false)
      continue;

    if (typeof(doc[field]) !== 'undefined' && doc[field] !== storageDoc[field]) {
      return {
        error: true
        , message: apiConst.MSG.HTTP_400_IMMUTABLE_FIELD.replace('{0}', field)
        , httpStatus: apiConst.HTTP.BAD_REQUEST
      };
    }
  }

  const commonValidationResult = opTools.validateCommon(doc, {
    isPatching
  });
  if (commonValidationResult !== true && commonValidationResult.error) {
    return commonValidationResult;
  }

  return true;
}

module.exports = validate;