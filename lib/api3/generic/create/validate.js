'use strict';

const apiConst = require('../../const.json')
  , stringTools = require('../../shared/stringTools')
  , opTools = require('../../shared/operationTools');


/**
 * Validation of document to create
 * @param {Object} opCtx
 * @param {Object} doc
 * @returns {{error: boolean, message: string, httpStatus: number} | true} - Error object or true for success
 */
async function validate(opCtx, doc) {

  if (Array.isArray(doc)) {
    // Validate each in parallel, but try to short-circuit on the first error
    const validationPromises = doc.map(item => {
      if (typeof item.identifier !== 'string' || stringTools.isNullOrWhitespace(item.identifier)) {
        return Promise.reject({
          error: true
          , message: apiConst.MSG.HTTP_400_BAD_FIELD_IDENTIFIER
          , httpStatus: apiConst.HTTP.BAD_REQUEST
        });
      }
      return Promise.resolve();
    });

    try {
      await Promise.all(validationPromises);
    } catch (validationError) {
      return validationError;
    }
  } else if (typeof(doc.identifier) !== 'string' || stringTools.isNullOrWhitespace(doc.identifier)) {
    return {
      error: true
      , message: apiConst.MSG.HTTP_400_BAD_FIELD_IDENTIFIER
      , httpStatus: apiConst.HTTP.BAD_REQUEST
    };
  }

  const commonValidationResult = opTools.validateCommon(doc);
  if (commonValidationResult !== true && commonValidationResult.error) {
    return commonValidationResult;
  }

  return true;
}

module.exports = validate;