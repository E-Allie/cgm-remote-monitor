'use strict';

const apiConst = require('../const.json')
  , stringTools = require('./stringTools')
  , uuid = require('uuid')
  , uuidNamespace = [...Buffer.from("NightscoutRocks!", "ascii")] // official namespace for NS :-)
;


function sendJSON({
  res
  , result
  , status
  , fields
}) {

  const json = {
    status: status || apiConst.HTTP.OK
    , result: result
  };

  if (result) {
    json.result = result
  }

  if (fields) {
    Object.assign(json, fields);
  }

  res.status(json.status).json(json);
}


function sendJSONStatus(res, status, title, description, warning) {

  const json = {
    status: status
  };

  if (title) {
    json.message = title
  }

  if (description) {
    json.description = description
  }

  // Add optional warning message.
  if (warning) {
    json.warning = warning;
  }

  res.status(status)
    .json(json);

  return title;
}


/**
 * Validate document's common fields
 * @param {Object} singleDoc
 * @param {Object} options
 * @returns {{error: boolean, message: string, httpStatus: number} | true} - Error object or true for success
 */
function validateSingleDoc(singleDoc, options) {

  const {
    isPatching
  } = options || {};

  if ((!isPatching || typeof(singleDoc.date) !== 'undefined')

    &&
    (typeof(singleDoc.date) !== 'number' ||
      singleDoc.date <= apiConst.MIN_TIMESTAMP)
  ) {
    return {
      error: true
      , message: apiConst.MSG.HTTP_400_BAD_FIELD_DATE
      , httpStatus: apiConst.HTTP.BAD_REQUEST
    };
  }


  if ((!isPatching || typeof(singleDoc.utcOffset) !== 'undefined')

    &&
    (typeof(singleDoc.utcOffset) !== 'number' ||
      singleDoc.utcOffset < apiConst.MIN_UTC_OFFSET ||
      singleDoc.utcOffset > apiConst.MAX_UTC_OFFSET)
  ) {
    return {
      error: true
      , message: apiConst.MSG.HTTP_400_BAD_FIELD_UTC
      , httpStatus: apiConst.HTTP.BAD_REQUEST
    };
  }


  if ((!isPatching || typeof(singleDoc.app) !== 'undefined')

    &&
    (typeof(singleDoc.app) !== 'string' ||
      stringTools.isNullOrWhitespace(singleDoc.app))
  ) {
    return {
      error: true
      , message: apiConst.MSG.HTTP_400_BAD_FIELD_APP
      , httpStatus: apiConst.HTTP.BAD_REQUEST
    };
  }

  return true;
}

/**
 * Validates a document or an array of documents.
 * @param {Object|Object[]} doc - The document or array of documents to validate.
 * @param {Object} options - Validation options, e.g., { isPatching: boolean }.
 * @returns {{error: boolean, message: string, httpStatus: number} | true} - Error object or true for success
 */
async function validateCommon(doc, options) {
  if (Array.isArray(doc)) {
    // If doc is an array, validate each item
    if (doc.length === 0) {
      return {
        error: true
        , message: "Input array cannot be empty."
        , httpStatus: apiConst.HTTP_400_BAD_REQUEST_BODY_EMPTY_ARRAY
      };
    }

    const validationPromises = doc.map(item => {
      const validationResult = validateSingleDoc(item, options);
      return validationResult ? Promise.resolve() : Promise.reject(validationResult);
    });

    try {
      await Promise.all(validationPromises);
    } catch (validationError) {
      return validationError;
    }

    // All items are valid, return true
    return true;
  } else {
    // If doc is not an array, validate it as a single object
    if (typeof doc !== 'object' || doc === null) {
      // return sendJSONStatus(res, apiConst.HTTP.BAD_REQUEST, "Input is not a valid object.");
      return {
        error: true
        , message: "Input is not a valid object."
        , httpStatus: apiConst.HTTP.BAD_REQUEST
      };
    }
    return validateSingleDoc(doc, options);
  }
}


/**
 * Calculate identifier for the document
 * @param {Object} doc
 * @returns string
 */
function calculateIdentifier(doc) {
  if (!doc)
    return undefined;

  let key = doc.device + '_' + doc.date;
  if (doc.eventType) {
    key += '_' + doc.eventType;
  }

  return uuid.v5(key, uuidNamespace);
}


/**
 * Validate identifier in the document
 * @param {Object} doc
 */
function resolveIdentifier(doc) {

  let identifier = calculateIdentifier(doc);
  if (doc.identifier) {
    if (doc.identifier !== identifier) {
      console.warn(`APIv3: Identifier mismatch (expected: ${identifier}, received: ${doc.identifier})`);
      console.log(doc);
    }
  } else {
    doc.identifier = identifier;
  }
}


module.exports = {
  sendJSON
  , sendJSONStatus
  , validateCommon
  , calculateIdentifier
  , resolveIdentifier
};