'use strict';

const _ = require('lodash')
  , apiConst = require('../../const.json')
  , security = require('../../security')
  , upsert = require('./upsert')
  , opTools = require('../../shared/operationTools');


/**
 * CREATE: Upserts a [new?] document or documents into the collection
 */
async function create(opCtx) {

  const {
    req
    , res
  } = opCtx;
  const docs = (Array.isArray(req.body) ? req.body : [req.body]);

  // Check for empty body: array or object
  if (_.isEmpty(req.body)) {
    return opTools.sendJSONStatus(res, apiConst.HTTP.BAD_REQUEST, apiConst.MSG.HTTP_400_BAD_REQUEST_BODY);
  } else if (docs.length === 0) {
    return opTools.sendJSONStatus(res, apiConst.HTTP.BAD_REQUEST, apiConst.MSG.HTTP_400_BAD_REQUEST_BODY_EMPTY_ARRAY);
  }

  await upsert(opCtx, docs);
}

function createOperation(ctx, env, app, col) {

  return async function operation(req, res) {

    const opCtx = {
      app
      , ctx
      , env
      , col
      , req
      , res
    };

    try {
      opCtx.auth = await security.authenticate(opCtx);

      await create(opCtx);

    } catch (err) {
      console.error(err);
      if (!res.headersSent) {
        return opTools.sendJSONStatus(res, apiConst.HTTP.INTERNAL_ERROR, apiConst.MSG.STORAGE_ERROR);
      }
    }
  };
}

module.exports = createOperation;