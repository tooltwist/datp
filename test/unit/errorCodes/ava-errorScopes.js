/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { registerErrorLibrary, availableErrorLibraries, findErrorDefinitionByName, generateErrorByName } from '../../../lib/errorCodes'






// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {

  const definition_EN = require('./errors-EN.json')
  // console.log(`definition_EN=`, definition_EN)
  const definition_FIL = require('./errors-FIL.json')
  // console.log(`definition_FIL=`, definition_FIL)

  registerErrorLibrary(definition_EN)
  registerErrorLibrary(definition_FIL)
})









test.serial('Check available libraries', async t => {

  const available = availableErrorLibraries()
  t.is(available.length, 2)
  t.is(available[0].scope, 'unit-tests')
  t.is(available[0].lang, 'EN')
  t.is(available[1].scope, 'unit-tests')
  t.is(available[1].lang, 'FIL')
})


test.serial('Find definition by name, default language', async t => {

  const errorDefinition = findErrorDefinitionByName('FIELD_IS_REQUIRED')
  // console.log(`errorDefinition=`, errorDefinition)
  t.is(errorDefinition.name, 'FIELD_IS_REQUIRED')
  t.is(errorDefinition.code, '400-0002')
  t.is(errorDefinition.httpStatus, 400)
  t.is(errorDefinition.message, '{{field}} is required. Required parameter field cannot be found in the request.')
})


test.serial('Find definition by name, English', async t => {

  const errorDefinition = findErrorDefinitionByName('FIELD_IS_REQUIRED', 'EN')
  // console.log(`errorDefinition=`, errorDefinition)
  t.is(errorDefinition.name, 'FIELD_IS_REQUIRED')
  t.is(errorDefinition.code, '400-0002')
  t.is(errorDefinition.httpStatus, 400)
  t.is(errorDefinition.message, '{{field}} is required. Required parameter field cannot be found in the request.')
})


test.serial('Find definition by name, Filipino', async t => {
  const errorDefinition = findErrorDefinitionByName('FIELD_IS_REQUIRED', 'FIL')
  // console.log(`errorDefinition=`, errorDefinition)
  t.is(errorDefinition.name, 'FIELD_IS_REQUIRED')
  t.is(errorDefinition.code, '400-0002')
  t.is(errorDefinition.httpStatus, 400)
  t.is(errorDefinition.message, 'Kailangan ang {{field}}. Ang field ng kinakailangang parameter ay hindi mahanap sa kahilingan.')
})


test.serial('Find definition by name, unknown language', async t => {
  const errorDefinition = findErrorDefinitionByName('FIELD_IS_REQUIRED', 'XYZ')
  // console.log(`errorDefinition=`, errorDefinition)
  t.is(errorDefinition.name, 'FIELD_IS_REQUIRED')
  t.is(errorDefinition.code, '400-0002')
  t.is(errorDefinition.httpStatus, 400)
  t.is(errorDefinition.message, '{{field}} is required. Required parameter field cannot be found in the request.')
})


test.serial('Generate error that is not registered', async t => {

  const error = generateErrorByName('ERROR_YARP', { animal: 'Wombat' }, null, 'Please include {{animal}} in your collection.' )
  // console.log(`Unknown error=`, error)
  t.is(error.name, 'ERROR_YARP')
  t.is(error.code, 'ERROR_YARP')
  t.is(error.httpStatus, 400)
  t.is(error.message, 'Please include Wombat in your collection.')
  t.is(typeof(error.data), 'object')
  t.is(error.data.animal, 'Wombat')
})


test.serial('Generate error in default language', async t => {
  const error = generateErrorByName('FIELD_IS_REQUIRED', { field: 'Wombat' }, null, '{{field}} is required. Required parameter field cannot be found in the request.' )
  t.is(error.name, 'FIELD_IS_REQUIRED')
  t.is(error.code, '400-0002')
  t.is(error.httpStatus, 400)
  t.is(error.message, 'Wombat is required. Required parameter field cannot be found in the request.')
  t.is(typeof(error.data), 'object')
  t.is(error.data.field, 'Wombat')
})


test.serial('Generate error in English', async t => {
  const error = generateErrorByName('FIELD_IS_REQUIRED', { field: 'Wombat' }, 'EN', '{{field}} is required. Required parameter field cannot be found in the request.' )
  t.is(error.name, 'FIELD_IS_REQUIRED')
  t.is(error.code, '400-0002')
  t.is(error.httpStatus, 400)
  t.is(error.message, 'Wombat is required. Required parameter field cannot be found in the request.')
  t.is(typeof(error.data), 'object')
  t.is(error.data.field, 'Wombat')
})


test.serial('Generate error in Filipino', async t => {
  const error = generateErrorByName('FIELD_IS_REQUIRED', { field: 'Wombat' }, 'FIL', '{{field}} is required. Required parameter field cannot be found in the request.' )
  t.is(error.name, 'FIELD_IS_REQUIRED')
  t.is(error.code, '400-0002')
  t.is(error.httpStatus, 400)
  t.is(error.message, 'Kailangan ang Wombat. Ang field ng kinakailangang parameter ay hindi mahanap sa kahilingan.')
  t.is(typeof(error.data), 'object')
  t.is(error.data.field, 'Wombat')
})


test.serial('Generate error in unknown language', async t => {
  const error = generateErrorByName('FIELD_IS_REQUIRED', { field: 'Wombat' }, 'XYZ', '{{field}} is required. Required parameter field cannot be found in the request.' )
  // console.log(`error=`, error)
  t.is(error.name, 'FIELD_IS_REQUIRED')
  t.is(error.code, '400-0002')
  t.is(error.httpStatus, 400)
  t.is(error.message, 'Wombat is required. Required parameter field cannot be found in the request.')
  t.is(typeof(error.data), 'object')
  t.is(error.data.field, 'Wombat')
})

