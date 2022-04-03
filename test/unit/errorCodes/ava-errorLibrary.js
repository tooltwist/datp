/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { ErrorLibrary } from '../../../lib/errorCodes'


test.serial('Create an English error library', async t => {

  // Load the JSON file
  const def_EN = require('./errors-EN.json')
  
  // Create the library and check it's values
  const lib = new ErrorLibrary(def_EN.scope, def_EN.lang, def_EN.description, def_EN.errors)
  // console.log(`lib=`, lib)
  t.is(lib.getScope(), 'unit-tests')
  t.is(lib.getLang(), 'EN')
  t.is(lib.getDescription(), 'Error codes for testing')
  
  const errorDefinition = lib.findErrorDefinitionByName('FIELD_IS_REQUIRED')
  t.is(errorDefinition.name, 'FIELD_IS_REQUIRED')
  t.is(errorDefinition.code, '400-0002')
  t.is(errorDefinition.httpStatus, 400)
  t.is(errorDefinition.message, '{{field}} is required. Required parameter field cannot be found in the request.')

  // Look for an error that does not exist
  const errorDefinition2 = lib.findErrorDefinitionByName('NO SUCH ERROR')
  t.falsy(errorDefinition2)
})



test.serial('Create an Filipino error library', async t => {

  // Load the JSON file
  const def_FIL = require('./errors-FIL.json')
  
  // Create the library and check it's values
  const lib = new ErrorLibrary(def_FIL.scope, def_FIL.lang, def_FIL.description, def_FIL.errors)
  t.is(lib.getScope(), 'unit-tests')
  t.is(lib.getLang(), 'FIL')
  t.is(lib.getDescription(), 'Error codes for testing')

  // Find an error definition
  const errorDefinition = lib.findErrorDefinitionByName('FIELD_IS_REQUIRED')
  t.is(errorDefinition.name, 'FIELD_IS_REQUIRED')
  t.is(errorDefinition.code, '400-0002')
  t.is(errorDefinition.httpStatus, 400)
  t.is(errorDefinition.message, 'Kailangan ang {{field}}. Ang field ng kinakailangang parameter ay hindi mahanap sa kahilingan.')

  // Look for an error that does not exist
  const errorDefinition2 = lib.findErrorDefinitionByName('NO SUCH ERROR')
  t.falsy(errorDefinition2)
})

