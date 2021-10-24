/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import fs from 'fs'

async function getDefinition(definition) {
  /*
    *  Load the definition of the step (which is probably a pipeline)
    */
  // console.log(`typeof(options.definition)=`, typeof(options.definition))
  switch (typeof(definition)) {
    case 'string':
      // console.log(`Loading definition for pipeline ${definition}`)
      const rawdata = fs.readFileSync(`./pipeline-definitions/transaction-${definition}.json`)
      return JSON.parse(rawdata);

  case 'object':
    // console.log(`already have definition`)
    return definition

  default:
    throw new Error(`Invalid value for parameter definition (${typeof(definition)})`)
  }
}

export default {
  getDefinition
}