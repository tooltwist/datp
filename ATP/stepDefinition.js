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