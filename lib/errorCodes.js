/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import assert from 'assert'

const DEFAULT_LANGUAGE_CODE = 'EN'
const VERBOSE = 0

/**
 * This class stores a list of error definitions for a specific scope/language.
 * 
 * The errors can be accessed by either name or by their error code.
 */
export class ErrorLibrary {
  #scope
  #lang
  #description
  #errorMapByName
  #errorMapByCode

  /**
   * 
   * @param {*} scope 
   * @param {*} description 
   * @param {Array} errorArray Array of { name, code, message, httpStatus }
   */
  constructor(scope, lang, description, errorArray) {
    this.#scope = scope
    this.#lang = lang
    this.#description = description

    this.#errorMapByName = new Map()
    this.#errorMapByCode = new Map()

    // Strip out unneeded values
    for (const err of errorArray) {
      const properties = Object.keys(err)
      for (const prop of properties) {
        if (prop!=='name' && prop!=='code' && prop!=='httpStatus' && prop!=='message') {
          delete err[prop]
        }
      }
    }

    // Add the errors to the map
    for (const err of errorArray) {
      this.#errorMapByName.set(err.name, err)
      this.#errorMapByCode.set(err.code, err)
    }
  }

  /**
   * @returns string
   */
  getScope() {
    return this.#scope
  }

  /**
   * @returns string
   */
  getLang() {
    return this.#lang
  }

  getDescription() {
    return this.#description
  }

  /**
   * Find an error by it's name
   * @param {string} name 
   * @returns { name, code, message, httpStatus }
   */
  findErrorDefinitionByName(name) {
    const error = this.#errorMapByName.get(name)
    return error ? error : null
  }

  /**
   * Find an error by it's error code
   * @param {string} code 
   * @returns { name, code, message, httpStatus }
   */
  findErrorDefinitionByCode(code) {
    const error = this.#errorMapByCode.get(code)
    return error ? error : null
  }
}


/**
 * This class allows a subsystem to define a set of errors, in multiple languages.
 */
export class ErrorScope {
  #scope
  #languages // lang -> ErrorLibrary
  #defaultLanguage // EN

  /**
   * 
   * @param {string} scope Usually the application name
   */
  constructor(scope) {
    this.#scope = scope
    this.#languages = new Map()
    this.#defaultLanguage = null
  }

  getScope() {
    return this.#scope
  }

  /**
   * 
   * @param {*} library 
   */
  registerErrorLibrary(library) {
    assert(library.getScope() === this.#scope)
    const lang = library.getLang()
    this.#languages.set(lang, library)

    if (lang === DEFAULT_LANGUAGE_CODE) {
      this.#defaultLanguage = library
    }
  }//- registerErrorLibrary


  /**
   * Find an error by it's name.
   * If a preferred language is specified, that language's library is checked
   * first (if it exists) and if the error is not found our default language
   * (i.e. English) is then checked. If no error is found null is returned.
   * @param {string} name
   * @param {string} preferredLanguage 
   * @returns object { name, code, message, httpStatus }
   */
  findErrorDefinitionByName(name, preferredLanguage=null) {

    // If they have a preferred language that is not English, see
    // if this scope has this error defined in that language.
    if (preferredLanguage && preferredLanguage !== DEFAULT_LANGUAGE_CODE) {
      const language = this.#languages.get(preferredLanguage)
      if (language) {
        const error = language.findErrorDefinitionByName(name)
        if (error) {
          return { ...error, scope: this.#scope }
        }
      }
    }

    // We haven't found the error in a non-English library, so look for the
    // error now in our English library (assuming one is defined).
    if (this.#defaultLanguage) {
      const error = this.#defaultLanguage.findErrorDefinitionByName(name)
      if (error) {
        return { ...error, scope: this.#scope }
      }
    }

    // Not found in a preferred language or in the English error library.
    return null
  }//- findErrorDefinitionByName

  /**
   * Find an error by it's error code.
   * If a preferred language is specified, that language's library is checked
   * first (if it exists) and if the error is not found our default language
   * (i.e. English) is then checked. If no error is found null is returned.
   * @param {string} code 
   * @param {string} preferredLanguage 
   * @returns object { name, code, message, httpStatus }
   */
  findErrorDefinitionByCode(code, preferredLanguage=null) {

    // If they have a preferred language that is not English, see
    // if this scope has this error defined in that language.
    if (preferredLanguage && preferredLanguage !== DEFAULT_LANGUAGE_CODE) {
      const language = this.#languages.get(preferredLanguage)
      if (language) {
        const error = language.findErrorDefinitionByName(name)
        if (error) {
          return { ...error, scope: this.#scope }
        }
      }
    }

    // We haven't found the error in a non-English library, so look for the
    // error now in our English library (assuming one is defined).
    if (!error && this.#defaultLanguage) {
      const error = this.#defaultLanguage.findErrorDefinitionByName(name)
      if (error) {
        return { ...error, scope: this.#scope }
      }
    }

    // Not found in a preferred language or in the English error library.
    return null
  }//- findErrorDefinitionByCode

  /**
   * 
   * @returns Array of { scope, lang }
   */
  available() {
    let list = [ ]
    for (const lang of this.#languages.keys()) {
      list.push({ scope: this.#scope, lang })
    }
    return list
  }
}

const scopes = new Map // scope -> ErrorScope


/**
 *  Register a set of error codes, usually from a JSON file.  
 * Example definition:
```json
      {
        "lang": "EN",
        "scope": "xpanse",
        "description": "XPanse errors",
        "errors": [
          {
            "name": "FIELD_IS_REQUIRED",
            "httpStatus": 400,
            "code": "400-0002",
            "message": "{{field}} is required. Required parameter field cannot be found in the request."
          },
          ...
        ]
      }
```
 * 
 * @param {object} definition
 * 
 */
export function registerErrorLibrary(definition) {

  // Create the library from the definition
  const scope = definition.scope
  const lang = definition.lang
  const description = definition.description
  const library = new ErrorLibrary(scope, lang, description, definition.errors)

  // Find the errors for the library's scope.
  let scopeErrors = scopes.get(scope)
  if (!scopeErrors) {
    scopeErrors = new ErrorScope(scope)
    scopes.set(scope, scopeErrors)
  }
  // Add this library to the scope
  scopeErrors.registerErrorLibrary(library)
}//- registerErrorLibrary


/**
 * Return a list of scope/language pairs for available error libraries.
 * @returns 
 */
export function availableErrorLibraries() {
  let list = [ ]
  for (const scope of scopes.values()) {
    const avail = scope.available()
    list = [ ...list, ...avail ]
  }
  return list
}

/**
 * Search the registered scopes and languages to find the definition for the specified error.
 * @param {string} name Error name
 * @param {string} preferredLanguage Preferred language if not the default (i.e. English)
 * @returns  object { name, code, message, httpStatus }
 */
export function findErrorDefinitionByName(name, preferredLanguage=null) {
  for (const scopeErrors of scopes.values()) {
    const error = scopeErrors.findErrorDefinitionByName(name, preferredLanguage)
    if (error) {
      return error
    }
  }
  return null
}

/**
 * This function generates an error based on an error name, getting the error
 * definition from the currently registered error libraries. The error scopes,
 * which normally equate to various modules in your program are checked sequentially
 * for an error definition, first for the user's preferred language (if there is one),
 * and then for the default language (i.e. English).
 * 
 * The values provided in `parameters` are substituted into the error's message,
 * replacing {{name}} with the value of `parameters.name`.
 * 
 * An optional `exampleMessage` may be provided, to simplfy development, and provide
 * better handling of missing error definitions. If the error cannot be found, two
 * things happen:
 * 
 * 1. A JSON code snippet is written to the console, that can be added to an error
 *    message definition.
 * 
 * 2. A semi-correct error is still provided so the application can continue.
 * 
 * This approach allows application coders to mostly ignore the error definition files
 * while they concentrate on the application login, knowing that skeleton error
 * definitions will be written to the console as a reminder during testing.
 * 
 * @param {string} name 
 * @param {string} exampleMessage 
 * @param {object} parameters Object containing name/value pairs.
 * @param {string} preferredLanguage 
 * @returns 
 */
export function generateErrorByName(name, parameters={}, preferredLanguage=null, exampleMessage='') {
  if (VERBOSE) console.log(`generateErrorByName(${name}, lang=${preferredLanguage}, exampleMessage=${exampleMessage})`)

  let error = findErrorDefinitionByName(name, preferredLanguage)
  // console.log(`error=`, error)

  // If the error was not found, display a useful message.
  let code
  let httpStatus
  let message
  let scope
  if (error) {
    code = error.code
    httpStatus = error.httpStatus
    message = error.message
    scope = error.scope
  } else {

    // Could not find this error in the various scope error libraries

    code = name
    // Try to guess the http status
    httpStatus = 400
    if (!exampleMessage) {
      exampleMessage = 'Error message goes here'
    }
    message = exampleMessage
    scope = 'undefined'
    // const c1 = code.charAt(0)
    // if ((c1==='4' || c1==='5') && !isNaN(c2) && !isNaN(c3)) {
    //     httpStatus = code.substring(0, 2)
    // }

    // Check all the parameters are included in the error message
    let sep = ' - '
    for (const param in parameters) {
      if (!exampleMessage.includes(`{{${param}}}`)) {
        exampleMessage += `${sep}{{${param}}}`
        sep = ', '
      }
    }

    // Write the snippet to the console.
    console.log(``)
    console.log(`Error '${name}' was not found in any of the error libraries.`)
    console.log(`Please edit the following JSON snippet and add it to your error library:`)
    console.log(`{`)
    console.log(`    "code": "${code}",`)
    console.log(`    "name": "${name}",`)
    console.log(`    "httpStatus": "${httpStatus}",`)
    console.log(`    "message": "${exampleMessage}",`)
    console.log(`}`)
  }

  // Substitute values into the message
  for (const param in parameters) {
    const value = parameters[param]
    // console.log(`insert ${param}=${value} into message`)
    const pattern = `{{${param}}}`
    message = message.replace(pattern, value.toString())
    // console.log(`message=`, message)
  }

  return {
    error: name,
    code,
    httpStatus,
    message,
    scope,
    data: { ...parameters }
  }
}
