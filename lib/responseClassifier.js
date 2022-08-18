

// export const RESPONSE_TIMEOUT = 'timeout'
export const RESPONSE_CLASSIFICATION_OK = 'ok'
export const RESPONSE_CLASSIFICATION_ERROR = 'error'
export const RESPONSE_CLASSIFICATION_OFFLINE = 'offline'

export const RESPONSE_RULE_TIMEOUT = 'timeout'
export const RESPONSE_RULE_STATUS = 'status'
export const RESPONSE_RULE_VALUE = 'value'
export const RESPONSE_RULE_DEFAULT = 'default'

/**
 * Pass this function details of an API call you made, and it
 * will classify the response according to a set of rules you
 * provide. This method is primarily used to determine when a backend
 * system is not available, but you can use it to determine custom
 * conditions as well.
 * 
 * Examples:
 * [
 *  { type: RESPONSE_RULE_TIMEOUT, classification: RESPONSE_CLASSIFICATION_OFFLINE },
 *  { type: RESPONSE_RULE_STATUS, value: 200, classification: RESPONSE_CLASSIFICATION_OK },
 *  { type: RESPONSE_RULE_STATUS, value: 404, classification: RESPONSE_CLASSIFICATION_ERROR },
 *  { type: RESPONSE_RULE_STATUS, value: '5*', classification: RESPONSE_CLASSIFICATION_ERROR },
 *  { type: RESPONSE_RULE_STATUS, classification: RESPONSE_CLASSIFICATION_ERROR }, // default
 *  { type: RESPONSE_RULE_VALUE, field: 'tx.status', value: 'TC', classification: RESPONSE_CLASSIFICATION_OK },
 *  { type: RESPONSE_RULE_VALUE, field: 'tx.status', value: 'NA', classification: RESPONSE_CLASSIFICATION_OFFLINE },
 *  { type: RESPONSE_RULE_VALUE, field: 'tx.status', value: 'FAIL', classification: RESPONSE_CLASSIFICATION_ERROR },
 *  { type: RESPONSE_RULE_VALUE, classification: RESPONSE_CLASSIFICATION_ERROR } // default
 * ]
 * 
 * @param {*} rules [{ type, field, value, classification}, ...]
 * @param {boolean} timedOut Pass true if your request timed out.
 * @param {number} status HTTP status returned by your request.
 * @param {object} data The response to your API call.
 */
export async function responseClassifier(rules, timedOut=false, status=0, data={}) {

  // Handle timeouts
  if (timedOut) {
    // Look for timeout in the rules table
    for (const rule of rules) {
      if (rule.type === RESPONSE_RULE_TIMEOUT) {
        return {
          classification: rule.classification,
          subClassification: rule.subClassification ? rule.subClassification : null,
          status: 599,
          data: { }
        }
      }
    }//- next rule

    // No rule for timeout. We'll call it an offline problem.
    return {
      classification: RESPONSE_CLASSIFICATION_OFFLINE,
      subClassification: null,
      status: 599,
      data: { }
    }
  }

  /*
   *  Look for status errors
   */
  for (const rule of rules) {
    if (rule.type === RESPONSE_RULE_STATUS) {

      /*
       * Let's look at the status
       */
      if (typeof(rule.value) === 'number') {

        // Looking for a specific status code
        if (status === rule.value) {
          // Exact match on the status code
          return {
            classification: rule.classification,
            subClassification: rule.subClassification ? rule.subClassification : null,
            status,
            data
          }
        }
      } else if (typeof(rule.value) === 'string') {

        // Looking for 200, 2XX, 21*, etc
        let statusStr = `${status}`
        let pattern = rule.value.toUpperCase()

        if (pattern.charAt(pattern.length-1) === '*') {

          // Handle 4*, etc
          pattern = pattern.substring(0, pattern.length-1)
          // console.log(`pattern=`, pattern)
          if (statusStr.length > pattern.length) {
            statusStr = statusStr.substring(0, pattern.length)
          }
          if (statusStr === pattern) {
            return {
              classification: rule.classification,
              subClassification: rule.subClassification ? rule.subClassification : null,
              status,
              data
            }
          }
          continue // on to the next rule
        } else {
          // Handle exact match or 50X, or 4XX etc
          while (pattern != '' && pattern.endsWith('X')) {
            pattern = pattern.substring(0, pattern.length - 1)
            statusStr = statusStr.substring(0, statusStr.length - 1)
          }
          if (statusStr === pattern) {
            return {
              classification: rule.classification,
              subClassification: rule.subClassification ? rule.subClassification : null,
              status,
              data
            }
          }
          continue // on to the next rule
        }
      }
    } else if (rule.type === RESPONSE_RULE_VALUE) {
      const field = rule.field
      const requiredValue = rule.value
      const actualValue = getValueFromResponse(data, field)

      if (actualValue !== undefined && actualValue === requiredValue) {
        return {
          classification: rule.classification,
          subClassification: rule.subClassification ? rule.subClassification : null,
          status,
          data
        }
      }
    }//- RESPONSE_RULE_VALUE
  }//- next rule

  // We didn't find a pattern match, do we specify a default rule?
  for (const rule of rules) {
    if (rule.type === RESPONSE_RULE_DEFAULT) {
      let classification = rule.classification
      if (classification === RESPONSE_CLASSIFICATION_OFFLINE) {
        // We cannot assume the default is offline!
        classification = RESPONSE_CLASSIFICATION_ERROR
      }
      return {
        classification,
        subClassification: null,
        status,
        data
      }
    }
  }

  // Default classification
  if (status < 400) {
    return {
      classification: RESPONSE_CLASSIFICATION_OK,
      subClassification: null,
      status,
      data
    }
  }
  return {
    classification: RESPONSE_CLASSIFICATION_ERROR,
    subClassification: null,
    status,
    data
  }
}

function getValueFromResponse(obj, path) {
  // console.log(`getValueFromResponse(path=${path})`, obj)
  if (typeof(obj) !== 'object' || !path) {
    return undefined
  }

  // See if this is multi-part (e.g. x.y.z)
  const pos = path.indexOf('.')
  if (pos === 0) {
    // .y
    return getValueFromResponse(obj, path.substring(1))
  } else if (pos < 0) {
    // y
    return obj[path]
  } else if (pos > 0) {
    // x.y
    const part1 = path.substring(0, pos)
    const part2 = path.substring(pos + 1)
    // console.log(`part1=`, part1)
    // console.log(`part2=`, part2)
    const value = obj[part1]
    return getValueFromResponse(value, part2)
  }
}

// function isDigit(c) {
//   return c >= '0' && c <= '9'
// }