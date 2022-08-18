import axios from 'axios'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR } from "./responseClassifier"

/**
 * This function follows the same symantics as axios.get(), except
 * it also classifies the response according to your set of rules.
 * See https://axios-http.com/docs/api_intro
 * @param {string} url 
 * @param {object} options 
 * @param {Array<object>} rules 
 * @returns 
 */
export async function axiosGET(url, options, rules) {
  try {
    const response = await axios.get(url, options)
    // console.log(`response=`, response)
    const { classification, subClassification, status, data } = await responseClassifier(rules, false, response.status, response.data)
    return  { classification, subClassification, status, data, response }
  } catch (e) {
    // console.log(`e=`, e)
    if (e.code === 'ECONNABORTED') {
      // Timeout
      const { classification, subClassification, status, data } = await responseClassifier(rules, true)
      const response = { }
      return  { classification, subClassification, status: 599, data, response }
    } else if (e.code === 'ENOTFOUND') {

      // Bad URL
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status: 404,
        data: { },
        response: e.response
      }
    } else {

      // Another error
      // console.log(`e=`, e)
      const status = (e.response && e.response.status) ? e.response.status : 500
      const data = (e.response && e.response.data) ? e.response.data : { }
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status,
        data,
        response: e.response
      }
    }
  }
}

/**
 * This function follows the same symantics as axios.delete(), except
 * it also classifies the response according to your set of rules.
 * See https://axios-http.com/docs/api_intro
 * @param {string} url 
 * @param {object} options 
 * @param {Array<object>} rules 
 * @returns 
 */
 export async function axiosDELETE(url, options, rules) {
  try {
    const response = await axios.delete(url, options)
    // console.log(`response=`, response)
    const { classification, subClassification, status, data } = await responseClassifier(rules, false, response.status, response.data)
    return  { classification, subClassification, status, data, response }
  } catch (e) {
    // console.log(`e=`, e)
    if (e.code === 'ECONNABORTED') {
      // Timeout
      const { classification, subClassification, status, data } = await responseClassifier(rules, true)
      const response = { }
      return  { classification, subClassification, status: 599, data, response }
    } else if (e.code === 'ENOTFOUND') {

      // Bad URL
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status: 404,
        data: { },
        response: e.response
      }
    } else {

      // Another error
      // console.log(`e=`, e)
      const status = (e.response && e.response.status) ? e.response.status : 500
      const data = (e.response && e.response.data) ? e.response.data : { }
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status,
        data,
        response: e.response
      }
    }
  }
}


/**
 * This function follows the same symantics as axios.post(), except
 * it also classifies the response according to your set of rules.
 * See https://axios-http.com/docs/api_intro
 * @param {string} url 
 * @param {object} options 
 * @param {Array<object>} rules 
 * @returns 
 */
 export async function axiosPOST(url, requestData, options, rules) {
  try {
    const response = await axios.post(url, requestData, options)
    // console.log(`response=`, response)
    const { classification, subClassification, status, data } = await responseClassifier(rules, false, response.status, response.data)
    return  { classification, subClassification, status, data, response }
  } catch (e) {
    // console.log(`e=`, e)
    if (e.code === 'ECONNABORTED') {
      // Timeout
      const { classification, subClassification, status, data } = await responseClassifier(rules, true)
      const response = { }
      return  { classification, subClassification, status: 599, data, response }
    } else if (e.code === 'ENOTFOUND') {

      // Bad URL
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status: 404,
        data: { },
        response: e.response
      }
    } else {

      // Another error
      // console.log(`e=`, e)
      const status = (e.response && e.response.status) ? e.response.status : 500
      const data = (e.response && e.response.data) ? e.response.data : { }
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status,
        data,
        response: e.response
      }
    }
  }
}

/**
 * This function follows the same symantics as axios.put(), except
 * it also classifies the response according to your set of rules.
 * See https://axios-http.com/docs/api_intro
 * @param {string} url 
 * @param {object} options 
 * @param {Array<object>} rules 
 * @returns 
 */
 export async function axiosPUT(url, requestData, options, rules) {
  try {
    const response = await axios.put(url, requestData, options)
    // console.log(`response=`, response)
    const { classification, subClassification, status, data } = await responseClassifier(rules, false, response.status, response.data)
    return  { classification, subClassification, status, data, response }
  } catch (e) {
    // console.log(`e=`, e)
    if (e.code === 'ECONNABORTED') {
      // Timeout
      const { classification, subClassification, status, data } = await responseClassifier(rules, true)
      const response = { }
      return  { classification, subClassification, status: 599, data, response }
    } else if (e.code === 'ENOTFOUND') {

      // Bad URL
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status: 404,
        data: { },
        response: e.response
      }
    } else {

      // Another error
      // console.log(`e=`, e)
      const status = (e.response && e.response.status) ? e.response.status : 500
      const data = (e.response && e.response.data) ? e.response.data : { }
      return {
        classification: RESPONSE_CLASSIFICATION_ERROR,
        subClassification: null,
        status,
        data,
        response: e.response
      }
    }
  }
}