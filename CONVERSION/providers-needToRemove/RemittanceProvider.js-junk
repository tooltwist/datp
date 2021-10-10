import errors from 'restify-errors'
import countries from '../lookup/countries'
import currencies from '../lookup/currencies'

export default class RemittanceProvider {

  constructor(code, name) {
    // console.log(`remittanceProvider.constructor()`)
    if (!code) {
      throw new Error(`Plugin ${this.constructor.name} called super() with incorrect parameters`)
    }
    this.code = code
    this.name = name
  }

  init(definition) {
    // Default init function, can be overridden if required
  }

  getProviderCode() {
    // throw new Error(`Invalid plugin ${this.constructor.name}: function getProviderCode is missing`)
    return this.code
  }

  getName() {
    // throw new Error(`Invalid plugin ${this.constructor.name}: function getName is missing`)
    return this.name
  }

  /**
   * This method can be overidden by providers.
   */
  getCurrencies() {
    // Default to just PHP
    const reply = { PHP: currencies['PHP'] }
    return reply
  }

  /**
   * This method can be overidden by providers.
   */
  getCountries() {
    // Default to just PH
    for (const c of countries) {
      if (c.code === 'PH') {
        return [ c ]
      }
    }
    return [ ] // Should not happen
  }

  /**
   * This method can be overidden by providers.
   */
  getFees(req, res, next) {
    console.log(`Unsupported method ${this.constructor.name}.getFees()`)
    res.send(new errors.NotImplementedError(`GetFees not supported by ${req.params.code}`))
    return next()
    // console.log(`req.params=`, req.params)
    // console.log(`req.body=`, req.body)

    // res.send({
    //   fees: 10.50,
    //   amount: 500.00,
    //   currency: 'PHP',
    //   total: 510.50,
    // })
    // return next()
  }
}