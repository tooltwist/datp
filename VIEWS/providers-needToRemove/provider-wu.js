import countries from '../lookup/countries'
import currencies from '../lookup/currencies'
import RemittanceProvider from './RemittanceProvider'

// See https://documenter.getpostman.com/view/4516853/SzzrZuwy

export default class RemittanceProviderWu extends RemittanceProvider {
  constructor() {
    // console.log(`RemittanceProviderWu.constructor()`)
    super('wu', 'Western Union')
  }

  getCurrencies() {
    // All currencies
    return currencies
  }

  getCountries() {
    // All countries
    return countries
  }

  // See https://documenter.getpostman.com/view/4516853/SzzrZuwy#97815ae3-b008-4198-a03b-33e78dacd839

  getFees(req, res, next) {
    console.log(`----------------------------------------`)
    console.log(`${this.constructor.name}.getFees()`)
    // console.log(`req.params=`, req.params)
    // console.log(`req.body=`, req.body)
    const providerCode = req.params.code

    //ZZZZ
    // Reference:

    res.send({
      provider_code: providerCode,
      // ZZZZZ Un-normalized example:
      // Reference: FeeInquiry: https://drive.google.com/file/d/1MWhrsoN4hHBSDhjeekuIcSPRMoBUuuR8/view
      "taxes": {
        "municipal_tax": 0,
        "state_tax": 0,
        "county_tax": 0,
        "tax_worksheet": ""
      },
      "originators_principal_amount": 100000,
      "destination_principal_amount": 100000,
      "gross_total_amount": 101500,
      "plus_charges_amount": 0,
      "pay_amount": 100000,
      "charges": 1500,
      "tolls": 0,
      "originating_currency_principal": "",
      "canadian_dollar_exchange_fee": 0,
      "message_charge": 0,
      "promo_code_description": "",
      "promo_sequence_no": "",
      "promo_name": "",
      "promo_discount_amount": 0,
      "exchange_rate": 1,
      "base_message_charge": "5000",
      "base_message_limit": "10",
      "incremental_message_charge": "500",
      "incremental_message_limit": "10"
    })
    return next()
  }
}