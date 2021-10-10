import RemittanceProvider from './RemittanceProvider'

// See https://documenter.getpostman.com/view/4516853/SzzrZuwy

export default class RemittanceProviderCebuana extends RemittanceProvider {
  constructor () {
    // console.log(`RemittanceProviderCebuana.constructor()`)
    super('cebuana', 'Cebuana')
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
      // https://documenter.getpostman.com/view/4516853/SzzrZuwy#97815ae3-b008-4198-a03b-33e78dacd839
      "ResultStatus": "Successful",
      "MessageID": 0,
      "LogID": 0,
      "ServiceFee": "1.0000"
    })
    return next()
  }
}