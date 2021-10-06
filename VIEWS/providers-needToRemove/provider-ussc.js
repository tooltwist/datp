import RemittanceProvider from './RemittanceProvider'

// See https://documenter.getpostman.com/view/4516853/SzzrZuwy

export default class RemittanceProviderUssc extends RemittanceProvider {
  constructor () {
    // console.log(`RemittanceProviderUssc.constructor()`)
    super('ussc', 'USSC')
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
      // https://documenter.getpostman.com/view/9236752/T17Dipay#40dd05d2-a823-4948-b0a8-c7ea64957be5
      "principal_amount": "100.00",
      "service_charge": "1.00",
      "message": "",
      "code": "0",
      "new_screen": "0",
      "journal_no": "011110556",
      "process_date": "20200701",
      "reference_number": "20200701PHB692534172",
      "total_amount": "101.00",
      "send_otp": "N"
    })
    return next()
  }
}