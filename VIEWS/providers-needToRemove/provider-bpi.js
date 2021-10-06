import RemittanceProvider from './RemittanceProvider'

export default class RemittanceProviderBpi extends RemittanceProvider {
  constructor () {
    // console.log(`RemittanceProviderBpi.constructor()`)
    super('bpi', 'Bank of the Philippine Islands')
  }
}