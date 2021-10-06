import errors from 'restify-errors'
// import query from '../lib/query'
import constants from '../lib/constants'
// import providers from '../providers/providers'
import currencies from '../lookup/currencies'
import providers from '../providers-needToRemove/providers'



export default {
  init,
}

async function init(server) {
  console.log(`countries/lookups:init()`)

  // Return all currencies
  server.get(`${constants.URL_PREFIX}/currencies`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/gateway/currencies`)
    res.send(currencies)
    next()
  })//- /gateway/currencies

  // Return a specific currency
  server.get(`${constants.URL_PREFIX}/currency/:currencyCode`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/gateway/currency/:currencyCode`)
    const currencyCode = req.params.currencyCode

    const currency = currencies[currencyCode]
    if (currency) {
      res.send(currency)
      return next()
    } else {
      return next(new errors.NotFoundError(`Unknown currency ${currencyCode}`))
    }
  })//- /gateway/currency/:currencyCode

  // Return currencies for a specific provider
  server.get(`${constants.URL_PREFIX}/currencies/:providerCode`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/gateway/currencies/:providerCode`)
    const providerCode = req.params.providerCode

    const provider = await providers.get(providerCode)
    console.log(`provider=`, provider)

    const currencies = provider.plugin.getCurrencies()
    res.send(currencies)
    next()
  })//- /gateway/currency/:currencyCode

}