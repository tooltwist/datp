import errors from 'restify-errors'
import constants from '../lib/constants'
import countries from '../lookup/countries'
import providers from '../providers-needToRemove/providers'



export default {
  init,
}

async function init(server) {
  console.log(`routes/countries:init()`)

  // Return all currencies
  server.get(`${constants.URL_PREFIX}/countries`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/gateway/countries`)
    res.send(countries)
    next()
  })//- /gateway/countries

  // Return a specific country
  server.get(`${constants.URL_PREFIX}/country/:countryCode`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/gateway/country/:countryCode`)
    const countryCode = req.params.countryCode
    for (const c of countries) {
      if (c.code === countryCode) {
        res.send(c.name)
        return next()
      }
    }
    // const country = countries.get(countryCode)
    return next(new errors.NotFoundError(`Unknown country code ${countryCode}`))
  })//- /gateway/country/:countryCode

  // Return countries for a specific provider
  server.get(`${constants.URL_PREFIX}/countries/:providerCode`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/gateway/countries/:providerCode`)
    const providerCode = req.params.providerCode

    const provider = await providers.get(providerCode)
    console.log(`provider=`, provider)

    const countries = provider.plugin.getCountries()
    res.send(countries)
    next()
  })//- /gateway/countries/:providerCode

}