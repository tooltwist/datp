// import constants from '../lib/constants'
import providers from './providers-needToRemove/providers'
import errors from 'restify-errors'
// import parameters from '../lib/parameters'
import query from '../database/query'
import constants from './lib/constants'
// import parameters from '../views/lib/parameters'


// const providers = [
//   {
//     code: 'bpi',
//     description: 'Bank of the Philippine Islands'
//   },
//   {
//     code: 'cebuana',
//     description: 'Cebuana'
//   },
//   {
//     code: 'iremit',
//     description: 'iRemit'
//   },
//   {
//     code: 'landbank',
//     description: 'Landbank'
//   },
//   {
//     code: 'metrobank',
//     description: 'Metrobank'
//   },
//   {
//     code: 'ria',
//     description: 'RIA'
//   },
//   {
//     code: 'smart',
//     description: 'Smart Pedala'
//   },
//   {
//     code: 'transfast',
//     description: 'Transfast'
//   },
//   {
//     code: 'ussc',
//     description: 'USSC'
//   },
//   {
//     code: 'xpress',
//     description: 'Xpress Money'
//   },
//   {
//     code: 'wu',
//     description: 'Western Union'
//   },
// ]


export default {
  init,
}

async function init(server) {

  // // Sort the list of providers
  // providers.sort((p1, p2) => {
  //   if (p1.code < p2.code) return -1
  //   if (p1.code > p2.code) return +1
  //   return 0
  // })

  // Return a list of providers
  server.get(`${constants.URL_PREFIX}/metadata/providers`, async function(req, res, next) {

    console.log(`---------------------------------`)
    console.log(`/metadata/providers`)

    const list = await providers.all()
    res.send(list)
    next()
  })//

  // Get a specific provider
  server.get(`${constants.URL_PREFIX}/metadata/provider/:code`, async function(req, res, next) {
    const code = req.params.code

    console.log(`---------------------------------`)
    console.log(`/metadata/provider/${code}`)

    const provider = await providers.get(code)
    // console.log(`provider=`, provider)
    if (!provider) {
      res.send(new errors.NotFoundError(`Unknown provider ${code}`))
      return next()
    }
    res.send(provider)
    next()
  })//

  // Return a list of services
  server.get(`${constants.URL_PREFIX}/metadata/services`, async function(req, res, next) {

    console.log(`---------------------------------`)
    console.log(`/metadata/services`)

    const sql = `
      SELECT S.service, S.description, C.category, C.description AS category_description
      FROM service S
      LEFT JOIN service_category C ON C.category = S.category
      ORDER BY C.sequence
      `
    console.log(`sql=`, sql)
    const result = await query(sql)
    console.log(`result=`, result)
    // const list = await providers.all()
    res.send(result)
    next()
  })//

  // Get services for a provider
  server.get(`${constants.URL_PREFIX}/metadata/services/:providerCode`, async function(req, res, next) {
    const providerCode = req.params.providerCode

    console.log(`---------------------------------`)
    console.log(`/metadata/services/${providerCode}`)

    const provider = await providers.get(providerCode)
    // console.log(`provider=`, provider)
    if (!provider) {
      res.send(new errors.NotFoundError(`Unknown provider ${providerCode}`))
      return next()
    }

    const sql = `
      SELECT ps.provider, ps.service, s.description
      FROM provider_service ps
      LEFT JOIN service s ON s.service = ps.service
      WHERE provider=?`
    const params = [ providerCode ]
    const result = await query(sql, params)
    // console.log(`result=`, result)
    res.send(result)
    next()
  })//

  // Get services for a provider
  server.get(`${constants.URL_PREFIX}/metadata/views/:providerCode/:serviceCode`, async function(req, res, next) {
    const providerCode = req.params.providerCode
    const serviceCode = req.params.serviceCode
    const withFields = req.params.fields
    const withMappings = req.params.mappings
    // console.log(`req.params=`, req.params)
    // console.log(`withFields=`, withFields)

    console.log(`---------------------------------`)
    console.log(`/metadata/views/${providerCode}/${serviceCode}`)

    // Check the provider exists
    const provider = await providers.get(providerCode)
    // console.log(`provider=`, provider)
    if (!provider) {
      res.send(new errors.NotFoundError(`Unknown provider ${providerCode}`))
      return next()
    }

    // Find the views for this provider / service
    const viewPattern = `${providerCode}-${serviceCode}-%`
    const views = await parameters.getViews(constants.PETNET_TENANT, viewPattern)
    console.log(`views=`, views)
    const reply = [ ]
    for (const v of views) {
      const viewSuffix = v.view.substring(viewPattern.length - 1)
      const record = {
        provider: providerCode,
        service: serviceCode,
        message: viewSuffix,
        view: v.view,
        description: v.description,
      }
      const version = -1 //ZZZZZZ Version should be specified
      if (withFields === 'true') {
        //ZZZZ Should use schema
        record.fields = await parameters.getFields(constants.PETNET_TENANT, v.view, version)
        // console.log(`record.fields=`, record.fields)
      }
      if (withMappings === 'true') {
        //ZZZZ Should use schema
        record.mapping = await parameters.getMapping(constants.PETNET_TENANT, v.view, version)
        console.log(`record.mapping=`, record.mapping)
      }
      reply.push(record)
    }
    console.log(`reply=`, reply)
    res.send(reply)
    next()
  })//

  // // Get services for a provider
  // server.get(`${constants.URL_PREFIX}/metadata/views/:providerCode/:serviceCode`, async function(req, res, next) {
  //   const providerCode = req.params.providerCode
  //   const serviceCode = req.params.serviceCode
  //   const withFields = req.params.fields
  //   const withMappings = req.params.mappings
  //   // console.log(`req.params=`, req.params)
  //   // console.log(`withFields=`, withFields)

  //   console.log(`---------------------------------`)
  //   console.log(`/metadata/views/${providerCode}/${serviceCode}`)

  //   // Check the provider exists
  //   const provider = await providers.get(providerCode)
  //   // console.log(`provider=`, provider)
  //   if (!provider) {
  //     res.send(new errors.NotFoundError(`Unknown provider ${providerCode}`))
  //     return next()
  //   }

  //   // Find the views for this provider / service
  //   const viewPattern = `${providerCode}-${serviceCode}-%`
  //   const views = await parameters.getViews(constants.PETNET_TENANT, viewPattern)
  //   console.log(`views=`, views)
  //   const reply = [ ]
  //   for (const v of views) {
  //     const viewSuffix = v.view.substring(viewPattern.length - 1)
  //     const record = {
  //       provider: providerCode,
  //       service: serviceCode,
  //       message: viewSuffix,
  //       view: v.view,
  //       description: v.description,
  //     }
  //     const version = -1 //ZZZZZZ Version should be specified
  //     if (withFields === 'true') {
  //       //ZZZZ Should use schema
  //       record.fields = await parameters.getFields(constants.PETNET_TENANT, v.view, version)
  //       // console.log(`record.fields=`, record.fields)
  //     }
  //     if (withMappings === 'true') {
  //       //ZZZZ Should use schema
  //       record.mapping = await parameters.getMapping(constants.PETNET_TENANT, v.view, version)
  //       console.log(`record.mapping=`, record.mapping)
  //     }
  //     reply.push(record)
  //   }
  //   console.log(`reply=`, reply)
  //   res.send(reply)
  //   next()
  // })//

}
