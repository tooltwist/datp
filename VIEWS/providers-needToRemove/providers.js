// import query from '../lib/query'
// import constants from '../lib/constants'

import RemittanceProviderUssc from './provider-ussc'
import RemittanceProviderCebuana from './provider-cebuana'
import RemittanceProviderBpi from './provider-bpi'
import RemittanceProviderWu from './provider-wu'
import query from '../../database/query'

const plugins = { } // providerCode -> plugin

const providers = [
  // {
  //   code: 'bpi',
  //   description: 'Bank of the Philippine Islands',
  //   plugin: new RemittanceProviderBpi(),
  // },
  // {
  //   code: 'cebuana',
  //   description: 'Cebuana',
  //   plugin: new RemittanceProviderCebuana(),
  // },
  // {
  //   code: 'iremit',
  //   description: 'iRemit'
  // },
  // {
  //   code: 'landbank',
  //   description: 'Landbank'
  // },
  // {
  //   code: 'metrobank',
  //   description: 'Metrobank'
  // },
  // {
  //   code: 'ria',
  //   description: 'RIA'
  // },
  // {
  //   code: 'smart',
  //   description: 'Smart Pedala'
  // },
  // {
  //   code: 'transfast',
  //   description: 'Transfast'
  // },
  // {
  //   code: 'ussc',
  //   description: 'USSC',
  //   plugin: new RemittanceProviderUssc(),
  // },
  // {
  //   code: 'xpress',
  //   description: 'Xpress Money'
  // },
  // {
  //   code: 'wu',
  //   description: 'Western Union',
  //   plugin: new RemittanceProviderWu(),
  // },
]


export default {
  init,
  get,
  all,
}

async function init() {
  console.log(`providers.init()`)

  // Initialise the provider plugins
  registerPlugin(new RemittanceProviderUssc())
  registerPlugin(new RemittanceProviderCebuana())
  registerPlugin(new RemittanceProviderBpi())
  registerPlugin(new RemittanceProviderWu())

  loadProviders()
}

async function registerPlugin(plugin) {
  // console.log(`register`, plugin)
  try {
    const code = plugin.getProviderCode()
    // const name = plugin.getName()
    // const definition = { code, name, plugin }
    plugin.init()
    plugins[code] = plugin
    // providers.push(definition)
  } catch (e) {
    console.log(e)
    // console.log(`Error registering remittance provider plugin (${plugin.constructor.name}):\n`, e)
  }
}

async function loadProviders() {

  // Load providers from the database
  const sql = `SELECT provider_code, name, status FROM provider`
  const dbProviders = await query(sql)
  // console.log(`dbProviders=`, dbProviders)

  // Set plugins for the providers
  for (const p of dbProviders) {
    const plugin = plugins[p.provider_code]
    if (p.provider_code !== 'std' && !plugin && (p.status !== 'noplugin' && p.status !== 'prepare')) {
      // Set to 'noplugin' status
      console.log(`No plugin found for provider ${p.provider_code} - updating database status to 'noplugin'.`)
      await query(`UPDATE provider SET status='noplugin' WHERE provider_code=?`, [ p.provider_code ])
    } else {
      const provider = {
        code: p.provider_code,
        name: p.name,
        status: p.status,
        plugin
      }
      providers.push(provider)
    }
  }

  // Add any new plugins to the database
  for (const pluginName in plugins) {
    let found = false
    for (const dbp of dbProviders) {
      if (dbp.provider_code === pluginName) {
        found = true
        break
      }
    }
    if (!found) {
      console.log(`Adding new provider ${pluginName} to the database.`)
      await query(`INSERT INTO provider (provider_code, name, status) VALUES (?, ?, ?)`, [ pluginName, pluginName, 'prepare' ])
    }
  }

  // Sort the list of providers
  providers.sort((p1, p2) => {
    if (p1.code < p2.code) return -1
    if (p1.code > p2.code) return +1
    return 0
  })
  // console.log(`providers=`, providers)
}

async function all() {
  console.log(`providers/providers:all()`)
  //ZZZZ Should clone the list, without functions, etc.
  const list = [ ]
  for (const p of providers) {
    list.push({
      code: p.code,
      name: p.name,
      status: p.status
    })
  }
  return list
}

async function get(code) {
  // console.log(`providers/providers:get(${code})`)
  for (const provider of providers) {
    // console.log(`provider=`, provider)
    if (provider.code === code) {
      // console.log(`- found`)
      return provider
    }
  }
  return null
}
