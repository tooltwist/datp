import errors from 'restify-errors'
// import query from '../lib/query'
import constants from '../CONVERSION/lib/constants'
// import providers from '../CONVERSION/providers/providers'
import countries from '../CONVERSION/lookup/countries'
// import formservice from '@tooltwist/formservice'
// import parser from '@tooltwist/formservice/schema-parser/index'
import parser from '../CONVERSION/formservice/schema-parser/index'
// const parse = require('@tooltwist/formservice/schema-parser/index')
// import schemaFile from '../formservice.config'
import Schema from '../CONVERSION/formservice/schema-parser/Schema'
import FieldProperties from '../CONVERSION/formservice/schema-parser/field_properties'
// import parameters from '../lib/parameters'
import formsAndFields from '../CONVERSION/lib/formsAndFields-dodgey'

export default {
  init,
}

async function init(server) {
  console.log(`routes/formservice_yarp:init()`)

  // Return all currencies
  server.get(`${constants.FORMSERVICE_URL_PREFIX}/view/:viewName`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/formservice/view/:viewName`)
    console.log(`req.query=`, req.query)
    console.log(`res.params=`, res.params)
    console.log(`req.body=`, req.body)


    const viewName = req.params.viewName
    console.log(`viewName=`, viewName)
    const createIfNotFound = (req.query.createIfNotFound === 'true')
    // console.log(`req.params.createIfNotFound=`, req.params.createIfNotFound)
    console.log(`createIfNotFound`, createIfNotFound)
    // console.log(`req.query=`, req.query)
    // console.log(`req.params=`, req.params)
    // console.log(`req=`, req)

    // formservice.getSchema()
    // let result = await getMappedView(context, 'thing', { })

    // console.log(`parser=`, parser)

    const schema = new Schema()
    // await schema.loadSchemaFromShortform(schemaFile.schema)
    const tenant = constants.PETNET_TENANT
    await schema.loadSchemaFromDatabase(tenant)

      // console.log(`---------------------------`)
      // console.log(`schema=`, schema)
      // console.log(`schema.views=`, schema.views)
      // console.log(`schema.views.list[1]=`, schema.views.list[1])
      // console.log(`Got schema`, schema.definition.schema)
      // console.log(`views=`, schema.views)
      let index = schema.views.index[viewName]
      // const index = schema.getView(viewName)
      // console.log(`index=`, index)
      if (index === undefined) {
        if (createIfNotFound) {
          // Create a new view and proceed
          console.log(`Creating a new view ${viewName}`)
          await schema.createView(viewName)
          index = schema.views.index[viewName]
        } else {
          console.log(`---------------------------------`)
          console.log(`schema=`, schema)
          console.log(`View ${viewName} not found`)
          return next(new errors.NotFoundError(`Unknown view ${viewName}`))
        }
      } else {
        console.log(`View found`)
      }
      const view = schema.views.list[index]
      // console.log(`view=`, view)
      // console.log(`view.fieldLookup.status=`, view.fieldLookup.status)
      // console.log(`view.referenceList.references=`, view.referenceList.references)
      // console.log(`view.fieldLookup=`, view.fieldLookup)

      // Sanitise the definition. We do not want to expose DB details.
      const reply = {
        name: view.name,
        label: view.label,
        fields: [ ],
        descriptionField: view.descriptionField,
        modes: view.modes,
      }
      for (const fieldName in view.fieldLookup) {
        const field = view.fieldLookup[fieldName]
        const fldRec = {
          name: fieldName,
          label: field.properties.label,
          type: field.properties.type,
          sequence: field.properties.sequence,
          // isDescription: field.properties.isDescription,
          properties: { }
        }
        // if (field.properties.type) {
        //   fldRec.type = field.properties.type
        // }
        // Copy the standard field properties and modifiers
        for (let property in FieldProperties) {
          const name = FieldProperties[property]
          // console.log(`property ${property} from ${name}`)
          if (field.properties[name]) {
            fldRec.properties[name] = true
          }
        }
        // console.log(`//////////// YIP YIP YARP END`)

        // if (field.properties.isPrimaryKey) {
        //   fldRec.is_primary_key = true
        // }
        // if (field.properties.isDescription) {
        //   fldRec.is_description = true
        // }
        // if (field.properties.isMandatory) {
        //   fldRec.is_mandatory = true
        // }
        // if (field.properties.isSearchable) {
        //   fldRec.is_searchable = true
        // }
        // if (view.isJsonView() && view.primaryKeyFields.includes(field.name)) {
        //   // Key field of JSON record
        //   fldRec.is_readonly = true
        // }
        if (field.reference) {
          fldRec.reference = field.reference
        }
        reply.fields.push(fldRec)
      }

      //ZZZZ  Need to copy modes

      if (view.referenceList) {
        reply.references = [ ]
        for (const reference of view.referenceList.references) {
          const newRef = {
            referenceType: reference.referenceType,
          }
          if (reference.targetAlias) {
            newRef.targetAlias = reference.targetAlias
          }
          newRef.targetView = reference.targetView
          newRef.fields = [ ]
          for (const field of reference.fields) {
            newRef.fields.push({
              field: field.field,
              targetFieldName: field.targetFieldName
            })
          }
          reply.references.push(newRef)
        }
      }

      console.log(`ALL DONE`)
      res.send(reply)
      return next()
    // })

  })//- /formservice/view/:viewname


  /**
   * Insert a new field
   */
  server.post(`${constants.FORMSERVICE_URL_PREFIX}/field/:viewName`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`POST /formservice/field/:viewName`)

    const viewName = req.params.viewName
    // console.log(`viewName=`, viewName)
    // console.log(`req.body=`, req.body)
    const newField = req.body

    try {
      const schema = new Schema()
      const tenant = constants.PETNET_TENANT
      await schema.loadSchemaFromDatabase(tenant)

      // schema.updateField(viewName, fieldName, req.body)

      // Get the field
      // console.log(`schema=`, schema)
      const view = schema.getView(viewName)
      // console.log(`view=`, view)
      if (!view) {
        return next(new errors.NotFoundError(`Unknown view ${viewName}`))
      }

      // console.log(`newField=`, newField)
      await view.addField(schema, newField)
    } catch (err) {
      console.log(`err=`, err)
      return next(err)
    }

    res.send({ status: 'ok' })
    return next()
  })//- field update


  /**
   * Update an existing field
   */
  server.put(`${constants.FORMSERVICE_URL_PREFIX}/field/:viewName/:fieldName`, async function (req, res, next) {

    const viewName = req.params.viewName
    const fieldName = req.params.fieldName
    console.log(`-------------------------------------`)
    console.log(`PUT /formservice/field/${viewName}/${fieldName}`)
    // console.log(`viewName=`, viewName)
    // console.log(`fieldName=`, fieldName)
    // console.log(`req.body=`, req.body)
    const newField = req.body

    try {
      const schema = new Schema()
      const tenant = constants.PETNET_TENANT
      await schema.loadSchemaFromDatabase(tenant)

      // schema.updateField(viewName, fieldName, req.body)

      // Get the field
      // console.log(`schema=`, schema)
      const view = schema.getView(viewName)
      // console.log(`view=`, view)
      if (!view) {
        return next(new errors.NotFoundError(`Unknown view ${viewName}`))
      }
      const field = view.getField(fieldName)
      // console.log(`field=`, field)
      if (!field) {
        return next(new errors.NotFoundError(`Unknown field ${viewName}/${fieldName}`))
      }
      field.update(schema, newField)
    } catch (err) {
      console.log(`err=`, err)
      return next(err)
    }

    res.send({ status: 'ok' })
    return next()
  })//- field update


  /**
   * Delete a field
   */
  server.del(`${constants.FORMSERVICE_URL_PREFIX}/field/:viewName/:fieldName`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`DELETE /formservice/field/:viewName/:fieldName`)

    const viewName = req.params.viewName
    const fieldName = req.params.fieldName
    // console.log(`viewName=`, viewName)
    // console.log(`fieldName=`, fieldName)

    try {
      const schema = new Schema()
      const tenant = constants.PETNET_TENANT
      await schema.loadSchemaFromDatabase(tenant)

      // Get the field
      // console.log(`schema=`, schema)
      const view = schema.getView(viewName)
      // console.log(`view=`, view)
      if (!view) {
        return next(new errors.NotFoundError(`Unknown view ${viewName}`))
      }
      const field = view.getField(fieldName)
      // console.log(`field=`, field)
      if (!field) {
        return next(new errors.NotFoundError(`Unknown field ${viewName}/${fieldName}`))
      }
      field.delete(schema)
    } catch (err) {
      console.log(`err=`, err)
      return next(err)
    }

    res.send({ status: 'ok' })
    return next()
  })// delete field


  /**
   * Get a mapping by it's ID.
   */
  server.get(`${constants.FORMSERVICE_URL_PREFIX}/mapping/:mappingId`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/formservice/mapping/:mappingId`)

    const mappingId = req.params.mappingId
    console.log(`mappingId=`, mappingId)
    const version = -1
    const mapping = await formsAndFields.getMapping(constants.PETNET_TENANT, mappingId, version)
    console.log(`mapping=`, mapping)
    res.send(mapping)
    return next()

    // formservice.getSchema()
    // let result = await getMappedView(context, 'thing', { })

    // console.log(`parser=`, parser)

    // const schema = new Schema()
    // // await schema.loadSchemaFromShortform(schemaFile.schema)
    // let tenant = 'petnet'
    // await schema.loadSchemaFromDatabase(tenant)

    //   console.log(`---------------------------`)
    //   // console.log(`schema=`, schema)
    //   // console.log(`schema.views=`, schema.views)
    //   // console.log(`schema.views.list[1]=`, schema.views.list[1])
    //   // console.log(`Got schema`, schema.definition.schema)
    //   // console.log(`views=`, schema.views)
    //   const index = schema.views.index[viewName]
    //   // console.log(`index=`, index)
    //   if (index === undefined) {
    //     return next(new errors.NotFoundError(`Unknown view ${viewName}`))
    //   }
    //   const view = schema.views.list[index]
    //   // console.log(`view=`, view)
    //   // console.log(`view.fieldLookup.status=`, view.fieldLookup.status)
    //   // console.log(`view.referenceList.references=`, view.referenceList.references)
    //   // console.log(`view.fieldLookup=`, view.fieldLookup)

    //   // Sanitise the definition. We do not want to expose DB details.
    //   const reply = {
    //     name: view.name,
    //     label: view.label,
    //     fields: [ ],
    //     descriptionField: view.descriptionField,
    //     modes: view.modes,
    //   }
    //   for (const fieldName in view.fieldLookup) {
    //     const field = view.fieldLookup[fieldName]
    //     const fldRec = {
    //       name: fieldName,
    //       label: field.properties.label,
    //       type: field.properties.type,
    //       sequence: field.properties.sequence,
    //       // isDescription: field.properties.isDescription,
    //       properties: { }
    //     }
    //     // if (field.properties.type) {
    //     //   fldRec.type = field.properties.type
    //     // }
    //     // Copy the standard field properties and modifiers
    //     for (let property in FieldProperties) {
    //       const name = FieldProperties[property]
    //       // console.log(`property ${property} from ${name}`)
    //       if (field.properties[name]) {
    //         fldRec.properties[name] = true
    //       }
    //     }
    //     // console.log(`//////////// YIP YIP YARP END`)

    //     // if (field.properties.isPrimaryKey) {
    //     //   fldRec.is_primary_key = true
    //     // }
    //     // if (field.properties.isDescription) {
    //     //   fldRec.is_description = true
    //     // }
    //     // if (field.properties.isMandatory) {
    //     //   fldRec.is_mandatory = true
    //     // }
    //     // if (field.properties.isSearchable) {
    //     //   fldRec.is_searchable = true
    //     // }
    //     // if (view.isJsonView() && view.primaryKeyFields.includes(field.name)) {
    //     //   // Key field of JSON record
    //     //   fldRec.is_readonly = true
    //     // }
    //     if (field.reference) {
    //       fldRec.reference = field.reference
    //     }
    //     reply.fields.push(fldRec)
    //   }

    //   //ZZZZ  Need to copy modes

    //   if (view.referenceList) {
    //     reply.references = [ ]
    //     for (const reference of view.referenceList.references) {
    //       const newRef = {
    //         referenceType: reference.referenceType,
    //       }
    //       if (reference.targetAlias) {
    //         newRef.targetAlias = reference.targetAlias
    //       }
    //       newRef.targetView = reference.targetView
    //       newRef.fields = [ ]
    //       for (const field of reference.fields) {
    //         newRef.fields.push({
    //           field: field.field,
    //           targetFieldName: field.targetFieldName
    //         })
    //       }
    //       reply.references.push(newRef)
    //     }
    //   }

      // console.log(`ALL DONE`)
      // res.send(reply)
      // return next()
    // })

  })//- /formservice/mapping/:mappingId

  /**
   * Save the mapping to a field.
   * If source is '-' any existing mapping will be deleted.
   */
  //  server.post(`${constants.FORMSERVICE_URL_PREFIX}/mapping/:mappingId/:fieldName/:source`, async function (req, res, next) {
  server.post(`${constants.FORMSERVICE_URL_PREFIX}/mapping`, async function (req, res, next) {
    console.log(`-------------------------------------`)
    console.log(`/formservice/mapping/:mappingId/:fieldname/:source`)

    const mapping = req.body
    console.log(`mapping=`, mapping)

    // /:mappingId/:fieldName/:source

    const mappingId = mapping.mappingId
    // console.log(`mappingId=`, mappingId)
    const version = -1
    const fieldName = mapping.fieldName
    const source = mapping.source
    const converter = mapping.converter ? mapping.converter : null
    await formsAndFields.setMapping(constants.PETNET_TENANT, mappingId, version, fieldName, source, converter)
    res.send({ status: 'ok' })
    return next()
  })//- /formservice/mapping/:mappingId/:fieldname/:source

}//- init
