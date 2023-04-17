/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors'
import constants from '../CONVERSION/lib/constants'
import Schema from '../CONVERSION/formservice/schema-parser/Schema'
import FieldProperties from '../CONVERSION/formservice/schema-parser/field_properties'
import formsAndFields from '../CONVERSION/lib/formsAndFields'
import { FORMSERVICE_URL_PREFIX } from '../CONVERSION/lib/constants'
import apiVersions from '../extras/apiVersions'
const { defineRoute, LOGIN_IGNORED } = apiVersions

export default {
  init,
}

async function init(server) {
  // console.log(`routes/formservice_yarp:init()`)

  // Return all currencies
  defineRoute(server, 'get', false, FORMSERVICE_URL_PREFIX, '/view/:viewName', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`-------------------------------------`)
      console.log(`/formservice/:version/view/:viewName`)

      const viewName = req.params.viewName
      const createIfNotFound = (req.query.createIfNotFound === 'true')

      const schema = new Schema()
      const tenant = constants.DEFAULT_TENANT
      await schema.loadSchemaFromDatabase(tenant)

        let index = schema.views.index[viewName]
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
        }
        const view = schema.views.list[index]
        // console.log(`view=`, view)

        // Sanitise the definition. We do not want to expose DB details.
        const reply = {
          name: view.name,
          label: view.label,
          fields: [ ],
          descriptionField: view.descriptionField,
          modes: view.modes,
          notes: view.notes,
          description: view.description,
        }
        for (const fieldName in view.fieldLookup) {
          const field = view.fieldLookup[fieldName]
          const fldRec = {
            name: fieldName,
            label: field.properties.label,
            type: field.properties.type,
            exampleValue: field.properties.exampleValue,
            defaultValue: field.properties.defaultValue,
            allowableValues: field.properties.allowableValues,
            sequence: field.properties.sequence,
            // isDescription: field.properties.isDescription,
            properties: { }
          }

          // Copy the standard field properties and modifiers
          for (let property in FieldProperties) {
            const name = FieldProperties[property]
            // console.log(`property ${property} from ${name}`)
            if (field.properties[name]) {
              fldRec.properties[name] = true
            }
          }

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

    }}
  ])//- /formservice/view/:viewname


  /**
   * Insert a new field
   */
   defineRoute(server, 'post', false, FORMSERVICE_URL_PREFIX, '/field/:viewName', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`-------------------------------------`)
      console.log(`POST /formservice/:version/field/:viewName`)

      const viewName = req.params.viewName
      const newField = req.body
      try {
        const schema = new Schema()
        const tenant = constants.DEFAULT_TENANT
        await schema.loadSchemaFromDatabase(tenant)

        // Get the field
        const view = schema.getView(viewName)
        if (!view) {
          return next(new errors.NotFoundError(`Unknown view ${viewName}`))
        }
        await view.addField(schema, newField)
      } catch (err) {
        console.log(`err=`, err)
        return next(err)
      }

      res.send({ status: 'ok' })
      return next()
    }}
  ])//- field update


  /**
   * Update an existing field
   */
   defineRoute(server, 'put', false, FORMSERVICE_URL_PREFIX, '/field/:viewName/:fieldName', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {

      const viewName = req.params.viewName
      const fieldName = req.params.fieldName
      console.log(`-------------------------------------`)
      console.log(`PUT /formservice/:version/field/${viewName}/${fieldName}`)
      console.log(`viewName=`, viewName)
      console.log(`fieldName=`, fieldName)
      console.log(`req.body=`, req.body)
      const newField = req.body

      try {
        const schema = new Schema()
        const tenant = constants.DEFAULT_TENANT
        await schema.loadSchemaFromDatabase(tenant)

        // Get the field
        const view = schema.getView(viewName)
        if (!view) {
          return next(new errors.NotFoundError(`Unknown view ${viewName}`))
        }
        const field = view.getField(fieldName)
        if (!field) {
          return next(new errors.NotFoundError(`Unknown field ${viewName}/${fieldName}`))
        }
        field.updateField(schema, newField)
      } catch (err) {
        console.log(`err=`, err)
        return next(err)
      }
      res.send({ status: 'ok' })
      return next()
    }}
  ])//- field update


  /**
   * Delete a field
   */
   defineRoute(server, 'del', false, FORMSERVICE_URL_PREFIX, '/field/:viewName/:fieldName', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`-------------------------------------`)
      console.log(`DELETE /formservice/:version/field/:viewName/:fieldName`)

      const viewName = req.params.viewName
      const fieldName = req.params.fieldName

      try {
        const schema = new Schema()
        const tenant = constants.DEFAULT_TENANT
        await schema.loadSchemaFromDatabase(tenant)

        // Get the field
        const view = schema.getView(viewName)
        if (!view) {
          return next(new errors.NotFoundError(`Unknown view ${viewName}`))
        }
        const field = view.getField(fieldName)
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
    }}
  ])// delete field


  /**
   * Get a mapping by it's ID.
   */
   defineRoute(server, 'get', false, FORMSERVICE_URL_PREFIX, '/mapping/:mappingId', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`-------------------------------------`)
      console.log(`/formservice/:version/mapping/:mappingId`)

      const mappingId = req.params.mappingId
      const version = -1
      const mapping = await formsAndFields.getMapping(constants.DEFAULT_TENANT, mappingId, version)
      res.send(mapping)
      return next()
    }}
  ])//- /formservice/mapping/:mappingId

  /**
   * Save the mapping to a field.
   * If source is '-' any existing mapping will be deleted.
   */
   defineRoute(server, 'post', false, FORMSERVICE_URL_PREFIX, '/mapping', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`-------------------------------------`)
      console.log(`/formservice/:version/mapping/:mappingId/:fieldname/:source`)

      const mapping = req.body
      console.log(`mapping=`, mapping)

      const mappingId = mapping.mappingId
      const version = -1
      const fieldName = mapping.fieldName
      const source = mapping.source
      const converter = mapping.converter ? mapping.converter : null
      await formsAndFields.setMapping(constants.DEFAULT_TENANT, mappingId, version, fieldName, source, converter)
      res.send({ status: 'ok' })
      return next()
    }}
  ])//- /formservice/mapping/:mappingId/:fieldname/:source

  /**
   * Update the description and notes for a view.
   */
   defineRoute(server, 'put', false, FORMSERVICE_URL_PREFIX, '/viewDetails', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`-------------------------------------`)
      console.log(`PUT /formservice/:version/view/details`)

      // Check the required parameters have been provided
      const view = req.body
      if (!view.name) {
        return next(new errors.BadRequestError('body must include view [name]'))
      }
      if (!view.version) {
        return next(new errors.BadRequestError('body must include [version]'))
      }
      if (!view.description && !view.notes) {
        return next(new errors.BadRequestError('body must include [description] or [notes]'))
      }
      view.tenant = constants.DEFAULT_TENANT

      // Update the database
      await formsAndFields.setViewDetails(view)

      res.send({ status: 'ok' })
      return next()
    }}
  ])//- /formservice/mapping/:mappingId/:fieldname/:source
}//- init
