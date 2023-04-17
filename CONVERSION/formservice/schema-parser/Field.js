/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
const humanize = require("underscore.string/humanize")
import FIELD_PROPERTIES  from './field_properties'
import errors from 'restify-errors'
// import field_properties from './field_properties';
import Schema from './Schema';
import query from '../../../database/query';
import dbupdate from '../../../database/dbupdate';


/*
 *  Field object.
 */
export default class Field {

  constructor(fieldName) {
    //console.log('new Field(' + fieldName + ')', fieldDef);
    this.name = fieldName;
  }

  async parseFromDatabase(view, row) {
    // if (row.name === 'method') {
    //   console.trace('field.parseFromDatabase(' + this.name + ')', row);
    // }
    this.viewName = view.name;
    // this.definition = fieldDef;
    this.properties = {
      name: this.name,
      column: this.name, // may get overridden
      sequence: row.sequence,
      type: row.type,
      label: row.label,
      exampleValue: row.example_value,
      defaultValue: row.default_value,
      allowableValues: row.allowable_values,
    };

    if (!this.properties.label) {
      this.properties.label = createLabel(this.name)  // may get overridden
    }

    if (row.type) {
      this.properties.type = row.type
    }
    if (row.is_primary_key) {
      this.properties.isPrimaryKey = true
    }
    if (view.isMysqlView()) {
      this.columnName = row.column_name
    }
    if (view.isJsonView() && row.is_index) {
      this.properties.isIndex = true
    }
    if (row.is_description) {
      this.properties.isDescription = true
    }
    if (row.is_mandatory) {
      this.properties.isMandatory = true
    }
    if (row.is_searchble) {
      this.properties.isSearchable = true
    }
    if (row.modifiers) {
      this.expandFieldDefinitionString(view, this, row.modifiers);
    }

    // if (row.modes) {
    //   // const arr =
    // }

    // if (fieldDef == null) {

    //   // Just use the default field definition set above.
    // } else if (typeof(fieldDef) == 'string') {
    //   // This is a shortcut field definition.
    //   this.expandFieldDefinitionString(view, this, fieldDef);
    // } else if (typeof(fieldDef) == 'object') {

    //   // Long hand field definition.
    //   copyFieldProperties(view, this, fieldDef, this.properties);

    // } else {
    //   // Unknown field type
    //   //ZZZZZ
    //   console.log('SCHEMA ERROR: invalid definition for ' + view.getName() + '.' + this.name + '.');
    //   this.broken = true;
    // }
  }

  async parseShortform(view, fieldDef) {
    // console.log('field.parseShortform(' + this.name + ')', fieldDef);
    this.viewName = view.name;
    this.definition = fieldDef;
    this.properties = {
      name: this.name,
      column: this.name, // may get overridden
      label: createLabel(this.name),  // may get overridden
    };

    if (fieldDef == null) {

      // Just use the default field definition set above.
    } else if (typeof(fieldDef) == 'string') {
      // This is a shortcut field definition.
      this.expandFieldDefinitionString(view, this, fieldDef);
    } else if (typeof(fieldDef) == 'object') {

      // Long hand field definition.
      copyFieldProperties(view, this, fieldDef, this.properties);

    } else {
      // Unknown field type
      //ZZZZZ
      console.log('SCHEMA ERROR: invalid definition for ' + view.getName() + '.' + this.name + '.');
      this.broken = true;
    }
  }

  toString () {
    return '[Field ' + this.name + ']';
  }

  getName () {
    return this.name;
  }

  fullname () {
    return '[Field:' + this.viewName + '.' + this.name + ']';
  }

  getColumnName () {
    return this.properties.column;
  }

  isPrimaryKey () {
    return this.properties.isPrimaryKey;
  }

  isIndex () {
    return this.properties.isIndex;
  }

  isDescription () {
    return this.properties.isDescription;
  }

  getReference () {
    // Returns { view: String, field: String }
    return this.reference;
  }

  expandFieldDefinitionString (view, field, fieldDef) {
    console.log(`expandFieldDefinitionString(${fieldDef})`);

    //var schema = field.view.schema;

    // Create the default field definition object.
    // var newDef = {
    //   name: field.name,
    //   column: field.name
    // };

    // console.log('++++++++++++++++++++> DEF: ' +  fieldDef);

    // String definition of the field
    var arr = fieldDef.split(',');
    for (var i = 0; i < arr.length; i++) {
      var val = arr[i].trim();

      if (val.startsWith('*')) {

        //PP New stuff
        view.addReference(this.name, val)

        // References another view/table
        //    field: *view
        //    field: *view.field
        field.reference = parseReference(val); // { view: String, field: String | null }
        // console.log(`${field.name} -> `, field.reference);


        // var referencedView = schema.getView[referencedViewName];
        // if (referencedView) {
        //   field.referencedView = referencedView;
        // } else {
        //   // Reference to Unknown view
        //   console.log('SCHEMA ERROR: Invalid reference from ' + field.fullname() + ' to view ' + referencedViewName + '.');
        //   field.broken = true;
        // }
      } else if (FIELD_PROPERTIES[val]) {

        // Set the field property
        var propertyName = FIELD_PROPERTIES[val];
        //console.log(field.fullname() + '.' + propertyName + ' = true');
        field.properties[propertyName] = true;
      } else {

        // No idea what to do with this field definition.
        console.log('SCHEMA ERROR: Unknown field qualifier for ' + field.fullname() + ': \'' + val + '\'');
        field.broken = true;
        break;
      }
    }
  }

  /**
   * Update this field with the provided values.
   * @param {Object} newField Object containing values to be updated.
   * Should be similar to a Field object, but values are optional.
   */
  async updateField(schema, newField) {
    console.log(`Field.updateField()`, newField)

    // Can only change if the definition came from the database
    if (schema.definitionFrom !== 'mysql') {
      throw new errors.NotImplementedError(`Schema cannot be updated`)
    }

    if (this.name !== newField.name) {
      console.log(`CHANGING NAME!!!`)
    } else {
      // Simple update
      // await updateField(schema, this, newField)
      let sql = `
        SELECT * FROM formservice_field
        WHERE tenant=? AND view=? AND name=?`
      let params = [ schema.tenant, this.viewName, this.name]
      // console.log(`sql=`, sql)
      // console.log(`params=`, params)
      const result = await query(sql, params)
      // console.log(`result=`, result)
      if (result.length < 1) {
        throw new errors.NotFoundError(`Internal error: field ${this.viewName}.${this.name} not found in the database`)
      }
      const field = result[0]

      sql = `UPDATE formservice_field SET`
      params = [ ]
      let sep = ''
      if (typeof(newField.sequence) !== 'undefined' && newField.sequence !== field.sequence) {
        sql += `${sep} sequence=?`
        params.push(newField.sequence)
        sep = ','
      }
      if (typeof(newField.type) !== 'undefined' && newField.type !== field.type) {
        console.log(`====> ${newField.type} vs ${field.type}`)
        sql += `${sep} type=?`
        params.push(newField.type)
        sep = ','
      }
      if (typeof(newField.label) !== 'undefined' && newField.label !== field.label) {
        sql += `${sep} label=?`
        params.push(newField.label)
        sep = ','
      }
      if (typeof(newField.defaultValue) !== 'undefined' && newField.defaultValue !== field.defaultValue) {
        sql += `${sep} default_value=?`
        params.push(newField.defaultValue)
        sep = ','
      }
      if (typeof(newField.exampleValue) !== 'undefined' && newField.exampleValue !== field.exampleValue) {
        sql += `${sep} example_value=?`
        params.push(newField.exampleValue)
        sep = ','
      }
      if (typeof(newField.allowableValues) !== 'undefined' && newField.allowableValues !== field.allowableValues) {
        sql += `${sep} allowable_values=?`
        params.push(newField.allowableValues)
        sep = ','
      }
      if (typeof(newField.columnName) !== 'undefined' && newField.columnName !== field.columnName) {
        sql += `${sep} column_name=?`
        params.push(newField.columnName)
        sep = ','
      }
      if (newField.properties) {
        if (typeof(newField.properties.isPrimaryKey) !== 'undefined' && (!!newField.properties.isPrimaryKey !== !!field.isPrimaryKey)) {
          sql += `${sep} is_primary_key=?`
          params.push(newField.properties.isPrimaryKey ? 1 : 0)
          sep = ','
        }
        if (typeof(newField.properties.isIndex) !== 'undefined' && (!!newField.properties.isIndex !== !!field.isIndex)) {
          sql += `${sep} is_index=?`
          params.push(newField.properties.isIndex ? 1 : 0)
          sep = ','
        }
        if (typeof(newField.properties.isDescription) !== 'undefined' && (!!newField.properties.isDescription !== !!field.isDescription)) {
          sql += `${sep} is_description=?`
          params.push(newField.properties.isDescription ? 1 : 0)
          sep = ','
        }
        if (typeof(newField.properties.isMandatory) !== 'undefined' && (!!newField.properties.isMandatory !== !!field.isMandatory)) {
          sql += `${sep} is_mandatory=?`
          params.push(newField.properties.isMandatory ? 1 : 0)
          sep = ','
        }
        if (typeof(newField.properties.isSearchable) !== 'undefined' && (!!newField.properties.isSearchable !== !!field.isSearchable)) {
          sql += `${sep} is_searchable=?`
          params.push(newField.properties.isSearchable ? 1 : 0)
          sep = ','
        }
      }
      if (typeof(newField.modifiers) !== 'undefined' && newField.modifiers !== field.modifiers) {
        sql += `${sep} modifiers=?`
        params.push(newField.modifiers ? 1 : 0)
        sep = ','
      }
      if (typeof(newField.modes) !== 'undefined' && newField.modes !== field.modes) {
        sql += `${sep} modes=?`
        params.push(newField.modes)
        sep = ','
      }
      sql += ` WHERE tenant=? AND view=? AND name=?`
      params.push(schema.tenant)
      params.push(this.viewName)
      params.push(this.name)

      console.log(`sql=`, sql)
      console.log(`params=`, params)
      if (sep) {
        // console.log(`Update ${schema.tenant}/${this.viewName}/${this.name}`)
        await dbupdate(sql, params)
      } else {
        // console.log(`no changes`)
      }
    }
  }


  /**
   * Delete this field.
   * @param {Schema} schema
   */
  async delete(schema) {
    console.log(`Field.delete()`)
    const sql = `
      DELETE FROM formservice_field WHERE tenant=? AND view=? AND name=?`
    let params = [ schema.tenant, this.viewName, this.name]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await query(sql, params)
    // console.log(`result=`, result)
}



}//- END OF CLASS

function copyFieldProperties(view, field, fromDef, to) {
  //console.log('copyFieldProperties()', from, to);

  // Copy the non-boolean fields
  if (fromDef.column) {
    to.column = fromDef.column;
  }
  if (fromDef.label) {
    to.label = fromDef.label;
  }

  // Copy the other values
  for (definitionName in FIELD_PROPERTIES) {
    let propertyName = FIELD_PROPERTIES[definitionName]

    if (typeof(fromDef[definitionName]) != 'undefined') {
      if (fromDef[definitionName]) {
        to[propertyName] = true
      } else {
        to[propertyName] = false
      }
    }
  }
  //console.log('After copying:', to);

  // Look after references
  if (fromDef.reference) {

    // console.log(`warp 1 ${field.name}, fromDef.reference`);
    view.addReference(field.name, fromDef.reference)

    // References another view/table
    //    field: *view
    //    field: *view.field
    field.reference = parseReference(fromDef.reference); // { view: String, field: String | null }
  }
}

// Fields that reference view have several forms:
// 	fieldname: *<viewname>
// 	fieldname: *<viewname>.<fieldname>
//
//  Returns { view: "<name>"|"<name>{<alias>}", field: String | null }
//  These will be resolved further later.
function parseReference(val) {
  //console.log(`parseReference(${val})`);

  if (val.substring(0, 1) === '*') {
    val = val.substring(1)
  }
  let referencedViewName = val;
  let referencedFieldName = null;

/*FROM_A3_BROKEN
*   The following was found in a3.broken. Should it be included here?
*
ZZ  // Default is to use the reference as a view name, referring
ZZ  // to the same field name in the referenced view.
ZZ  let referencedViewName = val;
ZZ  let referencedFieldName = defaultFieldName;
*
*/

  // See if we have viewname.fieldname
  let pos = referencedViewName.indexOf('.')
  if (pos > 0) {
    referencedFieldName = referencedViewName.substring(pos + 1)
    referencedViewName = referencedViewName.substring(0, pos)
    // console.log(`  -> ${referencedViewName}.${referencedFieldName}`)
  } else {
    // console.log(`  -> ${referencedViewName} PRIMARY_KEY`)
  }

  return { view: referencedViewName, field: referencedFieldName };
}

function createLabel(name) {
  const pos = name.lastIndexOf('.')
  if (pos >= 0) {
    name = name.substring(pos + 1)
  }
  return humanize(name)
}
