/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors'
import dbupdate from '../../../database/dbupdate';
import query from '../../../database/query';

import View from './View'
const { ViewList } = require('./ViewList');
// const { View } = require('./View');

/*
 *  Schema object.
 */
export default class Schema {
  constructor() {
    // this.definitions3 = []
    this.views = new ViewList()
    this.definitionFrom = 'unknown'
    this.tenant = null
  }

  /**
   * Load a schema from the database.
   *
   * @param {String} tenant
   */
  async loadSchemaFromDatabase(tenant) {
    console.log(`loadSchemaFromDatabase()`)

    this.definitionFrom = 'mysql'
    this.tenant = tenant

    // const sourceIndex = this.definitions3.length
    // this.definitions3.push({
    //   source: 'mysql',
    //   tenant,
    // })

    let sql = `
      SELECT tenant, view, description, view_type, notes FROM formservice_view
      WHERE tenant=?`
    let params = [ tenant ]
    const views = await query(sql, params)
    // console.log(`db views=`, views)

    for (const viewRow of views) {
      // console.log(`\nviewRow=`, viewRow)

      var view = new View(viewRow.view);
      await view.parseFromDatabase(viewRow)
      this.views.add(view)
      // console.log(`view=`, view)

    }
  }

  /**
   * Load a schema from it's short form definition.
   *
   * @param {Object} schemaDef
   */
  async loadSchemaFromShortform(schemaDef) {
    this.definitionFrom = 'shortform',
    this.definition = schemaDef

    // const sourceIndex = this.definitions3.length
    // this.definitions3.push({
    //   source: 'shortform',
    //   definition: schemaDef
    // })

    // Keep track of views
    // this.viewLookup = { }; // name -> View
    // this.viewArray = [ ]; // [View]


    // Load the view definitions
    for (let property in schemaDef) {
      if (!schemaDef.hasOwnProperty(property)) continue;
      var viewName = property;
      var viewDef = schemaDef[viewName];

      var view = new View(viewName);
      await view.parseShortform(viewDef)

      // this.viewLookup[viewName] = view;
      // this.viewArray.push(view);

      this.views.add(view)
    }

    //PP
    // New references
    // console.log(`\nCONSOLIDATING:\n`);
    this.views.forEach((view) => {
      // console.log(`\nBEFORE ${view.name}:\n`);
      // view.referenceList.dump();

      view.broken |= view.referenceList.consolidate(this);

      // console.log(`\nCONSOLIDATED ${view.name}:\n`);
      // view.referenceList.dump();

      if (!view.broken) {
        view.broken |= view.referenceList.validate(this);
      }
    })
    // console.log(`\AFTER:\n`);
    // this.views.forEach((view) => {
    //   view.referenceList.dump();
    // })

    // console.error(`OUR VIEWS:`, this.views);

    // Check the references from one view to another.
    // console.log(`SKIPPINE OLD verifyReferences`);
    // this.views.forEach((view) => {
    //   view.verifyReferences(this.views);
    // })

    // See if we have any errors
    this.broken = false;
    this.views.forEach((view) => {
      this.broken |= view.broken;
    });
    if (this.broken) {
      console.log();
      console.log();
      console.log(`************************************************`);
      console.log(`*                                              *`);
      console.log(`*      SCHEMA ERRORS - PLEASE CHECK ABOVE      *`);
      console.log(`*                                              *`);
      console.log(`************************************************`);
      console.log();
      console.log();
    }
  }

  getView(name) {
    // console.log(`Schema.getView(${name})`);
    // console.log(`this.views=`, this.views);
    // return this.viewLookup[name] ? this.viewLookup[name] : null;
    const view = this.views.get(name)
    console.log(`view=`, view)
    return view
  }

  views() {
    // return this.viewArray;
    return this.views
  }

  // updateField(viewName, fieldName, newField) {
  //   console.log(`updateField(${viewName}, ${fieldName})`, newField)

  //   //ZZZZ Error if schema is not from database

  //   // Get the field
  //   console.log(`schema=`, this)
  //   const view = this.getView(viewName)
  //   console.log(`view=`, view)
  //   if (!view) {
  //     throw new errors.NotFoundError(`Unknown view ${viewName}`)
  //   }
  //   const field = view.getField(fieldName)
  //   console.log(`field=`, field)
  //   if (!field) {
  //     throw new errors.NotFoundError(`Unknown field ${viewName}/${fieldName}`)
  //   }

  //   // If we are changing the name, this is a bit more complicated
  //   if (newField.name && newField.name != field.name) {
  //     // Changing field name
  //     // 1. Check the name is not used
  //     // 2. Update field details
  //     // 3. Add a new field to the DB
  //     // 4. Update references in the DB
  //     // 5. Delete original field from the DB
  //   } else {
  //     // Simple update
  //     field.update(this, newField)
  //   }
  // }

  async createView(viewName) {
    if (this.definitionFrom !== 'mysql') {
      throw new errors.MethodNotAllowedError(`Static schema cannot add a view`);
    }

    // Add to our schema
    var view = new View(viewName);
    // await view.parseShortform(viewDef)
    this.views.add(view)

    // Add to the database
    const version = -1
    const viewType = 'message'
    const sql = `INSERT INTO formservice_view (tenant, version, view, view_type) VALUES (?,?,?,?)`
    const params = [ this.tenant, version, viewName, viewType ]
    const reply = await dbupdate(sql, params)
    // console.log(`reply=`, reply)
    return view
  }

  toString() {
    return '[Schema object]';
  }
}
