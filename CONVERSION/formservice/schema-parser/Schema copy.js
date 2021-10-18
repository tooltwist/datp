import query from '../../lib/query';

import View from './View'
const { ViewList } = require('./ViewList');
// const { View } = require('./View');

/*
 *  Schema object.
 */
export default class Schema {
  constructor() {
    this.definitions3 = []
    this.views = new ViewList()
  }

  /**
   * Load a schema from the database.
   *
   * @param {String} tenant
   */
  async loadSchemaFromDatabase(tenant) {
    // console.log(`loadSchemaFromDatabase()`)

    let sql = `
      SELECT view, description, view_type FROM formservice_view
      WHERE tenant=?`
    let params = [ tenant ]
    const views = await query(sql, params)
    // console.log(`db views=`, views)

    for (const view of views) {
      console.log(`\nview=`, view)
      const version = -1
      sql = `
        SELECT sequence, name, type, mandatory FROM formservice_field
        WHERE tenant=? AND view=? AND version=?
        ORDER BY sequence`
      params = [ tenant, view.view, version ]
      const fields = await query(sql, params)
      console.log(`db fields for ${view.view}=`, fields)
    }
  }

  /**
   * Load a schema from it's short form definition.
   *
   * @param {Object} schemaDef
   */
  async loadSchemaFromShortform(schemaDef) {
    this.definitions3.push({
      source: 'shortform',
      definition: schemaDef
    })

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
    return this.views.get(name)
  }

  views() {
    // return this.viewArray;
    return this.views
  }

  toString() {
    return '[Schema object]';
  }
}
