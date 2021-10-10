const errors = require('restify-errors')

// this.useDatabase = function(_connection) {
// 	connection = _connection;
// }

// var formservice_select = require('./formservice_select');
// var formservice_insert = require('./formservice_insert');
// var formservice_update = require('./formservice_update');

const { Schema } = require('./Schema');
// const { ViewList } = require('./ViewList');
const { View } = require('./View');
const { Field } = require('./Field');
//
// class ViewList {
//   constructor ( ) {
//     this.index = { }; // name -> number (position in list)
//     this.list = [ ]; // [View]
//   }
//
//   add ( view ) {
//     this.index[view.name] = this.list.length
//     this.list.push(view)
//   }
//
//   get (name) {
//     // console.log(`viewlist.get(${name})`);
//     if (this.index[name] !== undefined) {
//       let position = this.index[name]
//       // console.log(`position is ${position}`);
//       if (position < 0 || position >= this.list.length) {
//         console.error(`Viewlist: Internal error; bad index value (View=${name}, position=${position})`);
//         return null
//       }
//       return this.list[position]
//     }
//     // unknown view
//     return null
//   }
//
//   forEach (fn) {
//     this.list.forEach(fn)
//   }
// }



module.exports.parseSchema = function (schemaDef, callback) {
  // console.log(`parseSchema()`, schemaDef);
  let schema = new Schema(schemaDef)
  return callback(null, schema)
}


module.exports.init = function init(server, urlPrefix, schemaDef) {
  console.log('TooltwistViews.init(' + urlPrefix + ')');
  console.log('TooltwistViews: loading schema...');
  console.log();


  var schema = new Schema(schemaDef);
  //console.log('schema=', schema);

  // if (schema.broken) {
  //   // We could not parse a field definition.
  //   console.log('SCHEMA ERROR: CANNOT PROCEED.');
  //   var err = new errors.ServiceUnavailableError('Schema definition error');
  //   // return callback(err);
  //   return;
  // }


  /*
  // Debug stuff
  var view = schema.getView('product');
  console.log('view:', view);
  console.log('view: ' + view);


  var field = view.getField('product_id');
  console.log('field:', field);
  console.log('field:' + field);

  var tableName = view.getTableName();
  console.log('tableName:', tableName);

  var fields = view.getPrimaryKeyFields();
  console.log('primaryKeyFields:', fields);
*/

/*
  var field = view.getDescriptionField();
  console.log('descriptionField:', field);
  */

  // Initialise the routes for each view.
  // console.log('TooltwistViews: adding routes...');
  // schema.views().forEach(function(view, i) {
  //
  //   // for (property in Schema.views()) {
  //   //   var viewName = property;
  //   //   var view = Schema.entityDef(viewName);
  //
  //   var viewName = view.getName();
  //   //console.log('view=' + view);
  //
  //
  //   // Wrapping this in a function prevents the viewName variable from changing.
  //   (function(schema, view) {
  //
  //       /*
  //        *  Route for select request for this view.
  //        */
  //       var path = urlPrefix + '/1.0/' + view.getName() + '/select';
  //       console.log('  /' + path);
  //       server.post({ path: path, version: "3.0.0" }, function(req, res, next) {
  //
  //         console.log('**** SELECTING');
  //         formservice_select.select(req, res, server, schema, view, next);
  //       });// post
  //
  //       /*
  //        *  Route for save request for this view.
  //        */
  //       var path = urlPrefix + '/1.0/' + view.getName() + '/save';
  //       console.log('  /' + path);
  //       server.post({ path: path, version: "3.0.0" }, function(req, res, next) {
  //         console.log('  called /' + path);
  //
  //         //
  //         //  See if a primary key has been specified.
  //         //  If it is a multi-column primary key, the query must either:
  //         //    1. Supply ALL primary keys, and we'll do an update, or
  //         //    2. Supply NONE of the primary keys, and we'll do an insert.
  //         //
  //         let primaryKeyFields = view.getPrimaryKeyFields();
  //         let primaryKeySpecified = false;
  //         let haveAllPrimaryKeys = true;
  //
  //         primaryKeyFields.forEach((primaryKeyField) => {
  //           let fieldName = primaryKeyField.getName()
  //           console.log('oooo checking primary key field ' + fieldName);
  //           if (typeof(req.params[fieldName]) == 'undefined') {
  //             // This primary key is not specified in the query.
  //             haveAllPrimaryKeys = false;
  //             console.log(`oooo primary key NOT found`);
  //           } else {
  //             // Have this primary key specified
  //             primaryKeySpecified = true
  //             console.log(`oooo primary key IS found`);
  //           }
  //         });
  //         console.log('oooo primaryKeySpecified=${primaryKeySpecified}, haveAllPrimaryKeys=${haveAllPrimaryKeys}');
  //
  //         // if (primaryKeySpecified && !haveAllPrimaryKeys) {
  //         //   let msg = `Saving ${tableName}: not all primary keys were specified`
  //         //   console.log(msg);
  //         //   let err = new errors.NotFoundError(msg);
  //         //   return callback(err);
  //         // }
  //
  //         if (primaryKeySpecified && haveAllPrimaryKeys) {
  //
  //           // We have the full primary key, so update.
  //           formservice_update.update(req, res, server, schema, view, (err) => {
  //             console.log(`\n\n\nRETURN FROM UPDATE IS `, err);
  //
  //             // If no record was found matching this key. Insert a new record.
  //             if (err && err.body.code === 'NotFound') {
  //               console.log('Update found no record to update, so will insert a new record.');
  //               formservice_insert.insert(req, res, server, schema, view, next);
  //               return;
  //             }
  //             return next(err);
  //           });
  //         } else {
  //
  //           // No primary key supplied, so insert.
  //
  //           console.log('\n\n\nINSERT\n\n\n', req.params);
  //           formservice_insert.insert(req, res, server, schema, view, next);
  //         }
  //       });// save
  //
  //       /*
  //        *  Route for insert request for this view.
  //        */
  //       var path = urlPrefix + '/1.0/' + view.getName() + '/insert';
  //       console.log('  /' + path);
  //       server.post({ path: path, version: "3.0.0" }, function(req, res, next) {
  //         console.log('  called /' + path);
  //         formservice_insert.insert(req, res, server, schema, view, next);
  //       });// insert
  //
  //       /*
  //        *  Route for update request for this view.
  //        */
  //       var path = urlPrefix + '/1.0/' + view.getName() + '/update';
  //       console.log('  /' + path);
  //       server.post({ path: path, version: "3.0.0" }, function(req, res, next) {
  //         console.log('  called /' + path);
  //         formservice_update.update(req, res, server, schema, view, next);
  //       });// update
  //
  //       console.log();
  //     })(schema, view);
  //
  // });// next view







//  });// loadSchema
}// exports.init()






//
// function loadSchemaZZZ(loadedSchema, callback/*(err, schema)*/) {
//
//   //
//   var schema = loadedSchema;
//
//   //ZZZZZ
//   // if (SchemaObj._isLoaded) {
//   //   return callback(null, SchemaObj);
//   // }
//   // if (SchemaObj._isBroken) {
//   //   // We could not parse a field definition.
//   //   var err = new errors.ServiceUnavailableError('Already loaded broken schema');
//   //   return callback(err);
//   // }
//
//   /*
//    *  Verify the schema and patch the schema definition structures with
//    *  shortcut information, so we don't need to dig around too much later.
//    *  - entity and field names.
//    *  - primary key and description fields for entities.
//    *  - links to references tables.
//    */
//
//   // Check each entity definition
//   // (Entities are defined as properties, so we iterate over the properties)
//   // http://stackoverflow.com/questions/8312459/iterate-through-object-properties
//   var isBroken = false;
//   for (var property in schema) {
//     if (!schema.hasOwnProperty(property)) continue;
//
//     // Check this entity definition
//     var entityName = property;
//     var entity = schema[entityName];
//     entity.name = entityName;
//     entity.primaryKeyFields = [ ]; // -> field definition
//     entity.descriptionField = null; // -> field definition
//
//     // Set any table name where default is used.
//     if (!entity.table) {
//       entity.table = entityName;
//     }
//
//     // Expand each field definition to include defaults.
//     // Also look for the primary key and description for this entity.
//     // (Fields are defined as properties of the 'fields' property).
//     // http://stackoverflow.com/questions/8312459/iterate-through-object-properties
//     var fields = entity.fields;
//     entity.fieldList = [ ];
//     for (property2 in fields) {
//       if (!fields.hasOwnProperty(property2)) continue;
//
//       // Expand this field
//       var fieldName = property2;
//       var field = fields[fieldName];
//       var expandedDef = expandFieldDef(schema, entityName, fieldName, field);
//       if (expandedDef.broken) {
//         isBroken = true;
//       }
//
//       // Update the schema definition for this field.
//       fields[fieldName] = expandedDef;
//       if (expandedDef.isPrimaryKey) {
//         entity.primaryKeyFields.push(expandedDef);
//       }
//       if (expandedDef.isIndex) {
//         entity.indexFields.push(expandedDef);
//       }
//       if (expandedDef.isDescription) {
//         entity.descriptionField = expandedDef;
//       }
//
//       // Add the field to our list.
//       entity.fieldList.push(expandedDef);
//     }// fields
//   }// entities
//
//   if (isBroken) {
//
//     //ZZZZZ
//     SchemaObj._isBroken = true;
//     // We could not parse a field definition.
//     let msg = 'Schema definition error'
//     console.log(msg);
//     var err = new errors.ServiceUnavailableError(msg);
//     return callback(err);
//   }
//
//
//   // Verify the fields in any mode definitions (and trim out spaces).
//
//
//
//   // Create an object providing the functionality of the Schema object.
//   SchemaObj = {
//
//     views: function() {
//       return schema;
//     },
//
//     entityDef: function(viewName) {
//       return schema[viewName];
//     },
//
//     table: function(entity) {
//       var record = schema[entity];
//       if (record) {
//         if (record.table) {
//           return record.table;
//         }
//         return entity;
//       }
//       return null;
//     },
//
//     primaryKeyFields: function(entityName) {
//       var record = schema[entityName];
//       if (record && record.primaryKeyFields) {
//         return record.primaryKeyFields;
//       }
//       return null;
//     },
//
//     descriptionField: function(entityName) {
//       var record = schema[entityName];
//       if (record && record.descriptionField) {
//         return record.descriptionField;
//       }
//       return null;
//     },
//
//     field: function(entityName, fieldName) {
//       var record = schema[entityName];
//       if (record && record.fields) {
//         var field = record.fields[fieldName];
//         if (field) {
//           return field;
//         }
//       }
//       return null;
//     },
//
//     fields: function(entityName) {
//       var record = schema[entityName];
//       if (record) {
//         return record.fieldList;
//       }
//       return null;
//     },
//
//     nocomma: null
//   };
//
//   //ZZZZZ
//   SchemaObj._isLoaded = true;
//   return callback(null, SchemaObj);
// }


// function expandFieldDefZZZZ(schema, entityName, fieldName, def) {
//
//   // Create the default field definition object.
//   var newDef = {
//     name: fieldName,
//     column: fieldName
//   };
//
//   // Parse the schema definition for the field.
//   if (def == null) {
//
//     // Use the default field definition.
//   }
//   else if (typeof(def) == 'object') {
//
//     // Already expanded, but check a few properties.
//     if (!def.name) {
//       def.name = fieldName;
//     }
//     if (!def.column) {
//       def.column = fieldName;
//     }
//     newDef = def;
//   } else if (typeof(def) == 'string') {
//
//     // String definition of the field
//     var arr = def.split(',');
//     for (var i = 0; i < arr.length; i++) {
//       var val = arr[i].trim();
//       if (val == 'primary') {
//
//         // This is the primary key for the entity
//         console.log(entityName + '.' + fieldName + ' is primary key');
//         newDef.isPrimaryKey = true;
//       }
//       else if (val == 'index') {
//
//         // This field will be indexed (only used for 'json' storageType)
//         console.log(entityName + '.' + fieldName + ' is index');
//         newDef.isIndex = true;
//       }
//       else if (val == 'description') {
//
//         // Use this as the description for the entity
//         console.log(entityName + '.' + fieldName + ' is description');
//         newDef.isDescription = true;
//       }
//       else if (val == 'searchable') {
//
//         // This field may be used for searching
//         console.log(entityName + '.' + fieldName + ' is searchable');
//         newDef.isSearchable = true;
//       }
//       else if (val == 'readonly') {
//
//         console.log(entityName + '.' + fieldName + ' is readonly');
//         newDef.readonly = true;
//       }
//       else if (val == 'hidden') {
//
//         // This field may be used for searching
//         console.log(entityName + '.' + fieldName + ' is hidden');
//         newDef.hidden = true;
//       }
//       else if (val == 'dropdown') {
//
//         // This field may be used for searching
//         console.log(entityName + '.' + fieldName + ' is dropdown');
//         newDef.dropdown = true;
//       }
//       else if (val == 'autocomplete') {
//
//         // This field may be used for searching
//         console.log(entityName + '.' + fieldName + ' is autocomplete');
//         newDef.autocomplete = true;
//       }
//       else if (val.startsWith('*')) {
//
//         // References another table
//         newDef.reference = val.substring(1).trim();
//         var referencedEntity = schema[newDef.reference];
//         if (referencedEntity) {
//           newDef.referencedEntity = referencedEntity;
//         } else {
//           // Reference to Unknown view
//           console.log('SCHEMA ERROR: Invalid reference from ' + entityName + '.' + fieldName + ' to ' + newDef.reference + '.');
//           newDef.broken = true;
//         }
//       } else {
//
//         // No idea what to do with this field definition.
//         console.log('SCHEMA ERROR: Unknown field qualifier for ' + entityName + '.' + fieldName + ': \'' + val + '\'');
//         newDef.broken = true;
//         break;
//       }
//     }
//   } else {
//     console.log('SCHEMA ERROR: invalid definition for ' + entityName + '.' + fieldName + '.');
//     newDef.broken = true;
//   }
//
//   return newDef;
// }
