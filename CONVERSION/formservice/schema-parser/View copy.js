const humanize = require("underscore.string/humanize")
const { Field } = require('./Field');
const { References } = require('./References');

const TYPE_MESSAGE = 'message'
const TYPE_JSON = 'json'
const TYPE_MYSQL = 'mapped'
const TYPE_UNKNOWN = 'unknown'

/*
 *  View object.
 */
export default class View {

  constructor(viewName) {
    this.name = viewName;
    this.storageType = TYPE_UNKNOWN

    this.fieldLookup = { }; // name -> Field
    // this.fieldArray = [ ]; // [Field]
    this.primaryKeyFields = [ ]; // -> field definition
    this.descriptionField = null; // -> field definition
    this.indexFields = [ ] // -> field definition (only used when storageType === 'json')

    // Keep track of the views referenced by this view.
    // We will need to check later that we specify all the primary key
    // fields of the referenced fields.
    // this.referencedViews = [ ]; // referencedViewName -> false (set to true once definition is verified)

    this.referenceList = new References(this.name)
  }

  async parseShortform (viewDef) {
    // let me = this

    //this.schema = schema;
    this.definition = viewDef;


    if (viewDef.label) {
      this.label = viewDef.label;
    } else {
      this.label = humanize(this.name);
    }

    // If this is stored as raw JSON, we don't need to do much.
    this.storageType = TYPE_MYSQL
    if (typeof(viewDef.type) !== 'undefined') {
      switch (viewDef.type) {
        case TYPE_MESSAGE:
        case TYPE_MYSQL:
        case TYPE_JSON:
          this.storageType = viewDef.type
          break

        default:
          if (viewDef.storeAsJSON) {
            this.storageType = TYPE_JSON
          }
          break
      }
    }

    if (viewDef.storageType === TYPE_JSON) {
      if (viewDef.key) {
        this.key = viewDef.key
      } else {
        this.key = '{{RANDOM}}'
        console.log(`View ${this.name} does not `);
      }

      // Add the key field
      var field = new Field(this, 'key', null);
      field.properties.column = 'unique_key'
      field.properties.label = 'Unique ID'
      field.properties.isDescription = false
      this.fieldLookup['key'] = field;
      this.primaryKeyFields.push('key')
    // }
    // if (viewDef.storeAsJSON) {
    //   this.storageType = 'json'
    //   if (viewDef.key) {
    //     this.key = viewDef.key
    //   } else {
    //     this.key = '{{RANDOM}}'
    //     console.log(`View ${this.name} does not `);
    //   }

    //   // Add the key field
    //   var field = new Field(this, 'key', null);
    //   field.properties.column = 'unique_key'
    //   field.properties.label = 'Unique ID'
    //   field.properties.isDescription = false
    //   this.fieldLookup['key'] = field;
    //   this.primaryKeyFields.push('key')
    } else if (viewDef.type === TYPE_MYSQL) {

      // This view must be mapped onto a database table
      // this.storageType = TYPE_MAPPED

      // Default table name is the view name.
      this.tableName = (viewDef.table) ? viewDef.table : this.name;
    }

    // If this is
    let isJsonView = (this.storageType === TYPE_JSON)
    if (isJsonView) {

    }


    // Parse the field definitions


    // Expand each field definition to include defaults.
    // Also look for the primary key and description for this entity.
    // Also, if this is a storeAsJSON view, update column names
    // (Fields are defined as properties of the 'fields' property).
    // http://stackoverflow.com/questions/8312459/iterate-through-object-properties
    let jsonIndexCnt = 0
    // console.log('Parsing fields:', fields);
    let fields = viewDef.fields;
    if (!fields && !isJsonView) {
      console.log(`SCHEMA ERROR: view ${this.name} is missing 'fields' property.`);
      this.broken = true;
    }
    let numFields = 0
    for (var property in fields) {
      if (!fields.hasOwnProperty(property)) continue;
      var fieldName = property;
      var fieldDef = fields[fieldName];

      var field = new Field(this, fieldName, fieldDef);

      this.fieldLookup[fieldName] = field;
      numFields++
      // this.fieldArray.push(field);
      if (!isJsonView && field.isPrimaryKey()) {
        this.primaryKeyFields.push(field.name);
        // this.primaryKeyFields.push(field);
      }
      if (field.isDescription()) {
        this.descriptionField = field.name;
        // this.descriptionField = field;
        if (isJsonView) {
          field.properties.column = 'description'
        }
      }
      if (field.isIndex()) {
        this.indexFields.push(field.name)
        if (isJsonView) {
          field.properties.column = `index_${1 + jsonIndexCnt++}`
        }
      }

      // // Remember if this references another view
      // let reference = field.getReference()
      // // { view: String, field: String }
      // if (reference) {
      //   this.referencedViews[reference.view] = true;
      // }
    }
    if (numFields===0 && !isJsonView) {
      console.log(`SCHEMA ERROR: view ${this.name} defines no fields.`);
      this.broken = true;
    }

    // Expand out the mode definitions.
    //ZZZZZ
    this.modes = (viewDef.modes) ? viewDef.modes : { };

    // Look for callback functions
    this._callbacks = (viewDef.extensions) ? viewDef.extensions : { };

    // if (this.storageType === 'json') {
    //   console.log(`JOOSIK`, this);
    // }
  }

  getName () {
    return this.name;
  }
  getSchema () {
    return this.schema;
  }
  getField (name) {
    return this.fieldLookup[name] ? this.fieldLookup[name] : null;
  }
  fields () {
    let arr = [ ]
    for (let field in this.fieldLookup) {
      arr.push(this.fieldLookup[field])
    }
    // console.log(`\n\n\n\nNEW ALL IS\n`, arr);
    // console.log(`\n\n\n\nFIELDARRAY IS\n`, this.fieldArray);
    return arr
    // return this.fieldArray;
  }
  getModes () {
    return this.modes;
  }
  getLabel () {
    return this.label;
  }
  getTableName () {
    if (this.storageType === 'json') {
      return 'generic_json'
    }
    return this.tableName;
  }
  getPrimaryKeyFields () {
    let arr = [ ];
    this.primaryKeyFields.forEach(fieldName => {
      arr.push(this.fieldLookup[fieldName])
    })
    return arr
    // return this.primaryKeyFields;
  }
  isSingleFieldPrimaryKey () {
    return (this.primaryKeyFields.length === 1)
  }
  getDescriptionField () {
    let fieldName = this.descriptionField
    return this.fieldLookup[fieldName]
    // return this.descriptionField;
  }
  getReference (referenceName) {
    return this.referenceList.getReference(referenceName)
  }
  getReferences () {
    return this.referenceList.references
  }
  callback (operation) {
    return (this._callbacks[operation]) ? this._callbacks[operation] : null;
  }
  toString () {
    return '[View:' + this.name + ']';
  }

  addReference(fromFieldName, reference) {
    this.referenceList.addFieldReference(fromFieldName, reference)
  }


//   // Check that all the field's referencing other views are valid.
//   // This couldn't be done previously in parseReference() because
//   // the views were not yet loaded when it was being called.
//   verifyReferences (viewList) {
//     console.log(`verifyReferences(${this.name})`);
//     console.log('    ==>', this.referencedViews);
//     console.log('    *=>', this.primaryKeyFields);
//     let viewName = this.getName()
//
//     /*  Some field references specify the view, but not the field.
//      *
//      *      <fieldname>: '*<viewname>'
//      *
//      *  In this case the field refers to a single primary key, but we
//      *  couldn't know what it was when we were parsing the reference,
//      *  because the views were not all loaded. So, patch the primary key
//      *  field name in now, and also confirm that it does refer to a single
//      *  field primary key.
//      */
// console.log(`verifyReferences PART 1`);
//     this.fieldArray.forEach((field) => {
//       let reference = field.getReference()
//       // { view: String, field: String }
//       if (reference && reference.field === null) {
//         console.log(`  ${field.name}=>`, reference);
//
//         // reference.view is either <name> or <name>{<alias>}, but need just <name>
//         let referencedViewName = reference.view
//         let pos = referencedViewName.indexOf('{')
//         if (pos > 0) {
//           referencedViewName = referencedViewName.substring(0, pos).trim();
//         }
//         console.log(`======= ${reference.view} ====> ${referencedViewName}`)
//
//         let targetView = viewList.get(referencedViewName)
//         if (!targetView) {
//           console.log(`SCHEMA ERROR: ${viewName}.${field.getName()} references unknown view ${referencedViewName}.`);
//           this.broken = true;
//           return; // On to the next field
//         }
//
//         let numPrimaryKeys = 0
//         targetView.getPrimaryKeyFields().forEach(keyField => {
//           if (numPrimaryKeys++ === 0) {
//             reference.field = keyField.getName()
//             // console.log(`- ${viewName}.${field.getName()}: patching in primary key for ${reference.view} - ${keyField.getName()}`)
//           }
//         })
//         if (numPrimaryKeys === 0) {
//           console.log(`SCHEMA ERROR: ${viewName}.${field.getName()} (*${reference.view}) references view with no primary key.`);
//           this.broken = true;
//         } else if (numPrimaryKeys > 1) {
//           console.log(`SCHEMA ERROR: ${viewName}.${field.getName()} (*${reference.view}) references view with multiple primary keys.`);
//           this.broken = true;
//         }
//       }
//     }) //- field
//
// console.log(`verifyReferences PART 2`);
//
//     /*  For each view this view references, check that we have
//      *  exact matching to it's primary keys. Error conditions:
//      *
//      *  - does not have references to all the primary key fields
//      *  - references some unknown field
//      *  - references a non primary key field
//      */
//     for (let referencedViewNameAndAlias in this.referencedViews) {
//       if (!this.referencedViews.hasOwnProperty(referencedViewNameAndAlias)) continue;
//
//       // Have either <name> or <name>{<alias>}, but need just <name>
//       // If the referenced view name has an alias specified using {...}, remove it.
//       // console.log(`\n\n*** CHECK REFERENCE FROM ${this.name} TO ${referencedViewNameAndAlias}`);
//       let referencedViewName = referencedViewNameAndAlias;
//       let pos = referencedViewNameAndAlias.indexOf('{')
//       if (pos > 0) {
//         referencedViewName = referencedViewNameAndAlias.substring(0, pos).trim();
//       }
//       // console.log(`======= ${referencedViewNameAndAlias} ====> ${referencedViewName}`)
//
//       // Find the view being referenced
//       let targetView = viewList.get(referencedViewName)
//       if (!targetView) {
//         // Already reported above
//         continue; // On to the next reference
//       }
//
//       // console.log('viewList=', viewList);
//       // console.log(`targetView=`, targetView);
//       let targetKeyFields = [ ]; // name -> false
//       targetView.primaryKeyFields.forEach((field) => {
//         targetKeyFields[field.name] = 'notfound';
//       })
//       // console.log('required target fields =', targetKeyFields);
//
//       // See if we have all of the required target fields (no more, no less)
//       // console.log('view is', this);
//       this.fieldArray.forEach((field) => {
//         let reference = field.getReference()
//         // { view: String, field: String | null }
//         if (reference && reference.view === referencedViewNameAndAlias) {
//           // console.log(`   >>> ${this.name}.${field.getName()} => `, reference);
//
//           if (reference.field !== null) {
//
//             // Check all our references are valid, and no duplicates.
//             if (targetKeyFields[reference.field] == 'notfound') {
//               // Correct reference
//               targetKeyFields[reference.field] = 'found';
//             } else if (targetKeyFields[reference.field] == 'found') {
//               // Already been mapped
//               console.log(`SCHEMA ERROR: view ${this.getName()} has multiple fields referencing`);
//               console.log(`              ${referencedViewNameAndAlias}.${reference.field}. Use an alias?`);
//               this.broken = true;
//               return; // on to the next field
//             } else {
//               // Reference to unknown primary key
//               console.log(`SCHEMA ERROR: ${this.name}.${field.getName()} references unknown primary key ${reference.view}.${reference.field}.`);
//               this.broken = true;
//               return; // on to the next field
//             }
//           } // field != null
//         }
//       })//- next field in view
//
//       // Check all the primary key fields were referenced.
//       for (let fieldName in targetKeyFields) {
//         if (!targetKeyFields.hasOwnProperty(fieldName)) continue;
//
//         if (targetKeyFields[fieldName] !== 'found') {
//           console.log(`SCHEMA ERROR: ${this.name} references ${referencedViewNameAndAlias} but is missing primary key ${fieldName}.`);
//           this.broken = true;
//           break; // on to the next field
//         }
//       }//- next primary key field in target
//     }//- next view+alias
// console.log(`verifyReferences PART 99`);
//
//   }//- verifyReferences
}
