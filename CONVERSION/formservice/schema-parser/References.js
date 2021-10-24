/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

/* A reference between tables.
 *  For a simple single field primary key reference, the name
 *  of the reference will be field name.
 *
 *  For multi-field primary key references, the name will be
 *  the name of the alias if provided, or else the name of the
 *  referenced view.
 *  Note that for multiple references to the same table,
 *  aliases will be required.
 */

module.exports.References = class References {
  constructor(viewName) {
    this.viewName = viewName
    this.references = [ ]
  }

  addFieldReference (fromFieldName, definition) {
    let ref = {
      name: null,
      referenceType: null,
      targetView: null,
      targetAlias: null,
      fields: [{
        field: fromFieldName,
        targetFieldName: null,
        definition: `${fromFieldName}: ${definition}`,
      }],
      verified: false
    }

    let reference = definition
    if (reference.startsWith('*')) {
      reference = reference.substring(1)
    }

    // Split 'reference' into view name, optional alias, and field name.
    //  Examples:
    //    *thing
    //    *thing.id
    //    *thing(mything)
    //    *thing(mything).id
    let pos = reference.indexOf('.')
    // if (pos == 0) {
    //   const msg = `Invalid field definition for ${this.viewName}.${fromFieldName}`
    //   throw new Error(msg)
    // }
    if (pos > 0) {
      ref.fields[0].targetFieldName = reference.substring(pos + 1)
      reference = reference.substring(0, pos)
    }
    pos = reference.indexOf('(')
    if (pos > 0 && reference.endsWith(')')) {
      ref.targetAlias = reference.substring(pos + 1, reference.length - 1)
      reference = reference.substring(0, pos)
    }
    ref.targetView = reference

    // // If no field name was specified, as if the target is a single-field primary key
    // if (ref.fields[0].targetFieldName.trim() === '') {
    //   let tv = schema.
    // }

    // Add to the list
    this.references.push(ref)
  }

  /*
   *  Group field references by:
   *    1. The alias, if an alias for the target view is provided.
   *    2. The field name, for single-field primary key references.
   *    3. The name of the view being referenced.
   *
   *  At the same time, resolve any references where a field name is
   *  not provided, to point to the primary key of the target view.
   */
  consolidate (schema) {
    // console.log(`consolidate ${this.viewName}`);

    // We will copy references across by categories, and
    // merge the definitions during the process.
    let newRefs = [ ]

    // Pass 1 - Find fields that reference with alias
    for (let i1 = 0; i1 < this.references.length; ) {
      let ref1 = this.references[i1]
      if (ref1.targetAlias) {
        this.references.splice(i1, 1) // Remove ref1 from array
        ref1.name = ref1.targetAlias
        ref1.referenceType = 'alias'
        newRefs.push(ref1)

        // Merge in any other references for the same alias.
        for (let i2 = i1; i2 < this.references.length; ) {
          let ref2 = this.references[i2]
          if (ref2.targetAlias === ref1.targetAlias) {
            // console.log(`targetAlias IS THE SAME!!!!!`);
            if (ref2.targetView !== ref1.targetView) {
              console.log(`Schema error: view ${this.viewName}: reference ${ref1.targetAlias} refers to multiple views`);
              return true // broken
            }
            this.references.splice(i2, 1) // will delete ref2
            ref2.fields.forEach(fld => { ref1.fields.push(fld) })
          }
          else {
            i2++
          }
        }
      } else {
        // Ref1 was not an alias
        i1++
      }
    }

    // Pass 2 - Find fields that reference single field primary keys.
    for (let i1 = 0; i1 < this.references.length; ) {
      let ref = this.references[i1]
      let target = schema.getView(ref.targetView)
      if (!target) {
        console.log(`Schema error: ${this.viewName}.${ref.fields[0].field}: unknown view: ${ref.fields[0].definition}`);
        return true
      }
      if (target.isSingleFieldPrimaryKey()) {
        this.references.splice(i1, 1) // Removes ref1
        ref.name = ref.fields[0].field
        ref.referenceType = 'single field primary key'
        newRefs.push(ref)
      } else {
        // Not single field primary key
        i1++
      }
    }

    // Pass 3 - Combine fields for each target view where there is no alias.
    while (this.references.length > 0) {
      let ref1 = this.references[0]
      this.references.splice(0, 1) // Remove ref1 from array
      ref1.name = ref1.targetView
      ref1.referenceType = 'by view name'
      newRefs.push(ref1)
      // console.log(`ref1=\n`, ref1);

      // Merge in any other references to the same view.
      for (let i2 = 0; i2 < this.references.length; ) {
        let ref2 = this.references[i2]
        // console.log(`ref2=\n`, ref2);
        // console.log(`  ++ ${ref2.targetView} vs ${ref1.targetView}`);
        if (ref2.targetView === ref1.targetView) {
          // console.log(`targetView IS THE SAME!!!!!`);
          this.references.splice(i2, 1) // will delete ref2
          ref2.fields.forEach(fld => { ref1.fields.push(fld) })
        }
        else {
          i2++
        }
      }
    }

    // Resolve references that don't have field names
    for (let i = 0; i < newRefs.length; i++) {
      let ref = newRefs[i]
      // console.log(`RESOLVE NEWREF`, ref);
      let target = schema.getView(ref.targetView)
      let primaryKeyFields = target.getPrimaryKeyFields()
      // let isMultiFieldReference = (ref.fields.length > 1)
      for (let f = 0; f < ref.fields.length; f++) {
        let fldref = ref.fields[f]
        // console.log(` --> `, fldref);
        // See if the target field is provided
        if (!fldref.targetFieldName) {

          // We have a target view, but no field is specified.
          //
          // Phil 23 May 2019 - commented this out, because we get an error when
          // multiple fields in a table reference the same view. Not sure the logic is right.
          //
          // if (isMultiFieldReference) {
          //   // Can't imply target field name, if multiple fields reference this target
          //   console.log(`Schema error: ${this.viewName}.${fldref.field}: reference needs to specify a field`);
          //   console.log(`(multiFieldReference)`);
          //   return true // broken
          // }
          if (primaryKeyFields.length === 0) {
            // Can't imply target field name, target has a multi-part primary key
            console.log(`Schema error: ${this.viewName}.${fldref.field}: reference view without a primary key`);
            console.log(`(no primary key)`);
            console.log(`target is`, target);
            return true // broken
          }
          if (primaryKeyFields.length > 1) {
            // Can't imply target field name, target has a multi-part primary key
            console.log(`Schema error: ${this.viewName}.${fldref.field}: reference needs to specify a field`);
            console.log(`(not a single field primary key)`);
            return true // broken
          }
          // console.log(`Setting to ${primaryKeyFields[0].name}`);
          fldref.targetFieldName = primaryKeyFields[0].name
        }
      }
      // console.log(`  view is `, target);
      // console.log(`fields are:`, target.fields());
      // let fields = target.fields()
      // for (let f = 0; f < fields.length; f++) {
      //   let field = fields[f]
      //   console.log(`   --- check field ${field.name}`, field);
      // }
    }


    //ZZZZZ
    // this.references.forEach(ref => {
    //   ref.name = ref.fields[0].field
    //   ref.referenceType = 'JUNK DUD HACK DEFAULT'
    //   newRefs.push(ref)}
    // )

    this.references = newRefs
    return false // not broken
  } // consolidate

  /*
   *  Validate the reference definitions.
   *  For each reference, check
   *  1. The entire primary key is specified.
   *  2. There are not multiple fields mapped onto any part of the key.
   */
  validate (schema) {
    // console.log(`\nReferences.validate(${this.viewName})`);

    for (let i = 0; i < this.references.length; i++) {
      let ref = this.references[i]

      // Get the target view
      let target = schema.getView(ref.targetView)
      if (!target) {
        console.log(`Schema error: invalid reference in view ${this.viewName} (${ref.fields[0].definition})`);
        return true // broken
      }

      // Check all primary key fields are specified.
      let pkeys = target.getPrimaryKeyFields()
      for (let keyCnt = 0; keyCnt < pkeys.length; keyCnt++) {
        let pkey = pkeys[keyCnt]
        // console.log(` - key=${pkey.name}`);

        // Check this primary key field is mapped
        let cntMapping = 0
        ref.fields.forEach(field => {
          // console.log(`    - ${field.targetFieldName} vs ${pkey.name}`);
          if (field.targetFieldName === pkey.name) {
            // console.log(`      Found primary key`);
            cntMapping++
            field._wasMapped = true
          }
        })
        // console.log(`   - cntMapping=${cntMapping}`);
        if (cntMapping == 0) {
          // No mapping to this primary key field
          console.log(`Schema error: view ${this.viewName}: incomplete primary key in reference '${ref.name}'`);
          return true // broken
        }
        if (cntMapping > 1) {
          // Multiple mappings to this primary key field
          console.log(`Schema error: view ${this.viewName}: multiple fields refer to : ${ref.name}.${pkey.name}`);
          return true // broken
        }
      }
      // console.log(`   - Checking all used`);
      // Check that each field in the reference was mapped to a primary key.
      for (let i = 0; i < ref.fields.length; i++) {
        let field = ref.fields[i]
        if (!field._wasMapped) {
          console.log(`Schema error: view ${this.viewName}: field ${field.field} does not map to a primary key`);
          return true // broken
        }
      }
    }
    return false // not broken
  }//- validateCheck reference

  // Get a specific reference by name
  getReference (referenceName) {
    for (let i = 0; i < this.references.length; i++) {
      let reference = this.references[i]
      if (reference.name === referenceName) {
        return reference
      }
    }
    return null
  }//- getReference

  dump ( ) {
    console.log(`References for ${this.viewName}:\n`);
    this.references.forEach((ref) => {
      console.log(`\n  - `, ref);
    })
  }

  toString ( ) {
    return `References[view=${this.viewName}, ${this.references.length} references]`
  }
}
