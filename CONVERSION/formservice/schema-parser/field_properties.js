/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
/*
 *  List of properties and modifiers for view fields.
 */

export default {
  /*
   *  Primary properties
   */
  primary: 'isPrimaryKey',
  index: 'isIndex', // json only
  description: 'isDescription',
  searchable: 'isSearchable',
  mandatory: 'isMandatory',

  /*
   *  Visibility
   */
  virtual: 'virtual',
  hidden: 'hidden',
  readonly: 'readonly',

  /*
   *  Modifiers
   */
  // String related modifiers
  text: 'text',
  richtext: 'richtext',
  autocomplete: 'autocomplete',
  dropdown: 'dropdown',

  // Boolean, integer, and string related.
  checkbox: 'checkbox',

  // Date and time related
  since: 'since',
  fromNow: 'fromNow',
  time: 'time',
  date: 'date',

  // Money related
  currency: 'currency',
  currency2: 'currency2',
  dollars: 'dollars',
  dollars2: 'dollars2',
  pounds: 'pounds',
  pounds2: 'pounds2',

  // Alignment
  'text-left': 'textLeft',
  'text-center': 'textCenter',
  'text-right': 'textRight',
  'text-justify': 'textJustify',
};

exports.isDateField = function(field) {
  // console.log('field_properties.isDateField:', field);

  if (
    field.properties.date
    || field.properties.time
    || field.properties.datetime
    || field.properties.fromNow
  ) {
    // Is a time-related value
    console.log('\n\nHAVE A TIME-RELATED FIELD:', field);
    return true;
  }
  return false;
}
