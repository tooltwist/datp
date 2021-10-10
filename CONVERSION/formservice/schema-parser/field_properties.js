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
