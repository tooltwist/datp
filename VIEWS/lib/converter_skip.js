

export default class {
  async convert(handler, destObject, toField, source, description) {
    // console.log(`converter_unknown.convert(${toField}, ${source})`)
    console.log(`converter: skipping ${toField}.`)
    const value = 'SKIP'
    handler.setValue(destObject, toField, value)
  }
}
