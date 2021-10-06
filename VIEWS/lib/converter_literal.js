

export default class {

  async convert(handler, destObject, toField, source, description) {
    // console.log(`converter_literal.convert(${toField}, ${source})`)
    const value = source
    handler.setValue(destObject, toField, value)
  }

}
