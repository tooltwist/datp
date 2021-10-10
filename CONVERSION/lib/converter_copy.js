

export default class {
  async convert(handler, destObject, toField, source, description) {
    // console.log(`converter_copy.convert(${toField}, ${source})`)
    try {
      const value = handler.getSourceValue(source)
      handler.setValue(destObject, toField, value)
    } catch (e) {
      console.log(`Rule failed: ${description} (${e.message})`)
    }
  }
}
