
module.exports.ViewList = class ViewList {
  constructor ( ) {
    this.index = { }; // name -> number (position in list)
    this.list = [ ]; // [View]
  }

  add ( view ) {
    this.index[view.name] = this.list.length
    this.list.push(view)
  }

  get (name) {
    // console.log(`viewlist.get(${name})`);
    if (this.index[name] !== undefined) {
      let position = this.index[name]
      // console.log(`position is ${position}`);
      if (position < 0 || position >= this.list.length) {
        console.error(`Viewlist: Internal error; bad index value (View=${name}, position=${position})`);
        return null
      }
      return this.list[position]
    }
    // unknown view
    return null
  }

  forEach (fn) {
    this.list.forEach(fn)
  }
}
