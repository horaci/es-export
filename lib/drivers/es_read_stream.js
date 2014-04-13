var stream = require('stream');

var ReadStream = function(driver) {
  this.driver = driver;
  this.total = 0;
  this.readed = 0;
  this.reading = false;

  this.driver.createCursor(function(err, result) {
    this.scroll_id = result._scroll_id;
    this.total = result.hits.total;
    this.hits = result.hits.hits;
  }.bind(this));

  stream.Readable.call(this, {
    highWaterMark: 1000,
    objectMode: true
  });
}

ReadStream.prototype = Object.create(stream.Readable.prototype, {
  constructor: { value: ReadStream }
});

ReadStream.prototype._read = function(size) {
  var self = this;

  if(!this.scroll_id) {
    console.log("Waiting for scroll cursor to be ready...");
    setTimeout(this._read.bind(this), 1000);
    return;
  }

  if(this.readed == this.total) {
    this.push(null);
    return;
  }

  if(this.reading) {
    return;
  }

  this.reading = true;

  if(this.hits && this.hits.length > 0) {
    for(var i=0; i<this.hits.length; i++) {
      this.push(this.hits[i]);
    }
    this.hits = null;
  }

  this.driver.readCursor(this.scroll_id, function(err, data) {
    if(data._scroll_id) self.scroll_id = data._scroll_id;
    if(data.hits && data.hits.hits && data.hits.hits.length > 0) {
      for(var i=0; i<data.hits.hits.length; i++) {
        self.readed++;
        self.push(data.hits.hits[i]);

        if((self.readed % 1000) == 0 || self.readed == self.total) {
          self.emit('readed');
        }
      }

      if(self.readed == self.total) {
        self.emit('all_readed');
        self.push(null);
      }
    } else {
      self.emit('all_readed');
      self.push(null); // end of stream
    }
    self.reading = false;
  })
};

module.exports = ReadStream;
