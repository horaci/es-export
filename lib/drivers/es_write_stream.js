var stream = require('stream');
var async = require('async');

var DEFAULT_SIZE = 250;
var META_FIELDS = ['_timestamp', '_routing', '_version', '_percolate', '_parent', '_ttl'];

var WriteStream = function(driver) {
  var self = this;

  this.driver = driver;
  this.written = 0;
  this.finished = false;
  this.drained = true;
  this.write_size = (driver.opts.size || DEFAULT_SIZE);

  this.cargo = async.cargo(this.sendBatch.bind(this), this.write_size);

  stream.Writable.call(this, {
    highWaterMark: 2,
    objectMode: true
  });

  this.cargo.saturated = function() { self.saturated = true;  };
  this.cargo.empty     = function() { self.saturated = false; };
  this.cargo.drain     = function() { self.drained   = true;  };
}

WriteStream.prototype = Object.create(stream.Writable.prototype, {
  constructor: { value: WriteStream }
});

WriteStream.prototype._write = function(object, encoding, callback) {
  var self = this;

  if(object && object !== "") {
    this.cargo.push(object);
    this.drained = false;

    var whileNotDrained = function() {
      if(self.cargo.length() > self.write_size) {
        setTimeout(whileNotDrained, 10);
      } else {
        callback();
      }
    }

    setImmediate(whileNotDrained);
  } else {
    setImmediate(callback);
  }
}

WriteStream.prototype.sendBatch = function(batch, callback) {
  var index = this.driver.opts.index;
  var type = this.driver.opts.type;

  this.written += batch.length;

  var requests = batch.reduce(function(memo, obj) {
    var meta = {
      index: {
        _index:   index || obj._index,
        _type:    type  || obj._type,
        _id:      obj._id,
        _version: obj._version
      }
    }

    if(obj.fields) {
      META_FIELDS.forEach(function(field) {
        if(obj.fields[field]) meta.index[field] = obj.fields[field];
      });
    }

    memo.push(JSON.stringify(meta))
    memo.push(JSON.stringify(obj._source));

    return memo;
  }, []).join("\n") + "\n";

  this.driver.post(['_bulk'], requests, function(err, response) {
    if(err) {
      console.log("Error writting batch (no retry logic yet):", err);
      console.log(JSON.stringify(response, null, 4));
      process.exit(1);
    }
    process.nextTick(callback);
  });
}

module.exports = WriteStream;
