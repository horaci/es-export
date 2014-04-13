var Options = require('./options');
var Driver = require('./driver');
var colors = require('colors');

var Exporter = function(opts) {
  this.opts = opts || Options();
  this.sourceDriver = new Driver(this._optionsFor('source'));
  this.targetDriver = new Driver(this._optionsFor('target'));
}

Exporter.prototype.start = function(cb) {
  this._mapping(function(err, write_result) {
    this._data(cb);
  }.bind(this));
}

Exporter.prototype._optionsFor = function(type) {
  return {
    driver:   'es',
    host:     this.opts[type + "Host"],
    port:     this.opts[type + "Port"],
    index:    this.opts[type + "Index"],
    type:     this.opts[type + "Type"],
    auth:     this.opts[type + "Auth"],
    verbose:  this.opts.verbose
  }
}

Exporter.prototype._mapping = function(cb) {
  var source_opts = this._optionsFor('source');

  this.sourceDriver.readMaping(function(err, mapping) {
    this.targetDriver.writeMapping(source_opts, mapping, cb);
  }.bind(this));
}

Exporter.prototype._data = function(cb) {
  var sourceStream = this.sourceDriver.createReadStream();
  var targetStream = this.targetDriver.createWriteStream();
  var start = Date.now();

  var onRead = function() {
    console.log("Readed:", sourceStream.readed, "/", sourceStream.total, "(" + (sourceStream.readed * 1000 / (Date.now()-start)).toFixed(2) + "/s)", "- Written:", targetStream.written, "(" + (targetStream.written * 1000 / (Date.now()-start)).toFixed(0) + "/s)");
  };

  sourceStream.on('readed', onRead);

  var whileNotDrained = function() {
    if(!targetStream.drained) {
      console.log("Waiting for writter");
      onRead();
      setTimeout(whileNotDrained, 1000);
    } else {
      console.log("Write finished, all good.")
      console.log("Readed:", sourceStream.readed, "- written:", targetStream.written);
      process.nextTick(cb);
    }
  }

  // sourceStream.on('all_readed', whileNotDrained);
  sourceStream.on('end', whileNotDrained);
  sourceStream.pipe(targetStream);
}

module.exports = Exporter;
