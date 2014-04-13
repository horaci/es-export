var Options = require('./options');
var Driver = require('./driver');

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
    driver: 'es',
    host:   this.opts[type + "Host"],
    port:   this.opts[type + "Port"],
    index:  this.opts[type + "Index"],
    type:   this.opts[type + "Type"],
    auth:   this._parseAuth(this.opts[type + "Auth"])
  }
}

Exporter.prototype._parseAuth = function(auth_str) {
  if(!auth_str) return;
  var parts = auth_str.split(':');
  return {user: parts[0], pass: parts[1]};
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

  sourceStream.on('readed', function() {
    console.log("Readed:", sourceStream.readed, "/", sourceStream.total, " - Written:", targetStream.written);
  });

  var whileNotDrained = function() {
    if(!targetStream.drained) {
      console.log("Waiting for writter. Written:", targetStream.written);
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
