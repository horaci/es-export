var DRIVERS = {
  "es":   require('./drivers/es')
}

var Driver = function(opts) {
  this.driver = new DRIVERS[opts.driver](opts);
}

Driver.prototype.readMaping = function(cb) {
  this.driver.readMapping(cb);
}

Driver.prototype.writeMapping = function(source, mapping, cb) {
  this.driver.writeMapping(source, mapping, cb);
}

Driver.prototype.createReadStream = function() {
  return this.driver.createReadStream();
}

Driver.prototype.createWriteStream = function() {
  return this.driver.createWriteStream();
}

module.exports = Driver;
