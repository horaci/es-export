var Exporter = require('./lib/exporter');

if(require.main === module) {
  new Exporter().start(function() {
    console.log("Finsihed")
    process.exit(0);
  });
} else {
  module.exports = Exporter;
}
