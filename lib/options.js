var nomnom = require('nomnom');

var OPTIONS = {
  sourceHost:   { abbr: 'a', default: 'localhost' },
  sourcePort:   { abbr: 'p', default: 9200 },
  sourceIndex:  { abbr: 'i' },
  sourceType:   { abbr: 't' },
  sourceSize:   { abbr: 'z', default: 10 },

  targetHost:   { abbr: 'b' },
  targetPort:   { abbr: 'q' },
  targetIndex:  { abbr: 'j' },
  targetType:   { abbr: 'u' },
  targetSize:   { abbr: 'y', default: 10 },

  sourceAuth:   { metavar: '<username:password>' },
  targetAuth:   { metavar: '<username:password>' },
}

module.exports = function() {
  var opts = nomnom.script('export').options(OPTIONS).parse();

  if(!opts.targetHost) opts.targetHost= opts.sourceHost;
  if(!opts.targetPort) opts.targetPort= opts.sourcePort;

  if(!opts.targetIndex) opts.targetIndex  = opts.sourceIndex;
  if(!opts.sourceIndex) opts.sourceIndex  = opts.targetIndex;

  if(!opts.targetType) opts.targetType= opts.sourceType;
  if(!opts.sourceType) opts.sourceType = opts.targetType;

  if(opts.sourceTarget && !opts.sourceIndex) {
    throw("Must set source or target index when setting source or target type");
  }

  if(opts.sourceHost == opts.targetHost && opts.sourcePort == opts.targetPort && opts.sourceIndex == opts.targetIndex && opts.sourceType == opts.targetType) {
    throw("Target should be different from source");
  }

  return opts;
}
