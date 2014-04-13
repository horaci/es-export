var request = require('request');
var async = require('async');
var ESReadStream = require('./es_read_stream');
var ESWriteStream = require('./es_write_stream');

var DEFAULT_PORT = 9200;
var DEFAULT_SIZE = 50;

var ElasticSearchDriver = function(opts) {
  this.opts = opts;
}

ElasticSearchDriver.prototype.createReadStream = function() {
  return new ESReadStream(this);
}

ElasticSearchDriver.prototype.createWriteStream = function() {
  return new ESWriteStream(this);
}

ElasticSearchDriver.prototype.readMapping = function(cb) {
  var self = this;

  this.get([this.opts.index, this.opts.type, '_mapping'], function(err, mapping) {
    if(self.opts.type) return cb(err, mapping);

    self.get([self.opts.index, '_settings'], function(err, settings) {
      var result;

      if(self.opts.index) {
        result = {
          mappings: mapping[self.opts.index],
          settings: settings[self.opts.index].settings
        };
      } else {
        result = Object.keys(mapping).reduce(function(memo, el) {
          memo[el] = {
            mappings: mapping[el],
            settings: settings[el].settings
          };
          return memo;
        }, {});
      }

      cb(null, result);
    });
  });
}

ElasticSearchDriver.prototype.writeMapping = function(source, mapping, cb) {
  var dest_type = this.opts.type || source.type;
  var dest_index = this.opts.index || source.index;

  if(!mapping) return cb("no_mappings");

  if(source.index && source.type) {
    this.putTypeMapping(dest_index, dest_type, mapping, cb, true);
  } else if(source.index) {
    this.putIndexMapping(dest_index, mapping, cb);
  } else {
    this.putAllMappings(mapping, cb);
  }
}

ElasticSearchDriver.prototype.putTypeMapping = function(index, type, mapping, cb, with_index) {
  var self = this;

  if(with_index) {
    this.head([index], function(err, exists) {
      if(exists.statusCode == 404) {
        this.put([index], function(err, result) {
          self.putTypeMapping(index, type, mapping, cb);
        });
      } else {
        self.putTypeMapping(index, type, mapping, cb);
      }
    });
  } else {
    this.put([index, type, '_mapping'], mapping, cb);
  }
}

ElasticSearchDriver.prototype.putIndexMapping = function(index, mapping, cb) {
  var self = this;

  this.head([index], function(err, exists) {
    if(exists.statusCode == 404) {
      self.post([index], mapping, cb);
    } else {
      // create types individually
      async.mapSeries(Object.keys(mapping.mappings), function(type, async_cb) {
        self.putTypeMapping(index, type, mapping.mappings[type], async_cb);
      }, cb);
    }
  });
}

ElasticSearchDriver.prototype.putAllMappings = function(mapping, cb) {
  async.mapSeries(Object.keys(mapping), function(index, async_cb) {
    this.putIndexMapping(index, mapping[index], async_cb);
  }.bind(this), cb);
}

ElasticSearchDriver.prototype.createCursor = function(cb) {
  var query = {
    fields : ['_source', '_timestamp', '_version', '_routing', '_percolate', '_parent', '_ttl'],
    size : this.opts.size || DEFAULT_SIZE,
  }

  if(this.opts.index) {
    query.query = {
      indices: {
        indices: [ this.opts.index ],
        query: { match_all:{} },
        no_match_query : 'none'
      }
    };
  }

  if(this.opts.type) {
    query.filter = { type: { value: this.opts.type }};
  }

  this.post(['_search?search_type=scan&scroll=5m'], query, cb);
}

ElasticSearchDriver.prototype.readCursor = function(scroll_id, cb) {
  this.post(['_search/scroll?scroll=5m'], scroll_id, cb);
}

// *** internal *** //

ElasticSearchDriver.prototype.request = function(method, path_parts, data, cb) {
  if(data instanceof Function) {
    cb = data;
    data = undefined;
  }

  var opts = {
    url: this._urlFor(path_parts),
    auth: this.opts.auth,
    method: method
  }

  if(typeof data == 'string') {
    opts.body = data;
  } else {
    opts.json = data;
  }

  console.log(opts.method.red, opts.url.green, data ? ((typeof data == 'string' ? data : JSON.stringify(data)).slice(0,100).replace(/[\n]/g, '\\n') + "...").grey.italic : '');

  request(opts, function(err, res, body) {
    this._parseJSONResponse(err, res, body, cb);
  }.bind(this));
}

ElasticSearchDriver.prototype.get = function(path_parts, cb) {
  this.request('GET', path_parts, cb);
}

ElasticSearchDriver.prototype.put = function(path_parts, body, cb) {
  this.request('PUT', path_parts, body, cb);
}

ElasticSearchDriver.prototype.post = function(path_parts, body, cb) {
  this.request('POST', path_parts, body, cb);
}

ElasticSearchDriver.prototype.head = function(path_parts, cb) {
  var opts = {
    url: this._urlFor(path_parts),
    auth: this.opts.auth,
    method: 'HEAD'
  }

  console.log(opts.method.red, opts.url.green);

  request(opts, function(err, res, body) {
    cb(err, {statusCode: res && res.statusCode });
  })
}

ElasticSearchDriver.prototype._urlFor = function(path_parts) {
  var path = '/' + path_parts.filter(function(el) { return el }).join('/');
  return 'http://' + this.opts.host + ":" + (this.opts.port || DEFAULT_PORT) + path;
}

ElasticSearchDriver.prototype._parseJSONResponse = function(err, res, body, cb) {
  var json;

  if(typeof body == 'string') {
    try {
      json = JSON.parse(body);
    } catch(err) {
      if(!err) err = 'invalidJSON';
    }
  } else {
    json = body;
  }

  if(!err) {
    cb(null, json);
  } else {
    cb(err || (res && res.statusCode) || 'error', json || body);
  }
}

module.exports = ElasticSearchDriver;
