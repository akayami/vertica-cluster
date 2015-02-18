/**
 * New node file
 */

//var poolModule = require('generic-pool');
var itempool = require('item-pool');
var merge = require('merge');
var vertica = require('vertica');

module.exports = function(config) {
	
	function wrapper() {
		var pools = {};
		for(var pool in config.pools) {
			var cfg = merge(true, config.cluster);
			cfg.create = function(callback) {
				var cfg = merge(true, config.global, config.pools[pool].config);
				var conn = vertica.connect(cfg);
				conn.query("set autocommit='off'", function(err, result) {
					if(err) {
						callback(err);
					} else {
						callback(null, conn);
					}
				})
			},
			cfg.acquire =  function(err, item, cb) {
				cb(null, item);		
			},
			cfg.release =  function(err, item, cb) {
				cb(null, item);		
			},
			cfg.destroy = function(err, conn, callback) {
				conn.disconnect();
				callback(null, conn);
			}
			pools[pool] = new itempool(cfg);
		}
		
		
		this.startup = function(pool, callback) {
			if(pools[pool]) {
				pools[pool].startup(callback)
			} else {
				callback(new Error("Specified pool does not exist"));
			}
		}

		this.get = function(pool, callback) {		
			pools[pool].acquire(function(err, resource) {
				if(err) {
					callback(err, null)
				} else {					
					callback(null, resource);
				}
			});
		}
		
		this.getPool = function(pool, callback) {
			if(pools[pool]) {
				callback(null, pools[pool]);
			} else {
				callback(new Error("Specified pool does not exist"));
			}
		}

		this.shutdown = function() {
			for(var pool in pools) {
				pools[pool].destroyAllNow();
			}
		}
	
	}
	return new wrapper();
};