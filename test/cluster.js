/**
 * New node file
 */

/**
 * Provide an object defining configuration of your vertica test machine.
 */

var vertica = require('./vertica-login');

var config = {
	
	cluster : {
		max: 5,
		min: 1, 
		idleTimeoutMillis: 5000,
		log: false
	},
	global : {
		host: 'default-host',
		user: 'default-user',
		password: 'default-password',
		database: 'default-db'	
	},
	pools : {
		pool1 : {
			config : vertica,
			nodes : [ {}, {}]
		},
		pool2 : {
			config : vertica,
			nodes : [ {}, {}]
		}
	}
}

var cluster = require('../index.js');

describe('Cluster', function() {
	this.timeout(60000);
	
	it('Should accept config', function(done) {
		try {
			var c = new cluster(config);
			done();
		} catch(e) {
			done(new Error(e.message));
		}
	});

	it('Should return connection', function(done) {
		try {
			var c = new cluster(config);
			c.get('pool1', function(err, conn) {
				if(err) {done(err)};
				done();
			})	
		} catch(e) {
			done(e.message);
		}
	});
	
	it('Should start up a pool', function(done) {
		try{
			var c = new cluster(config);
			c.startup('pool1', function(err) {
				if(err) {
					done(err)
				} else {
					done();
				}
			});
		} catch(e) {
			done(e.message);
		}
	});
	describe('Pool Operations', function() {
		var c;
		before(function(done) {
			c = new cluster(config);
			done();
		})
		it('Should return a pool', function(done) {
			c.getPool('pool1', function(err, pool) {
				if(err) {
					done(err);
				} else {
					done();
				}
			})
		});
		
		before(function(done) {
			c.startup('pool1', function(err) {
				if(err) {
					done(err);
				} else {
					done();
				}
			})
		});
		
		it('Should return a started pool', function(done) {
			c.getPool('pool1', function(err, pool) {
				if(err) {
					done(err);
				} else {
					if(pool.hasStarted()) {
						done();
					} else {
						done(new Exception('Incorrect pool state returned'));
					}
				}
			});
		});
	})
	
	it('Should return a connection quickly after starting up the pool', function(done) {
		try{
			var c = new cluster(config);
			c.startup('pool1', function(err) {
				var t = Date.now();
				c.get('pool1', function(err, conn) {
					var p = Date.now();
					if(err) {
						done(err)
					} else {
						if(t - p > 100) {
							done(new Error('Delay too big for connection retrival'));
						} else {
							done();
						}
					}
				})
			});
		} catch(e) {
			done(e.message);
		}
	})
	
	describe('Connection manipulation block', function() {
		var c;
		before(function(done) {
			c = new cluster(config);
			c.startup('pool1', function(err) {
				if(err) {
					done(err);
				} else {
					done();
				}
			});
		});
		it('Should release a connection', function(done) {			
			c.get('pool1', function(err, conn) {
				conn.release(function(err) {
					if(err) {
						done(err);
					} else {
						done();
					}
				})
			})
		});		
	})
})
