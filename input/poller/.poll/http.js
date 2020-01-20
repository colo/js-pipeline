'use strict'

const path = require('path'),
			App = require('node-app-http-client');


const Conf = process.env.NODE_ENV === 'production'
      ? require('../../../../../config/poll/http/prod.conf')
      : require('../../../../../config/poll/http/dev.conf');

var debug = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Http');
var debug_events = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Http:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Http:Internals');

module.exports = new Class({
  Extends: App,


  get: function(err, resp, body){
		debug('this.get %o', body);

		if(err){
			debug('this.get error %o', err);
			//this.fireEvent(this.ON_CONNECT_ERROR, err);
		}
  },
  //post: function(err, resp, body){
  //},
  /**
   * http://stackoverflow.com/questions/19098797/fastest-way-to-flatten-un-flatten-nested-json-objects
   *
   * */
  _flatten_obj: function(data) {
		debug_internals('_flatten_obj %o', data);

		var result = {};
		function recurse (cur, prop) {
			if (Object(cur) !== cur) {
					result[prop] = cur;
			} else if (Array.isArray(cur)) {
					 for(var i=0, l=cur.length; i<l; i++)
							 recurse(cur[i], prop + "[" + i + "]");
					if (l == 0)
							result[prop] = [];
			} else {
					var isEmpty = true;
					for (var p in cur) {
							isEmpty = false;
							recurse(cur[p], prop ? prop+"."+p : p);
					}
					if (isEmpty && prop)
							result[prop] = {};
			}
		}

		recurse(data, "");
		return result;
	},
	use: function(mount, app){
		debug('use %o %o', mount, app);

		var id = Object.keys(this._flatten_obj(mount))[0];

		app.options.id = id;
		
		app.addEvent(app.ON_CONNECT_ERROR, function(err){
			debug_events('app.ON_CONNECT_ERROR %o', err);

			this.fireEvent(this.ON_CONNECT_ERROR);
		}.bind(this));

		//throw new Error();
		this.parent(mount, app);
	},
  initialize: function(options){
		options = options || {};
		options = Object.merge(Conf, options);

		this.parent(options);//override default options

		this.profile('root_init');//start profiling

		/*this.addEvent(this.ON_USE, function(mount, app){
			////console.log('poll.ON_USE');
			////console.log(Object.keys(this._flatten_obj(mount))[0]);
			//app.ON_CONNECT = 'onConnect';
			//app.ON_CONNECT_ERROR = 'onConnectError';

			/*if(app.ON_CONNECT_ERROR){
				app.addEvent(app.ON_CONNECT_ERROR, function(err){

					////console.log('app.ON_CONNECT_ERROR');

					this.fireEvent(this.ON_CONNECT_ERROR, err);

				}.bind(this));
			}
		}.bind(this));*/

		//this.load(path.join(__dirname, '/apps'));

		//this.os.addEvent('onHostname', function(hostname){
			//////console.log('poll.os.onHostname');

			//this.id = hostname;
			//this.fireEvent(this.ON_CONNECT, hostname);
		//}.bind(this));

		//this.os.addEvent('onHostnameError', function(err){
			//////console.log('poll.os.onHostError');

			//this.fireEvent(this.ON_CONNECT_ERROR, err);
		//}.bind(this));

		var first_connect = function(result){
			debug_internals('first_connect %o', result);

			/**
			 * test for a P|CouchDB
			 *
			 * */
			//var db = new RegExp(/(p|c)ouchdb/g);
			//if(db.test(result.body)){
				//options.id = JSON.decode(result.body).uuid;
				//return new CradlePoll(options);
			//}
			///**
			 //* test for a P|CouchDB
			 //*
			 //* */
			//else{
				this.options.id = JSON.decode(result.body).id;//set ID

				if(Array.isArray(this.options.load)){
					Array.each(this.options.load, function(app){
						this.load(path.join(process.cwd(), app));
					}.bind(this));
				}
				else if(this.options.load){
					this.load(path.join(process.cwd(), this.options.load));
				}

				//this.load(path.join(__dirname, '../../../../../apps'));
			//}

		}.bind(this);

		this.addEvent(this.ON_CONNECT, function(result){
			debug_events('this.ON_CONNECT');
			first_connect(result);
		});

		this.addEvent(this.ON_CONNECT, function(){this.removeEvent(this.ON_CONNECT, first_connect)});

		this.profile('root_init');//end profiling

		this.log('root', 'info', 'root started');
  },
  connect: function(){
		debug('this.connect');
		try{
			//this.os.api.get({uri: 'hostname'});
			this.api.get({uri: ''});
		}
		catch(e){
			////console.log(e);
		}
	}

});


//let first_connect = function(result){

		//};

		//if(instance != null && options.load){
			//////console.log(process.cwd());
			//this.addEvent(instance.ON_CONNECT, function(result){
				//debug_events('instance.ON_CONNECT');
				//first_connect(result);
			//});

			//this.addEvent(instance.ON_CONNECT, function(){this.removeEvent(instance.ON_CONNECT, first_connect)});

		//}

		//return instance;
