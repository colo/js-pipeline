'use strict'

//var App = require('node-app-http-client'),
const path = require('path');

const App = require('node-app-socket.io-client/index');

var debug = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Socket.io');
var debug_events = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Socket.io:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Socket.io:Internals');

module.exports = new Class({
	Extends: App,

	options: {

	  requests : {
			once: [],
			periodical: [],
			range: [],
			//monitor: [],
			//config: [],
		},

		id: '',
		path: '',

		authentication: {
			username: 'test',
			password: '123',
			// sendImmediately: false,
		},

		logs: {
			loggers: {
				error: null,
				access: null,
				profiling: null
			},

			path: './logs',

			//default: [
				//{ transport: winston.transports.Console, options: { colorize: 'true', level: 'warning' } },
				//{ transport: winston.transports.File, options: {level: 'info', filename: null } }
			//]
		},

		//authorization: {
			//config: path.join(__dirname,'../../../../../config/rbac.json'),
		//},

		routes: {
			// info: [
			// 	{
			// 		path: '',
			// 		callbacks: ['info'],
			// 	}
			// ],
			//get: [
				//{
				//path: ':database/:cache',
				//callbacks: ['get'],
				//version: '',
				//},
			//]
		},

		//api: {
			//version: '1.0.0',

			//routes: {
				//info: [
					//{
					//path: '',
					//callbacks: ['exists'],
					//version: '',
					//},
				//]
			//},
		//},
  },
  //get: function(err, resp){
		////console.log('---GET RESP---');
		////console.log(resp);
	//},
  // info: function(err, resp){
	// 	debug('this.info %o', resp);
  //
	// 	//console.log('---INFO RESP---');
	// 	//this.get({uri: 'dashboard/cache', doc: 'localhost.colo.os.blockdevices@1515636560970'});
	// 	//console.log(resp);
	// 	if(err){
	// 		debug('this.info error %o', err);
	// 		//this.fireEvent(this.ON_CONNECT_ERROR, err);
	// 	}
	// },
  //exists: function(err, resp, body, req){
	//},
	initialize: function(options){
		//options = options || {};
		//options.scheme = 'http';

		this.parent(options);//override default options

		this.profile('socket.io_init');//start profiling

		var first_connect = function(){
			debug_internals('first_connect %o', this.io);

			//console.log('Socket.ioPoll.ON_CONNECT');
			//console.log(result);

			/**
			 * test for a P|CouchDB
			 *
			 * */

			/**
			 * test for a P|CouchDB
			 *
			 * */
			else{
				this.options.id = this.io.id

				if(Array.isArray(this.options.load)){
					Array.each(this.options.load, function(app){
						this.load(path.join(process.cwd(), app));
					}.bind(this));
				}
				else if(this.options.load){
					this.load(path.join(process.cwd(), this.options.load));
				}

				this.addEvent(this.ON_USE_APP, function(mount, app){
					debug_events('this.ON_USE_APP %o', app);

					app.addEvent(app.ON_CONNECT_ERROR, function(err){

						debug_events('app.ON_CONNECT_ERROR %o', err);

						this.fireEvent(this.ON_CONNECT_ERROR, err);
					}.bind(this))
				}.bind(this))

				this.load(path.join(__dirname, '../../../../../apps'));
			}

		}.bind(this);

		this.addEvent(this.ON_CONNECT, function(){
			debug_events('this.ON_CONNECT');
			first_connect();
		});

		this.addEvent(this.ON_CONNECT, function(){this.removeEvent(this.ON_CONNECT, first_connect)});

		//this.addEvent('onRange', function(req){
			////console.log('crale client->onRange');
		//});

		this.profile('socket.io_init');//end profiling

		this.log('socket.io', 'info', 'socket.io started');
  },
  // connect: function(){
	// 	// debug('this.connect');
  //   //
	// 	// try{
	// 	// 	//this.os.api.get({uri: 'hostname'});
	// 	// 	//this.api.get({uri: ''});
	// 	// 	//this.get({uri: 'dashboard/cache', id: 'localhost.colo.os.blockdevices@1515636560970'});
	// 	// 	////console.log(this.info.attempt());
	// 	// 	// this.info();
	// 	// }
	// 	// catch(e){
	// 	// 	//console.log(e);
	// 	// }
	// }
});
