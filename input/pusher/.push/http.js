'use strict'

const path = require('path'),
			App = require('node-express-app'),
			http = require('http');
			
var debug = require('debug')('Server:App:Pipeline:Input:Pusher:Push:Http');
var debug_events = require('debug')('Server:App:Pipeline:Input:Pusher:Push:Http:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Pusher:Push:Http:Internals');

module.exports = new Class({
  Extends: App,
  
  ON_CONNECT: 'onConnect',
  ON_CONNECT_ERROR: 'onConnectError',
  
  server: null,
  
  options: {
		id: '',
		path: '/',
		
		authentication: null,
		
		logs: null,
		//logs: {
			//loggers: {
				//error: null,
				//access: null,
				//profiling: null
			//},
			
			//path: './logs',
			
			////default: [
				////{ transport: winston.transports.Console, options: { colorize: 'true', level: 'warning' } },
				////{ transport: winston.transports.File, options: {level: 'info', filename: null } }
			////]
		//},
		
		authorization: null,
		
		params: null,
		
		middlewares: null,
	  
		routes: {
			all: [
				{
					path: '',
					callbacks: ['404'],
					version: '',
				},
			]
		},
			
		api: {
			routes: {
				all: [
					{
						path: '',
						callbacks: ['404'],
						version: '',
					},
				]
			},
		},
	},
  use: function(mount, app){
		debug('use instanceOf(app, ExpressApp) %o', instanceOf(app, App));
		
		if(instanceOf(app, App) === true)
			this.parent(mount, app);
	},
	//use: function(mount, app){
		//debug('use %o', app);
		
		//var id = Object.keys(this._flatten_obj(mount))[0];
		
		//app.options.id = id;
		
		//app.addEvent(app.ON_CONNECT_ERROR, function(err){
			//debug_events('app.ON_CONNECT_ERROR %o', err);
			
			//this.fireEvent(this.ON_CONNECT_ERROR);
		//}.bind(this));
											
		//this.parent(mount, app);
	//},
  initialize: function(options){
		
		
		//options = options || {};
		//options = Object.merge(Conf, options);
		
		this.parent(options);//override default options
		
		if(this.logs)
			this.profile('InputPusherHttp_init');//start profiling
		
		var first_connect = function(){
			debug_internals('first_connect');
			var addr = this.server.address();
			debug_internals('first_connect addr %o', addr);
				var bind = typeof addr === 'string'
				? addr
				: addr.address+':'+addr.port+'['+addr.family+']';
				
				this.options.id = bind;//set ID
				this.load(path.join(__dirname, '../../../../../apps'));
			
		}.bind(this);
		
		this.addEvent(this.ON_CONNECT, function(result){
			debug_events('this.ON_CONNECT');
			first_connect(result);
		});
		
		this.addEvent(this.ON_CONNECT, function(){this.removeEvent(this.ON_CONNECT, first_connect)});
		
		this.app.set('port', this.options.port);
		this.server = http.createServer(this.app);


		
		this.server.on('error', function(err){
			debug_internals('httpServer err %o', err);
			this.fireEvent(this.ON_CONNECT_ERROR, error);
		}.bind(this));
		
		this.server.on('listening', function(){
			var addr = this.server.address();
			var bind = typeof addr === 'string'
				? 'pipe ' + addr
				: 'port ' + addr.port;
			
			debug_internals('Listening on %s', bind);
			
			this.fireEvent(this.ON_CONNECT);
		}.bind(this));
		
		if(this.logs){
			this.profile('InputPusherHttp_init');//end profiling
			this.log('InputPusherHttp', 'info', 'InputPusherHttp started');
		}
		
		debug_internals('this.initialize');
  },
  connect: function(){
		debug_internals('this.connect');
		
		try{
			//this.os.api.get({uri: 'hostname'});
			//this.api.get({uri: ''});
			this.server.listen(this.options.port);
		}
		catch(e){
			debug_internals('Server Listen err %o', e);
		}
	}
	
});


