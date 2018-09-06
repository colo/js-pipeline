'use strict'

//var App = require('node-app-http-client'),
const path = require('path');

const App = require('node-app-imap-client');

var debug = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Imap');
var debug_events = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Imap:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Imap:Internals');

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
			sendImmediately: false,
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
		
		authorization: {
			config: path.join(__dirname,'../../../../../config/rbac.json'),
		},
		
		routes: {
			//connect: [
				//{
					//callbacks: ['connect'],
				//}
			//],
			
		},
			
		
  },
  
  //connect: function(resp, options){
		//debug('this.connect %o', resp);
		//debug('this.connect options %o', options);
		
		////if(err){
			////debug('this.version error %o', err);
			////this.fireEvent(this.ON_CONNECT_ERROR, err);
		////}
	//},
	initialize: function(options){
		debug('init %o', options);
		
		this.parent(options);//override default options
		
		debug('init %o', options);
		
		this.profile('imap_init');//start profiling
		
		var first_connect = function(resp){
			debug_internals('first_connect %o', resp);
			
			this.options.id = resp.host+'.'+resp.user;
			
			//this.load(path.join(__dirname, '../../../../../apps'));
			if(Array.isArray(this.options.load)){
				Array.each(this.options.load, function(app){
					this.load(path.join(process.cwd(), app));
				}.bind(this));
			}
			else if(this.options.load){
				this.load(path.join(process.cwd(), this.options.load));
			}
			
		}.bind(this);
		
		this.addEvent(this.ON_CONNECT, function(result){
			debug_events('this.ON_CONNECT');
			first_connect(result);
		});
		
		this.addEvent(this.ON_CONNECT, function(){this.removeEvent(this.ON_CONNECT, first_connect)});
		
					
		this.profile('imap_init');//end profiling
		
		this.log('imap', 'info', 'imap started');
  },
  //connect: function(){
		//debug('this.connect');
		
		//try{
			//this.connect();
		//}
		//catch(e){
			////console.log(e);
		//}
	//}
});
