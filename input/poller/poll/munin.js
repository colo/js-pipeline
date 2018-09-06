'use strict'

//var App = require('node-app-http-client'),
const path = require('path');

const App = require('node-app-munin-client');

var debug = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Munin');
var debug_events = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Munin:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Poller:Poll:Munin:Internals');

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
			version: [
				{
					callbacks: ['version'],
				}
			],
			
		},
			
		
  },
  
  version: function(resp, options){
		debug('this.version %o', resp);
		debug('this.version options %o', options);
		
		//if(err){
			//debug('this.version error %o', err);
			//this.fireEvent(this.ON_CONNECT_ERROR, err);
		//}
	},
	initialize: function(options){
		debug('init %o', options);
		
		this.parent(options);//override default options
		
		debug('init %o', options);
		
		this.profile('munin_init');//start profiling
		
		var first_connect = function(resp){
			var result = resp.response;
			
			debug_internals('first_connect %o', result);
			
			//this.options.id = result.node+'.'+result.version;
			this.options.id = result.node;
			this.options.version = result.version;
			
			//this.addEvent(this.ON_USE_APP, function(mount, app){
				//debug_events('this.ON_USE_APP %o', app);
				
				//app.addEvent(app.ON_CONNECT_ERROR, function(err){
					
					//debug_events('app.ON_CONNECT_ERROR %o', err);
					
					//this.fireEvent(this.ON_CONNECT_ERROR, err);
				//}.bind(this))
			//}.bind(this))
			
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
		
					
		this.profile('munin_init');//end profiling
		
		this.log('munin', 'info', 'munin started');
  },
  connect: function(){
		debug('this.connect');
		
		try{
			this.version();
		}
		catch(e){
			//console.log(e);
		}
	}
});
