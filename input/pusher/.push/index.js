'use strict'

//var App = require('node-app-http-client'),
const path = require('path');
			//Http = require('./http');


var debug = require('debug')('Server:App:Pipeline:Input:Pusher:Push');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Pusher:Push:Internals');


module.exports = new Class({

	initialize: function(options){

		debug('initialize %o', options);

		return new options.module(options);

		//switch(options.scheme) {
				//case 'http':
				//case 'https':
						//return new Http(options);
						//break;

				//default:
						//throw new Error('Unkown scheme')
		//}
	}
});
