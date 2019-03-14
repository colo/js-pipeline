'use strict'

//var App = require('node-app-http-client'),
const path = require('path');
			//Http = require('./http'),
			//Cradle = require('./cradle'),
			//Munin = require('./munin'),
			//Imap = require('./imap');

var debug = require('debug')('Server:App:Pipeline:Input:Poller:Poll');

module.exports = new Class({

	initialize: function(options){

		debug('initialize %o', options);
		let instance = undefined

		if(options.instance){
			instance = options.instance
		}
		else if (options.module){
			 instance = new options.module(options)
			 options.module = instance
		 }


		return instance
		//switch(options.scheme) {
				//case 'http':
				//case 'https':
						//return new Http(options);
						//break;

				//case 'puochdb':
				//case 'cuochdb':
				//case 'cradle':
						//return new Cradle(options);
						//break;

				//case 'munin':
						//return new Munin(options);
						//break;

				//case 'imap':
						//return new Imap(options);
						//break;

				//default:
						//throw new Error('Unkown scheme')
		//}
	}
});
