'use strict'

const path = require('path'),
			Push = require('../index')

const debug = require('debug')('js-pipeline:Pusher'),
			debug_internals = require('debug')('js-pipeline:Pusher:Internals'),
			debug_events = require('debug')('js-pipeline:Pusher:Events')

module.exports = new Class({
	Implements: [Options, Events],

	ON_SUSPEND: 'onSuspend',
	ON_RESUME: 'onResume',
	ON_EXIT: 'onExit',

	ON_ONCE: 'onOnce',
	ON_RANGE: 'onRange',


	ON_CLIENT_CONNECT: 'onClientConnect',
	ON_CLIENT_CONNECT_ERROR: 'onClientConnectError',

	ON_DOC: 'onDoc',
	ON_DOC_ERROR: 'onDocError',

	ON_ONCE_DOC: 'onOnceDoc',
	ON_ONCE_DOC_ERROR: 'onOnceDocError',

	ON_DOC_SAVED: 'onDocSaved',

	ON_PERIODICAL_DOC: 'onPeriodicalDoc',
	ON_PERIODICAL_DOC_ERROR: 'onPeriodicalDocError',

	ON_RANGE_DOC: 'onRangeDoc',
	ON_RANGE_DOC_ERROR: 'onRangeDocError',

	// ON_ONCE_REQUESTS_UPDATED: 'onOnceRequestsUpdated',
	// ON_RANGE_REQUESTS_UPDATED: 'onRangeRequestsUpdated',
	// ON_PERIODICAL_REQUESTS_UPDATED: 'onPeriodicalRequestsUpdated',


	pushers: {},
	//connected pushers
	conn_pushers: {},

	//pushers that couldn't connect
	err_pushers: {},

	err_pusher_count: [],

	//db: null,
	options: {

		id: null,
		conn: [],

		connect_retry_count: 10,
		connect_retry_periodical: 5000,

		//requests: {
			//periodical: 1000,
		//},

		//docs:{
			//buffer_size: 10,
			//timer: 5, //seconds
		//}

	},

	initialize: function(options){

		this.setOptions(options);

		if(typeof(this.options.conn) != 'array' && this.options.conn instanceof Array && !Array.isArray(this.options.conn)){
			var pusher = this.options.conn;
			this.options.conn = [];
			this.options.conn.push(pusher);
		}

		//console.log(this.options);

		this.addEvent(this.ON_EXIT, function(){
			//console.log('this.ON_EXIT');
			debug_events('ON_EXIT');
			process.exit(0);
		}.bind(this));


	},
	connect: function(){
		var pushers = this.options.conn;

			Array.each(pushers, function(pusher, index){

				if(!this.conn_pushers || !this.conn_pushers[index]){//to prevent multiples ON_ERROR trying to reconnect

					if(!this.err_pusher_count[index])
						this.err_pusher_count[index] = 0;

					var push = null;

					//checks if the 'pusher app' already failed, else create a new one
					if(this.err_pushers && this.err_pushers[index]){

						debug('Connect->err_pushers %d', index);

						push = this.err_pushers[index];
					}
					else{

						debug('Connect->create pusher %o', pusher);

						push = new Push(pusher);

						push['ON_DOC'] = this.ON_DOC;
						push['ON_DOC_ERROR'] = this.ON_DOC_ERROR;

						push['ON_ONCE'] = this.ON_ONCE;
						push['ON_RANGE'] = this.ON_RANGE;

						push['ON_PERIODICAL_DOC'] = this.ON_PERIODICAL_DOC;
						push['ON_PERIODICAL_DOC_ERROR'] = this.ON_PERIODICAL_DOC_ERROR;

						push['ON_ONCE_DOC'] = this.ON_ONCE_DOC;
						push['ON_ONCE_DOC_ERROR'] = this.ON_ONCE_DOC_ERROR;

						push['ON_DOC_SAVED'] = this.ON_DOC_SAVED;

						push['ON_RANGE_DOC'] = this.ON_RANGE_DOC;
						push['ON_RANGE_DOC_ERROR'] = this.ON_RANGE_DOC_ERROR;

						//this.addEvent(this.ON_ONCE, function(req){
							//debug_events('ON_ONCE %o', req);
							//push.fireEvent(push.ON_ONCE, req);
						//}.bind(this));

						//this.addEvent(this.ON_RANGE, function(req){
							//debug_events('ON_RANGE %o', req);
							//push.fireEvent(push.ON_RANGE, req);
						//}.bind(this));
						push.addEvent(push.ON_DOC, function(doc, options){
 							debug_events('push.ON_DOC %o', arguments);
 							this.fireEvent(this.ON_DOC, [doc, options]);
 						}.bind(this));

 						push.addEvent(push.ON_DOC_ERROR, function(doc, options){
 							debug_events('push.ON_DOC_ERROR %o', arguments);
 							this.fireEvent(this.ON_DOC_ERROR, [doc, options]);
 						}.bind(this));

						push.addEvent(push.ON_ONCE_DOC, function(doc, options){
							debug_events('push.ON_ONCE_DOC %o', arguments);
							this.fireEvent(this.ON_ONCE_DOC, [doc, options]);
						}.bind(this));

						push.addEvent(push.ON_ONCE_DOC_ERROR, function(doc, options){
							debug_events('push.ON_ONCE_DOC_ERROR %o', arguments);
							this.fireEvent(this.ON_ONCE_DOC_ERROR, [doc, options]);
						}.bind(this));

						push.addEvent(push.ON_PERIODICAL_DOC, function(doc, options){
							debug_events('push.ON_PERIODICAL_DOC %o', arguments);
							this.fireEvent(this.ON_PERIODICAL_DOC, [doc, options]);

						}.bind(this));

						push.addEvent(push.ON_PERIODICAL_DOC_ERROR, function(doc, options){
							debug_events('push.ON_PERIODICAL_DOC_ERROR %o', arguments);
							this.fireEvent(this.ON_PERIODICAL_DOC_ERROR, [doc, options]);

						}.bind(this));


						push.addEvent(push.ON_RANGE_DOC, function(doc, options){
							debug_events('push.ON_RANGE_DOC %o', arguments);
							this.fireEvent(this.ON_RANGE_DOC, [doc, options]);
						}.bind(this));

						push.addEvent(push.ON_RANGE_DOC_ERROR, function(doc, options){
							debug_events('push.ON_RANGE_DOC_ERROR %o', arguments);
							this.fireEvent(this.ON_RANGE_DOC_ERROR, [doc, options]);
						}.bind(this));


						push.addEvent(push.ON_USE, function(mount, app){
							debug_events('push.ON_USE %o', app);

							app['ON_DOC'] = this.ON_DOC;
							app['ON_DOC_ERROR'] = this.ON_DOC_ERROR;

							app['ON_ONCE_DOC'] = this.ON_ONCE_DOC;
							app['ON_ONCE_DOC_ERROR'] = this.ON_ONCE_DOC_ERROR;

							app['ON_PERIODICAL_DOC'] = this.ON_PERIODICAL_DOC;
							app['ON_PERIODICAL_DOC_ERROR'] = this.ON_PERIODICAL_DOC_ERROR;

							app['ON_RANGE_DOC'] = this.ON_RANGE_DOC;
							app['ON_RANGE_DOC_ERROR'] = this.ON_RANGE_DOC_ERROR;


							app.addEvent(this.ON_DOC, function(doc){
								debug_events('app.ON_DOC %o', doc);
								this.fireEvent(this.ON_DOC, [doc, {type: 'push', input_type: push, app: app}]);
							}.bind(this));

							app.addEvent(this.ON_DOC_ERROR, function(err){
								debug_events('app.ON_DOC_ERROR %o', err);
								this.fireEvent(this.ON_DOC_ERROR, [err, {type: 'push', input_type: push, app: app}]);
							}.bind(this));

							app.addEvent(this.ON_ONCE_DOC, function(doc){
								debug_events('app.ON_ONCE_DOC %o', doc);
								this.fireEvent(this.ON_ONCE_DOC, [doc, {type: 'once', input_type: push, app: app}]);
							}.bind(this));

							app.addEvent(this.ON_PERIODICAL_DOC, function(doc){
								debug_events('app.ON_PERIODICAL_DOC %o', doc);
								this.fireEvent(this.ON_PERIODICAL_DOC, [doc, {type: 'periodical', input_type: push, app: app}]);
							}.bind(this));

							app.addEvent(this.ON_RANGE_DOC, function(doc){
								debug_events('app.ON_RANGE_DOC %o', doc);
								this.fireEvent(this.ON_RANGE_DOC, [doc, {type: 'range', input_type: push, app: app}]);
							}.bind(this));

						}.bind(this));

						push.addEvent(push.ON_CONNECT, function(result){
							debug_events('push.ON_CONNECT %o', result);

							this._register_pusher(index, push);
						}.bind(this));

						push.addEvent(push.ON_CONNECT_ERROR, function(err){
							debug_events('push.ON_CONNECT_ERROR %o', err);
							this._register_error(index, push);
						}.bind(this));


					}

					try{

						if(this.options.connect_retry_count < 0 || this.err_pusher_count[index] < this.options.connect_retry_count){
							//push.os.api.get({uri: 'hostname'});
							debug_internals('Trying to connect %o', push);
							push.connect();
						}

					}
					catch(e){
						debug_internals('Error connecting: %o', e);
					}

				}//
			}.bind(this));

		//}.bind(this));
	},
	//dispatch_range: function(req, app, app_req, type){
		//if(req.get('Range')){

			////var type = req.get('Range').trim().split(' ', 1)[0];
			////var type = req.get('Range').trim().split(' ', 1)[0];

			//var words = /(\w+)(?:\s)(\w+)(?:-)(\w+)/g;
			//var match = words.exec(req.get('Range').trim());
			//var type = match[1].toLowerCase();

			////console.log('--HEADER---');
			////console.log(match);

			//var start, end;

			//switch (type){
				//case 'date':
				//case 'utc':
					//var date = /^(\d\d\d\d)?(\d\d)?(\d\d)?(\d\d)?(\d\d)?(\d\d)?(\d\d)?$/;
					//start = date.exec(match[2]);
					//end = date.exec(match[3]);

					////console.log(start);
					////console.log(end);

					//start.forEach(function(value, i){
						//if(!value)
							//start[i] = 0;
					//});

					//end.forEach(function(value, i){
						//if(!value)
							//end[i] = 0;
					//});

					//if(type == 'utc'){
						//start = new Date(
							//Date.UTC(start[1], start[2], start[3], start[4], start[5], start[6], start[7])
						//).getTime();

						//end = new Date(
							//Date.UTC(end[1], end[2], end[3], end[4], end[5], end[6], end[7])
						//).getTime();
					//}
					//else{
						//start = new Date(start[1], start[2], start[3], start[4], start[5], start[6], start[7]).getTime();
						//end = new Date(end[1], end[2], end[3], end[4], end[5], end[6], end[7]).getTime();
					//}


				//break;

				//case 'posix':
				//case 'epoch':
					//start = parseInt(match[2]);
					//end = parseInt(match[3]);

				//break;

				//default:
					//throw new Error('Type ['+type+'] not implemented');

			//};


			//req['opt'] = {
				//range: {
					//type: type,
					//start: start,
					//end: end
				//},
			//};
		//}
		//else{
			//throw new Error('No Range header!');
		//}

		//debug_internals('dispatch_range %o', req['opt']);

		//return this.dispatch(req, app, app_req, type);
	//},
	//dispatch: function(req, app, app_req, type){
		//debug_internals('->dispatch %o', type);

		//var key = Object.keys(app_req)[0];
		//var value = Object.values(app_req)[0];

		//var verb = (key == 'api') ? Object.keys(value)[0] : key;
		//var data = (key == 'api') ? Object.values(value)[0] : value;
		////var original_app = app;

		//var dispatch_app = (key == 'api') ? app.api : app;

		//var req_client = (req == null) ? null : {
			//agent: req.headers['user-agent'],
			//ip: req.ip
		//};

		////req_client = req_client || null;
		////console.log(req_client);
		////console.log(data);

		//app.options.requests.current = {};
		//app.options.requests.current.type = type;
		//app.options.requests.current.client = req_client;

		//if(data instanceof Function){//this way we may run PRE-FUNCTION for complex requests
			//debug_internals('dispatch->function %o', [req, dispatch_app[verb]]);

			//data.attempt([req, dispatch_app[verb]], app);
		//}
		//else{
			//debug_internals('dispatch %o', dispatch_app[verb]);
			//dispatch_app[verb](data);
		//}

	//},
	_register_pusher: function(index, push){
		//console.log('CONNECTED hostname');
		//console.log(index);
		////console.log(push);

		//this.conn_pushers[push_id+'.'+hostname] = push;
		if(!this.conn_pushers)
			this.conn_pushers = [];

		this.conn_pushers[index] = push;

		if(this.err_pusher_count)
			delete this.err_pusher_count[index];

		if(this.err_pushers)
			delete this.err_pushers[index];

		debug_internals('_register_pusher %o', push);

		this.fireEvent(this.ON_CLIENT_CONNECT, push);

	},
	_register_error: function(index, push){
		//console.log('DISCONNECTED hostname');
		//console.log(index);
		//console.log(this.options.conn);

		this.err_pusher_count[index] += 1;

		var err_pusher = {};
		err_pusher = [];
		err_pusher[index] = this.options.conn[index];
		//err_pusher.push(pusher);

		if(!this.err_pushers)
			this.err_pushers = [];

		if(this.err_pushers[index] != push){
			this.err_pushers[index] = push;

			var reconnect_timer = null;

			var reconnect = function(err_pusher){
				//console.log('RE CONNECTING');
				//console.log(err_pusher);

				reconnect_timer = this.connect.periodical(this.options.connect_retry_periodical, this, err_pusher);
			}.bind(this);


			var reconnect_stop = function(){clearInterval(reconnect_timer)}.bind(this);

			process.on('exit', (code) => reconnect_stop);

			//this.addEvent(this.ON_SUSPEND, reconnect_stop);

			//push.addEvent(push.ON_CONNECT_ERROR, stop);

			//this.addEvent(this.ON_RESUME, reconnect_start);

			push.addEvent(push.ON_CONNECT, function(){
				debug_events('push.ON_CONNECT');
				reconnect_stop();
			});

			reconnect(err_pusher);
		}

		if(this.conn_pushers)
			delete this.conn_pushers[index];

		debug_internals('_register_error %o', push);


		this.fireEvent(this.ON_CLIENT_CONNECT_ERROR, err_pusher);
	},


});
