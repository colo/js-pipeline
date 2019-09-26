'use strict'

var path = require('path'),
		mootools = require('mootools'),
		Poll = require('./poll');

var debug = require('debug')('Server:App:Pipeline:Input:Poller');
var debug_internals = require('debug')('Server:App:Pipeline:Input:Poller:Internals');
var debug_events = require('debug')('Server:App:Pipeline:Input:Poller:Events');

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

	ON_ONCE_REQUESTS_UPDATED: 'onOnceRequestsUpdated',
	ON_RANGE_REQUESTS_UPDATED: 'onRangeRequestsUpdated',
	ON_PERIODICAL_REQUESTS_UPDATED: 'onPeriodicalRequestsUpdated',

	//ON_CONFIG_DOC: 'onConfigDoc',
	//ON_CONFIG_DOC_ERROR: 'onConfigDocError',

	//ON_MONITOR_DOC: 'onMonitorDoc',
	//ON_MONITOR_DOC_ERROR: 'onMonitorDocError',

	pollers: {},
	//connected pollers
	conn_pollers: {},

	//pollers that couldn't connect
	err_pollers: {},

	err_poller_count: [],

	periodicals: {},

	//db: null,
	options: {

		suspended: false,
		id: null,
		conn: [],

		connect_retry_count: 10,
		connect_retry_periodical: 5000,

		requests: {
			periodical: 1000,
		},

		docs:{
			buffer_size: 10,
			timer: 5, //seconds
		}

	},

	initialize: function(options){

		this.setOptions(options);

		//console.log('Input->Poller->suspended', this.options.suspended)

		if(typeof(this.options.conn) != 'array' && this.options.conn instanceof Array && !Array.isArray(this.options.conn)){
			var poller = this.options.conn;
			this.options.conn = [];
			this.options.conn.push(poller);
		}

		//////console.log(this.options);

		this.addEvent(this.ON_EXIT, function(){
			//////console.log('this.ON_EXIT');
			debug_events('ON_EXIT');
			if(process && process.exit && typeof process.exit == 'function')//process.exit works only nodejs, not in browsers
				process.exit(0);

		}.bind(this));


	},
	//connect: function(connect_pollers){
	connect: function(){
		//////console.log('POLLER CONNECT');


		//////console.log(this.options.conn);
		var pollers = this.options.conn;

		//Object.each(connect_pollers, function(pollers, poll_id){

			//if(!this.err_poller_count[poll_id])
				//this.err_poller_count[poll_id] = [];

			Array.each(pollers, function(poller, index){
				////////console.log(poll_id);
				//////console.log(pollers);
				//////console.log(index);

				if(!this.conn_pollers || !this.conn_pollers[index]){//to prevent multiples ON_ERROR trying to reconnect
					//////console.log('DEBERÍA ENTRAR SOLO UNA VEZ!!!');
					//////console.log(index);


					if(!this.err_poller_count[index])
						this.err_poller_count[index] = 0;

					var poll = null;

					//checks if the 'poller app' already failed, else create a new one
					if(this.err_pollers && this.err_pollers[index]){
						//////console.log('errored poller');

						debug('Connect->err_pollers %d', index);

						poll = this.err_pollers[index];
					}
					else{
						//////console.log('create poller');

						debug('Connect->create poller %o', poller);

						poll = new Poll(poller);

						poll['ON_EXIT'] = this.ON_EXIT;
						poll['ON_SUSPEND'] = this.ON_SUSPEND;
						poll['ON_RESUME'] = this.ON_RESUME;

						poll['ON_DOC'] = this.ON_DOC;
						poll['ON_DOC_ERROR'] = this.ON_DOC_ERROR;

						poll['ON_ONCE'] = this.ON_ONCE;
						poll['ON_RANGE'] = this.ON_RANGE;

						poll['ON_PERIODICAL_DOC'] = this.ON_PERIODICAL_DOC;
						poll['ON_PERIODICAL_DOC_ERROR'] = this.ON_PERIODICAL_DOC_ERROR;

						poll['ON_ONCE_DOC'] = this.ON_ONCE_DOC;
						poll['ON_ONCE_DOC_ERROR'] = this.ON_ONCE_DOC_ERROR;

						poll['ON_DOC_SAVED'] = this.ON_DOC_SAVED;

						poll['ON_RANGE_DOC'] = this.ON_RANGE_DOC;
						poll['ON_RANGE_DOC_ERROR'] = this.ON_RANGE_DOC_ERROR;

						poll['ON_ONCE_REQUESTS_UPDATED'] = this.ON_ONCE_REQUESTS_UPDATED;
						poll['ON_RANGE_REQUESTS_UPDATED'] = this.ON_RANGE_REQUESTS_UPDATED;
						poll['ON_PERIODICAL_REQUESTS_UPDATED'] = this.ON_PERIODICAL_REQUESTS_UPDATED;

						this.addEvent(this.ON_EXIT, function(req){
							debug_events('ON_EXIT %o', req);
							if(Array.isArray(req))
								req = [req]

							poll.fireEvent(poll.ON_EXIT, req);
						}.bind(this));

						let __poll_suspend = function(req){
							debug_events('ON_SUSPEND %o', req);
							if(Array.isArray(req))
								req = [req]

							poll.fireEvent(poll.ON_SUSPEND, req);
						}.bind(this)

						this.addEvent(this.ON_SUSPEND, __poll_suspend);

						let __poll_resume = function(req){
							debug_events('ON_RESUME %o', req);
							if(Array.isArray(req))
								req = [req]

							poll.fireEvent(poll.ON_RESUME, req);
						}.bind(this)

						this.addEvent(this.ON_RESUME, __poll_resume);

						this.addEvent(this.ON_ONCE, function(req){
							debug_events('ON_ONCE %o', req);
							if(Array.isArray(req))
								req = [req]

							poll.fireEvent(poll.ON_ONCE, req);
						}.bind(this));

						this.addEvent(this.ON_RANGE, function(req){
							debug_events('ON_RANGE %o', req);
							// console.log('js-pipeline INPUT ON_RANGE', req)
							if(Array.isArray(req))
								req = [req]

							poll.fireEvent(poll.ON_RANGE, req);
						}.bind(this));

						this.addEvent(this.ON_DOC_SAVED, function(err, result){
							debug_events('ON_DOC_SAVED', err, result);
							// console.log('js-pipeline INPUT ON_RANGE', req)
							// if(Array.isArray(req))
							// 	req = [req]

							poll.fireEvent(poll.ON_DOC_SAVED, [err, result]);
						}.bind(this));

						/**
						 * Events from poll to Poller
						 * */

					 	// poll.addEvent(poll.ON_EXIT, function(){
						// 	debug_events('poll.ON_EXIT %o', arguments);
						// 	this.fireEvent(this.ON_EXIT, [arguments]);
						// }.bind(this));
						poll.addEvent(poll.ON_SUSPEND, function(){
 							debug_events('poll.ON_SUSPEND %o', arguments);
							this.removeEvent(this.ON_SUSPEND, __poll_suspend);

 							this.fireEvent(this.ON_SUSPEND, [arguments]);

							this.addEvent(this.ON_SUSPEND, __poll_suspend);


 						}.bind(this));

						poll.addEvent(poll.ON_RESUME, function(){
 							debug_events('poll.ON_RESUME %o', arguments);
							this.removeEvent(this.ON_RESUME, __poll_resume);

 							this.fireEvent(this.ON_RESUME, [arguments]);

							this.addEvent(this.ON_RESUME, __poll_resume);


 						}.bind(this));

						poll.addEvent(poll.ON_DOC, function(doc, options){
 							debug_events('poll.ON_DOC %o', arguments);
 							this.fireEvent(this.ON_DOC, [doc, options]);
 						}.bind(this));

 						poll.addEvent(poll.ON_DOC_ERROR, function(doc, options){
 							debug_events('poll.ON_DOC_ERROR %o', arguments);
 							this.fireEvent(this.ON_DOC_ERROR, [doc, options]);
 						}.bind(this));

						poll.addEvent(poll.ON_ONCE_DOC, function(doc, options){
							debug_events('poll.ON_ONCE_DOC %o', arguments);
							this.fireEvent(this.ON_ONCE_DOC, [doc, options]);
						}.bind(this));

						poll.addEvent(poll.ON_ONCE_DOC_ERROR, function(doc, options){
							debug_events('poll.ON_ONCE_DOC_ERROR %o', arguments);
							this.fireEvent(this.ON_ONCE_DOC_ERROR, [doc, options]);
						}.bind(this));

						poll.addEvent(poll.ON_PERIODICAL_DOC, function(doc, options){
							debug_events('poll.ON_PERIODICAL_DOC %o', arguments);
							this.fireEvent(this.ON_PERIODICAL_DOC, [doc, options]);

						}.bind(this));

						poll.addEvent(poll.ON_PERIODICAL_DOC_ERROR, function(doc, options){
							debug_events('poll.ON_PERIODICAL_DOC_ERROR %o', arguments);
							this.fireEvent(this.ON_PERIODICAL_DOC_ERROR, [doc, options]);

						}.bind(this));


						poll.addEvent(poll.ON_RANGE_DOC, function(doc, options){
							debug_events('poll.ON_RANGE_DOC %o', arguments);
							this.fireEvent(this.ON_RANGE_DOC, [doc, options]);
						}.bind(this));

						poll.addEvent(poll.ON_RANGE_DOC_ERROR, function(doc, options){
							debug_events('poll.ON_RANGE_DOC_ERROR %o', arguments);
							this.fireEvent(this.ON_RANGE_DOC_ERROR, [doc, options]);
						}.bind(this));

						poll.addEvent(poll.ON_ONCE_REQUESTS_UPDATED, function(){
								debug_internals('poll.ON_REQUESTS_UPDATED %o', poll.options.requests.once);
								// let key = poll.options.id;
								poll.removeEvents(poll.ON_ONCE)
								this._process_requests(poll, null, { once: poll.options.requests.once });

						}.bind(this));

						poll.addEvent(poll.ON_RANGE_REQUESTS_UPDATED, function(){
								debug_internals('poll.ON_REQUESTS_UPDATED %o', poll.options.requests.range);
								// let key = poll.options.id;
								poll.removeEvents(poll.ON_RANGE)
								this._process_requests(poll, null, { range: poll.options.requests.range });

						}.bind(this));

						poll.addEvent(poll.ON_PERIODICAL_REQUESTS_UPDATED, function(){
								debug_internals('poll.ON_PERIODICAL_REQUESTS_UPDATED %o', poll.options.requests.periodical);
								let key = poll.options.id;

								if(this.periodicals[key])//if exists, stop and delete all, and process again
									Array.each(this.periodicals[key], function(fn){
										const {type, timer} = fn;
										switch(type){
											case 'task':
												timer.stop();

											case 'timer':
												clearInterval(timer);

											default:
												delete this.periodicals[key];

										}

									}.bind(this));

								Array.each(poll.options.requests.periodical, function(app_req){
										this._process_periodical(poll, null, app_req, 'periodical');
								}.bind(this));

						}.bind(this));

						/**
						 * DOCS from poll to Poller
						 * */

						if(poll.options.requests){

							this._process_requests(poll, null, poll.options.requests);

						}

						poll.addEvent(poll.ON_USE, function(mount, app){
							debug_events('poll.ON_USE %o', app);


							app['ON_EXIT'] = this.ON_EXIT;
							poll.addEvent(poll.ON_EXIT, function(){
								debug_events('poll.ON_EXIT %o', arguments);
								app.fireEvent(this.ON_EXIT, [arguments]);
							}.bind(this));

							// app['ON_RESUME'] = this.ON_RESUME;
							// app['ON_SUSPEND'] = this.ON_SUSPEND;

							app['ON_DOC'] = this.ON_DOC;
							app['ON_DOC_ERROR'] = this.ON_DOC_ERROR;

							app['ON_ONCE_DOC'] = this.ON_ONCE_DOC;
							app['ON_ONCE_DOC_ERROR'] = this.ON_ONCE_DOC_ERROR;

							app['ON_PERIODICAL_DOC'] = this.ON_PERIODICAL_DOC;
							app['ON_PERIODICAL_DOC_ERROR'] = this.ON_PERIODICAL_DOC_ERROR;

							app['ON_RANGE_DOC'] = this.ON_RANGE_DOC;
							app['ON_RANGE_DOC_ERROR'] = this.ON_RANGE_DOC_ERROR;

							app['ON_ONCE_REQUESTS_UPDATED'] = this.ON_ONCE_REQUESTS_UPDATED;
							app['ON_RANGE_REQUESTS_UPDATED'] = this.ON_RANGE_REQUESTS_UPDATED;
							app['ON_PERIODICAL_REQUESTS_UPDATED'] = this.ON_PERIODICAL_REQUESTS_UPDATED;

							app['ON_ONCE'] = this.ON_ONCE;
							app['ON_RANGE'] = this.ON_RANGE;

							/**
							 * app can fire ON_ONCE and ON_RANGE events
							 * */

						 	// app.addEvent(app.ON_EXIT, function(req){
 							// 	debug_events('app.ON_EXIT %o', req);
 							// 	poll.fireEvent(poll.ON_EXIT, req);
 							// }.bind(this));
							// app.addEvent(app.ON_RESUME, function(req){
							// 	debug_events('app.ON_RESUME %o', req);
							// 	poll.fireEvent(poll.ON_RESUME, req);
							// }.bind(this));
							// app.addEvent(app.ON_SUSPEND, function(req){
							// 	debug_events('app.ON_SUSPEND %o', req);
							// 	poll.fireEvent(poll.ON_SUSPEND, req);
							// }.bind(this));

							app.addEvent(app.ON_ONCE, function(req){
								debug_events('app.ON_ONCE %o', req);
								poll.fireEvent(poll.ON_ONCE, req);
							}.bind(this));

							app.addEvent(app.ON_RANGE, function(req){
								debug_events('ON_RANGE %o', req);
								poll.fireEvent(poll.ON_RANGE, req);
							}.bind(this));
							/**
						 * app can fire ON_ONCE and ON_RANGE events
						 * */
						 app.addEvent(app.ON_ONCE_REQUESTS_UPDATED, function(){
							 debug_internals('app.ON_ONCE_REQUESTS_UPDATED %o', app.options.requests.once);
							 poll.removeEvents(poll.ON_ONCE)
							 this._process_requests(poll, app, { once: app.options.requests.once } );
						 }.bind(this));

						 app.addEvent(app.ON_RANGE_REQUESTS_UPDATED, function(){
							 debug_internals('app.ON_RANGE_REQUESTS_UPDATED %o', app.options.requests.range);
							 poll.removeEvents(poll.ON_RANGE)
							 this._process_requests(poll, app, { range: app.options.requests.range } );
						 }.bind(this));

							app.addEvent(app.ON_PERIODICAL_REQUESTS_UPDATED, function(){
									debug_internals('app.ON_PERIODICAL_REQUESTS_UPDATED %o', app.options.requests.periodical);
									let key = app.options.id;

									if(this.periodicals[key])//if exists, stop and delete all, and process again
										Array.each(this.periodicals[key], function(fn){
											const {type, timer} = fn;
											switch(type){
												case 'task':
													timer.stop();

												case 'timer':
													clearInterval(timer);

												default:
													delete this.periodicals[key];

											}

										}.bind(this));

									Array.each(app.options.requests.periodical, function(app_req){
											this._process_periodical(poll, app, app_req, 'periodical');
									}.bind(this));

							}.bind(this));

							//app['ON_CONFIG_DOC'] = this.ON_CONFIG_DOC;
							//app['ON_CONFIG_DOC_ERROR'] = this.ON_CONFIG_DOC_ERROR;

							//app['ON_MONITOR_DOC'] = this.ON_MONITOR_DOC;
							//app['ON_MONITOR_DOC_ERROR'] = this.ON_MONITOR_DOC_ERROR;

							//
							var docs_id = this.options.id +'.'+poll.options.id +'.'+app.options.id;
							var metadata_merge = {
								//domain: domain,
								//id: poll_id,
								id: this.options.id,
								host: poll.options.id,
								path: app.options.id
							};

							/**
							 * DOCS from app to poll
							 * */
							 app.addEvent(app.ON_DOC, function(doc){
 								debug_events('app.ON_DOC %o', arguments);
 								poll.fireEvent(poll.ON_DOC, [doc, {type: 'doc', input_type: poll, app: app}]);
 							}.bind(this));

 							app.addEvent(app.ON_DOC_ERROR, function(doc){
 								debug_events('app.ON_DOC_ERROR %o', arguments);
 								poll.fireEvent(poll.ON_DOC_ERROR, [doc, {type: 'doc', input_type: poll, app: app}]);
 							}.bind(this));
							app.addEvent(app.ON_ONCE_DOC, function(doc){
								debug_events('app.ON_ONCE_DOC %o', doc);
								poll.fireEvent(poll.ON_ONCE_DOC, [doc, {type: 'once', input_type: poll, app: app}]);
							}.bind(this));

							app.addEvent(app.ON_ONCE_DOC_ERROR, function(doc){
								debug_events('app.ON_ONCE_DOC_ERROR %o', doc);
								poll.fireEvent(poll.ON_ONCE_DOC_ERROR, [doc, {type: 'once', input_type: poll, app: app}]);
							}.bind(this));

							app.addEvent(app.ON_PERIODICAL_DOC, function(doc){
								debug_events('app.ON_PERIODICAL_DOC %o', doc);
								poll.fireEvent(poll.ON_PERIODICAL_DOC, [doc, {type: 'periodical', input_type: poll, app: app}]);
							}.bind(this));

							app.addEvent(app.ON_PERIODICAL_DOC_ERROR, function(doc){
								debug_events('app.ON_PERIODICAL_DOC_ERROR %o', doc);
								poll.fireEvent(poll.ON_PERIODICAL_DOC_ERROR, [doc, {type: 'periodical', input_type: poll, app: app}]);
							}.bind(this));


							app.addEvent(app.ON_RANGE_DOC, function(doc){
								debug_events('app.ON_RANGE_DOC %o', doc);
								poll.fireEvent(poll.ON_RANGE_DOC, [doc, {type: 'range', input_type: poll, app: app}]);
							}.bind(this));

							app.addEvent(app.ON_RANGE_DOC_ERROR, function(doc){
								debug_events('app.ON_RANGE_DOC_ERROR %o', doc);
								poll.fireEvent(poll.ON_RANGE_DOC_ERROR, [doc, {type: 'range', input_type: poll, app: app}]);
							}.bind(this));

							/**
							 * DOCS from app to poll
							 * */

							if(app.options.requests){

								this._process_requests(poll, app, app.options.requests);

							}

						}.bind(this));

						poll.addEvent(poll.ON_CONNECT, function(result){
							debug_events('poll.ON_CONNECT %o', result);

							this._register_poller(index, poll);
						}.bind(this));

						poll.addEvent(poll.ON_CONNECT_ERROR, function(err){
							debug_events('poll.ON_CONNECT_ERROR %o', err);
							this._register_error(index, poll);
						}.bind(this));


					}

					try{

						if(this.options.connect_retry_count < 0 || this.err_poller_count[index] < this.options.connect_retry_count){
							//poll.os.api.get({uri: 'hostname'});
							poll.connect();
						}

					}
					catch(e){
						//////console.log(e);

					}

				}//
			}.bind(this));

		//}.bind(this));
	},
	_process_requests: function(poll, app, requests){
		app = app || poll;

		Object.each(requests, function(requests_by_type, type){
			//app.options.requests_by_type.current = type;

			debug_internals('requests_by_type %o', requests_by_type);
			debug_internals('requests_by_type type %s', type);

			Array.each(requests_by_type, function(app_req){

				debug_internals('single req %o', app_req);

				switch(type){
					case 'once':
						//app.options.requests_by_type.current = type;
						////////console.log('TYPE');
						////////console.log(type);
						//dispatch_app[verb](data);
						if(this.options.suspended == false)
							this.dispatch(null, app, app_req, type);

						poll.addEvent(poll.ON_ONCE, function(req){
							debug_events('poll.ON_ONCE %o', req);
							this.dispatch(req, app, app_req, type);
						}.bind(this));

						//poll.addEvent(poll.ON_CONNECT, function(req){
							//debug_events('poll.ON_CONNECT %o', req);
							//this.dispatch(null, app, app_req, type);
						//}.bind(this));

						break;

					case 'periodical':
						this._process_periodical(poll, app, app_req, type);

						break;

					case 'range':

						poll.addEvent(poll.ON_RANGE, function(req){
							debug_events('poll.ON_RANGE %o', req);
							this.dispatch_range(req, app, app_req, type);
						}.bind(this));
						break;

				}

			}.bind(this));


		}.bind(this));

	},
	_process_periodical: function(poll, app, app_req, type){
		app = app || poll;

		let key = app.options.id;
		debug_internals('_process_periodical app %s', app.options.id);

		if(!this.periodicals[key]) this.periodicals[key] = [];

		var periodical_timer = null;
		var task = null;

		var start = function(){
			if(this.options.suspended == false){
				//console.log('Poller->start', app.options.id)

				let found = this.periodicals[key].some(function(item, index){
					return item.periodical == app_req;
				}.bind(this));

				//////console.log('Poller->start->found', found)

				if(found != true && (!periodical_timer || !periodical_timer._repeat)){

					if(typeof(this.options.requests.periodical) == 'function'){
						let cb = this.dispatch.pass([null, app, app_req, type], this);

						task = this.options.requests.periodical(cb);
						task.start();
						this.periodicals[key].push({type: 'function', timer: task, periodical: app_req});
					}
					else{

						periodical_timer = this.dispatch.periodical(
							this.options.requests.periodical,
							this,
							[null, app, app_req, type]
						);

						//////console.log('Poller->start->periodical_timer', periodical_timer)

						this.periodicals[key].push({type: 'timer', timer: periodical_timer, periodical: app_req});

					}
				}
			}
		}.bind(this);

		var stop = function(){
			this.periodicals[key] = [];

			if(typeof(this.options.requests.periodical) == 'function' && task){
				task.stop();
			}
			else{
				if(periodical_timer && (Number.isInteger(periodical_timer) || periodical_timer._repeat)){
					clearInterval(periodical_timer);
				}
			}
		}.bind(this);

		if(process && process.on){
			process.on('exit',  (code) => stop);
		}
		else if(window && window.onbeforeunload){
			window.onbeforeunload = (code) => stop
		}

		this.addEvent(this.ON_SUSPEND, function(){
			this.options.suspended = true
			debug_events('ON_SUSPEND');
			stop();
		});

		poll.addEvent(poll.ON_CONNECT_ERROR, function(){
			debug_events('poll.ON_CONNECT_ERROR');
			stop();

			poll.addEvent(poll.ON_CONNECT,start);
		});

		this.addEvent(this.ON_RESUME, function(){
			this.options.suspended = false
			debug_events('ON_RESUME');
			start();
		});
		// this.addEvent(this.ON_RESUME, start);

		poll.removeEvent(poll.ON_CONNECT, start);


		start();
	},
	//_process_periodical: function(poll, app, app_req, type){
		//app = app || poll;

		//let key = app.options.id;
		//debug_internals('_process_periodical app %s', app.options.id);

		//if(!this.periodicals[key]) this.periodicals[key] = [];

		//var periodical_timer = null;
		//var task = null;

		//var start = function(){
			//let found = this.periodicals[key].some(function(item, index){
				//return item.periodical == app_req;
			//}.bind(this));

			//if(found != true && (!periodical_timer || !periodical_timer._repeat)){

				//if(typeof(this.options.requests.periodical) == 'function'){
					//let cb = this.dispatch.pass([null, app, app_req, type], this);

					//task = this.options.requests.periodical(cb);
					//task.start();
					//this.periodicals[key].push({type: 'function', timer: task, periodical: app_req});
				//}
				//else{
					//periodical_timer = this.dispatch.periodical(
						//this.options.requests.periodical,
						//this,
						//[null, app, app_req, type]
					//);
					//this.periodicals[key].push({type: 'timer', timer: periodical_timer, periodical: app_req});

				//}
			//}
		//}.bind(this);

		//var stop = function(){
			//if(typeof(this.options.requests.periodical) == 'function'){
				//task.stop();
			//}
			//else{
				//if(Number.isInteger(periodical_timer) || periodical_timer._repeat){
					//clearInterval(periodical_timer);
				//}
			//}
		//}.bind(this);

		//process.on('exit', (code) => stop);

		//this.addEvent(this.ON_SUSPEND, function(){
			//debug_events('ON_SUSPEND');
			//stop();
		//});

		//poll.addEvent(poll.ON_CONNECT_ERROR, function(){
			//debug_events('poll.ON_CONNECT_ERROR');
			//stop();
		//});

		////this.addEvent(this.ON_RESUME, function(){
			////debug_events('ON_RESUME');
			////start();
		////});
		//this.addEvent(this.ON_RESUME, start);

		////poll.addEvent(poll.ON_CONNECT, function(){
			////debug_events('poll.ON_CONNECT');
			////start();
		////});
		//poll.addEvent(poll.ON_CONNECT,start);

		//start();
	//},
	dispatch_range: function(req, app, app_req, type){
		//////console.log('dispatch_range')

		let build_range = function(req){
			if((!req.get && req.Range) || req.get('Range')){
				let range = (!req.get) ? req.Range.trim() : req.get('Range').trim()

				// ////console.log('range', range)
				//var type = req.get('Range').trim().split(' ', 1)[0];
				//var type = req.get('Range').trim().split(' ', 1)[0];

				var words = /(\w+)(?:\s)(\w+)(?:-)(\w+)/g;
				var match = words.exec(range);
				var type = match[1].toLowerCase();

				//////console.log('--HEADER---');
				//////console.log(match);

				var start, end;

				switch (type){
					case 'date':
					case 'utc':
						var date = /^(\d\d\d\d)?(\d\d)?(\d\d)?(\d\d)?(\d\d)?(\d\d)?(\d\d)?$/;
						start = date.exec(match[2]);
						end = date.exec(match[3]);

						//////console.log(start);
						//////console.log(end);

						start.forEach(function(value, i){
							if(!value)
								start[i] = 0;
						});

						end.forEach(function(value, i){
							if(!value)
								end[i] = 0;
						});

						if(type == 'utc'){
							start = new Date(
								Date.UTC(start[1], start[2], start[3], start[4], start[5], start[6], start[7])
							).getTime();

							end = new Date(
								Date.UTC(end[1], end[2], end[3], end[4], end[5], end[6], end[7])
							).getTime();
						}
						else{
							start = new Date(start[1], start[2], start[3], start[4], start[5], start[6], start[7]).getTime();
							end = new Date(end[1], end[2], end[3], end[4], end[5], end[6], end[7]).getTime();
						}


					break;

					case 'posix':
					case 'epoch':
						start = parseInt(match[2]);
						end = parseInt(match[3]);

					break;

					default:
						throw new Error('Type ['+type+'] not implemented');

				};


				return {
					type: type,
					start: start,
					end: end
				}

			}
			else{
				throw new Error('No Range header!');
			}
		}

		let range = undefined
		if(Array.isArray(req)){
			range = []
			Array.each(req, function(_req){
				try{
					range.push(build_range(_req))
				}
				catch(e){}

			})
		}
		else{
			range = build_range(req)
		}

		req['opt'] = {
			range: range
		};

		debug_internals('dispatch_range %o', req['opt']);

		return this.dispatch(req, app, app_req, type);
	},
	dispatch: function(req, app, app_req, type){
		debug_internals('->dispatch %o', type);

		var key = Object.keys(app_req)[0];
		var value = Object.values(app_req)[0];

		var verb = (key == 'api') ? Object.keys(value)[0] : key;
		var data = (key == 'api') ? Object.values(value)[0] : value;
		//var original_app = app;

		var dispatch_app = (key == 'api') ? app.api : app;

		var req_client = (req == null) ? null : {
			agent: (req.headers) ? req.headers['user-agent'] : 'internal',
			ip: req.ip || '127.0.0.1'
		};

		//req_client = req_client || null;
		//////console.log(req_client);
		//////console.log(data);

		app.options.requests.current = {};
		app.options.requests.current.type = type;
		app.options.requests.current.client = req_client;

		if(data instanceof Function){//this way we may run PRE-FUNCTION for complex requests
			debug_internals('dispatch->function %o', [req, dispatch_app[verb]]);

			data.attempt([req, dispatch_app[verb], app], app);
		}
		else{
			debug_internals('dispatch %o', dispatch_app[verb]);
			dispatch_app[verb](data);
		}

	},
	_sanitize_doc: function(doc, doc_id, metadata){

		//let toJSON = function(doc){
			//debug_internals('toJSON %o', doc);

			//let json = JSON.decode(JSON.stringify(doc));

			//debug_internals('toJSON->json %o', doc);

			//return doc;
		//};

		//if(doc)
			//doc = toJSON(doc);
		debug_internals('TO _sanitize_doc isArray %s', Array.isArray(doc));

		if(!doc.data){
			var new_doc = { data: null };
			if(Array.isArray(doc)){
				new_doc.data = doc;
			}
			else{
				new_doc.data = (doc instanceof Object) ? Object.clone(doc) : doc;
			}

			doc = new_doc;
		}

		debug_internals('TO _sanitize_doc %o', doc);

		var timestamp = Date.now();

		if(!doc._id){
			doc._id = doc_id +'@'+timestamp;
		}

		doc['metadata']	 = Object.clone(metadata);
		doc['metadata']['type'] = null;
		doc['metadata']['client'] = null;
		doc['metadata']['timestamp'] = timestamp;

		debug_internals('_sanitize_doc %o', doc);

		return doc;
	},
	//_register_poller: function(poll_id, index, poll){
	_register_poller: function(index, poll){
		if(!this.conn_pollers[index] || this.conn_pollers[index] != poll){
			//////console.log('CONNECTED hostname');
			////console.log('-----', index);
			////console.log('-----', poll);

			//this.conn_pollers[poll_id+'.'+hostname] = poll;
			if(!this.conn_pollers)
				this.conn_pollers = [];

			this.conn_pollers[index] = poll;

			if(this.err_poller_count)
				delete this.err_poller_count[index];

			if(this.err_pollers)
				delete this.err_pollers[index];

			debug_internals('_register_poller %o', poll);

			this.fireEvent(this.ON_CLIENT_CONNECT, poll);
		}
	},
	//_register_error: function(poll_id, index, poll){
	_register_error: function(index, poll){
		//////console.log('DISCONNECTED hostname');
		////console.log('_register_error -----', index);


		this.err_poller_count[index] += 1;

		var err_poller = {};
		err_poller = [];
		err_poller[index] = this.options.conn[index];
		//err_poller.push(poller);

		if(!this.err_pollers)
			this.err_pollers = [];

		if(this.err_pollers[index] != poll){
			this.err_pollers[index] = poll;

			var reconnect_timer = null;

			var reconnect = function(err_poller){
				//////console.log('RE CONNECTING');
				//////console.log(err_poller);

				reconnect_timer = this.connect.periodical(this.options.connect_retry_periodical, this, err_poller);

				//process.exit(1);
			}.bind(this);


			var reconnect_stop = function(){clearInterval(reconnect_timer)}.bind(this);

			// process.on('exit', (code) => reconnect_stop);
			if(process && process.on){
				process.on('exit',  (code) => reconnect_stop);
			}
			else if(window && window.onbeforeunload){
				window.onbeforeunload = (code) => reconnect_stop
			}

			//this.addEvent(this.ON_SUSPEND, reconnect_stop);

			//poll.addEvent(poll.ON_CONNECT_ERROR, stop);

			//this.addEvent(this.ON_RESUME, reconnect_start);

			poll.addEvent(poll.ON_CONNECT, function(){
				debug_events('poll.ON_CONNECT');
				reconnect_stop();
			});

			reconnect(err_poller);
		}

		if(this.conn_pollers)
			delete this.conn_pollers[index];

		debug_internals('_register_error %o', poll);


		this.fireEvent(this.ON_CLIENT_CONNECT_ERROR, err_poller);
	},


});
