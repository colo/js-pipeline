'use strict'

const	mootools = require('mootools'),
			Poller = require('./input/poller'),
			Pusher = require('./input/pusher');
			//CradleOutput = require('./output/cradle');

var debug = require('debug')('Server:App:Pipeline');
var debug_internals = require('debug')('Server:App:Pipeline:Internals');
var debug_events = require('debug')('Server:App:Pipeline:Events');

module.exports = new Class({
  Implements: [Options, Events],

	ON_SUSPEND: 'onSuspend',
	ON_RESUME: 'onResume',
	ON_EXIT: 'onExit',

	ON_ONCE: 'onOnce',
	ON_RANGE: 'onRange',

	ON_DOC: 'onDoc',
	ON_DOC_ERROR: 'onDocError',

	ON_ONCE_DOC: 'onOnceDoc',
	ON_ONCE_DOC_ERROR: 'onOnceDocError',

	ON_PERIODICAL_DOC: 'onPeriodicalDoc',
	ON_PERIODICAL_DOC_ERROR: 'onPeriodicalDocError',

	ON_SAVE_DOC: 'onSaveDoc',
	ON_SAVE_MULTIPLE_DOCS: 'onSaveMultipleDocs',

	inputs: undefined,
	//filters: [],
	outputs: [],

	options: {
		input: null,
		filters: null,
		output: null,

	},
	initialize: function(options){
		this.setOptions(options);

		//console.log(this.options);
		debug_internals('PollerInputs %o', this.options.input);


		if(this.options.input){
			if(Array.isArray(this.options.input)){//on array maybe use input.id as object.key
				this.inputs = []
				Array.each(this.options.input, function(input, index){
					let _input = this.__process_input(input)
					if(_input) this.inputs.push(_input)
					debug_internals('PollerInputs %s %o', index, _input)
				}.bind(this));
			}
			else{
				this.inputs = {}
				Object.each(this.options.input, function(input, name){
					let _input = this.__process_input(input)
					if(_input) this.inputs[name] = _input
					debug_internals('PollerInputs %s %o', name, _input)
				}.bind(this))
			}


		}

		if(this.options.output){
			Array.each(this.options.output, function(output){
				let type = Object.keys(output)[0];

				if(typeof(output) == 'function'){
					this.outputs.push(output);
				}
				else if(output[type].module){
					this.outputs.push(new output[type].module(output[type]));
				}
				else{
					throw new Error('Output ['+type+'] not implemented');
				}


			}.bind(this));
		}


		this.start();
	},
	__process_input: function(input){
		let type = Object.keys(input)[0];
		let result = undefined

		switch (type){
			case 'poll':
				debug_internals('Adding PollerInput %o', input[type]);
				result = new Poller(input[type])
				break;

			case 'push':
				debug_internals('Adding PusherInput %o', input[type]);
				result = new Pusher(input[type])
				break;

			default:
				throw new Error('Input ['+type+'] not implemented');
		}

		return result
	},
	__start_input: function(input){
		this.addEvent(this.ON_SUSPEND, function(req){
			if(Array.isArray(req))
				req = [req]

			input.fireEvent(input.ON_SUSPEND, req);
		});
		this.addEvent(this.ON_RESUME, function(req){
			if(Array.isArray(req))
				req = [req]

			input.fireEvent(input.ON_RESUME, req);
		});
		this.addEvent(this.ON_EXIT, function(req){
			if(Array.isArray(req))
				req = [req]

			input.fireEvent(input.ON_EXIT, req);
		});
		this.addEvent(this.ON_RANGE, function(req){
			// console.log('js-pipeline ON_RANGE', req)
			if(Array.isArray(req))
				req = [req]

			input.fireEvent(input.ON_RANGE, req);
		});
		this.addEvent(this.ON_ONCE, function(req){
			if(Array.isArray(req))
				req = [req]

			input.fireEvent(input.ON_ONCE, req);
		});

		//input['ON_SAVE_DOC'] = this.ON_SAVE_DOC;
		//input['ON_SAVE_MULTIPLE_DOCS'] = this.ON_SAVE_MULTIPLE_DOCS;

		//input.addEvent(input.ON_SAVE_DOC, function(doc){
			//this.fireEvent(this.ON_SAVE_DOC, doc);
		//}.bind(this));

		//input.addEvent(input.ON_SAVE_MULTIPLE_DOCS, function(docs){
			//this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [docs]);
		//}.bind(this));

		if(input.ON_DOC)
			input.addEvent(input.ON_DOC, function(doc, opts){
				debug_events('input.ON_DOC %o', doc);

				if(this.options.filters && this.options.filters.length > 0){
					opts.input = input;
					// this._apply_filters(doc, opts, Array.clone(this.options.filters), Array.clone(this.options.filters).shift());
					this._apply_filters(doc, opts);
				}
				else{
					if(Array.isArray(doc)){
						this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [doc]);
					}
					else{
						this.fireEvent(this.ON_SAVE_DOC, doc);
					}
				}


			}.bind(this));

		if(input.ON_ONCE_DOC)
			input.addEvent(input.ON_ONCE_DOC, function(doc, opts){
				debug_events('input.ON_ONCE_DOC %o', doc);

				if(this.options.filters && this.options.filters.length > 0){
					opts.input = input;
					// this._apply_filters(doc, opts, Array.clone(this.options.filters), Array.clone(this.options.filters).shift());
					this._apply_filters(doc, opts);
				}
				else{
					if(Array.isArray(doc)){
						this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [doc]);
					}
					else{
						this.fireEvent(this.ON_SAVE_DOC, doc);
					}
				}


			}.bind(this));

		if(input.ON_PERIODICAL_DOC)
			input.addEvent(input.ON_PERIODICAL_DOC, function(doc, opts){
				debug_events('input.ON_PERIODICAL_DOC %o', doc);

				if(this.options.filters && this.options.filters.length > 0){
					opts.input = input;
					// this._apply_filters(doc, opts, Array.clone(this.options.filters), Array.clone(this.options.filters).shift());
					this._apply_filters(doc, opts);
				}
				else{
					if(Array.isArray(doc)){
						this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [doc]);
					}
					else{
						this.fireEvent(this.ON_SAVE_DOC, doc);
					}
				}

			}.bind(this));

		if(input.ON_RANGE_DOC)
			input.addEvent(input.ON_RANGE_DOC, function(doc, opts){
				debug_events('input.ON_RANGE_DOC %o', doc);

				if(this.options.filters && this.options.filters.length > 0){
					opts.input = input;
					// this._apply_filters(doc, opts, Array.clone(this.options.filters), Array.clone(this.options.filters).shift());
					this._apply_filters(doc, opts);
				}
				else{
					if(Array.isArray(doc)){
						this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [doc]);
					}
					else{
						this.fireEvent(this.ON_SAVE_DOC, doc);
					}
				}

			}.bind(this));

		input.connect();
	},
	start: function(){
		if(Array.isArray(this.inputs)){
			Array.each(this.inputs, function(input){
				this.__start_input(input)
			}.bind(this));
		}
		else{
			Object.each(this.inputs, function(input){
				this.__start_input(input)
			}.bind(this));
		}



		Array.each(this.outputs, function(output){

			this.addEvent(this.ON_SAVE_DOC, function(doc){
				debug_events('ON_SAVE_DOC %o', doc);

				if(typeof(output) == 'function'){
					//output(JSON.encode(doc));
					output(doc);
				}
				else{
					output.fireEvent(output.ON_SAVE_DOC, doc);
				}
			}.bind(this));

			this.addEvent(this.ON_SAVE_MULTIPLE_DOCS, function(docs){
				debug_events('ON_SAVE_MULTIPLE_DOCS %o', docs);
				if(typeof(output) == 'function'){
					//output(JSON.encode(docs));
					output(docs);
				}
				else{
					output.fireEvent(output.ON_SAVE_MULTIPLE_DOCS, [docs]);
				}
			}.bind(this));

			//input.connect();
		}.bind(this));

	},
	output: function(save_doc){
		if(Array.isArray(save_doc)){
			this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [save_doc]);
		}
		else{
			this.fireEvent(this.ON_SAVE_DOC, save_doc);
		}
		//this.fireEvent(this.ON_SAVE_DOC, save_doc);
	},
	// _apply_filters: function(doc, opts, filters){
	// 	let current = null
  //
	// 	// if(filters != undefined){
	// 		// let new_current = undefined
	// 		if(Array.isArray(filters)){
	// 			current = filters.shift()
	// 		}
	// 		// else{
	// 		// 	current = filters
	// 		// 	filters = undefined
	// 		// 	// new_current = undefined
	// 		// }
	// 		if(typeOf(current) == 'function') {
	// 	    current(
	// 				doc,
	// 				opts,
	// 				function(new_doc, new_opts, next, pipeline){
	// 					// console.log('filters', filters, new_doc)
  //
	// 					if(filters){
	// 						this._apply_filters(new_doc, new_opts, filters)
	// 					}
	// 					else{
	// 						this.output(new_doc)
	// 					}
	// 				}.bind(this),
	// 				this
	// 			)
	// 	  }
	// 		else{
	// 			this.output(doc)
	// 		}
	// 	// }
  //
	// },
	_apply_filters: function(doc, opts){

		//debug_internals('_apply_filters %o', doc);
		//debug_internals('_apply_filters opts %o', opts);

		/**
		 * test
		 * */
		//this.filters = [
			//function(doc){
				//Array.each(doc.data, function(value, index){
					//doc.data[index] = ++value;
				//});

				//return doc;
			//},
			//function(doc){
				//doc.data.push(99);
				//return doc;
			//}
		//];
		/**
		 * test
		 * */

		//let filter = this.options.filters[0];
		let current = null;
		let prev = null;

		for(let i = this.options.filters.length - 1; i >= 0 ; i--){

			if(i == this.options.filters.length - 1){
				debug_internals('_apply_filters last')

				if(this.options.filters.length == 1){//if there is only one filter, 'next' must be sent to "output/save"

					//current = function(save_doc){
						//if(Array.isArray(save_doc)){
							//this.fireEvent(this.ON_SAVE_MULTIPLE_DOCS, [save_doc]);
						//}
						//else{
							//this.fireEvent(this.ON_SAVE_DOC, save_doc);
						//}
						////this.fireEvent(this.ON_SAVE_DOC, save_doc);
					//}.bind(this);
					current = this.output.bind(this)
					debug_internals('_apply_filters last 1')

				}
				else{
					let self = this
					current = function(filtered_doc){

						this.options.filters[i](filtered_doc, opts, this.output.bind(this), this);

					}.bind(this);

					debug_internals('_apply_filters last 2')

				}
			}
			else if(i != 0){
				debug_internals('_apply_filters not zero %d', i);

				prev = current;
				current = function(filtered_doc, opts){
					this.options.filters[i](filtered_doc, opts, function(new_doc, opts){
						prev(new_doc, opts, undefined, this);
					}.bind(this), this);
				}.bind(this);
			}

			if(i == 0){//first filter, start running
				debug_internals('_apply_filters start %o', doc);

				this.options.filters[i](doc, opts, current, this);
			}


			//current(doc, opts, prev);


		}

		//filter(doc, opts, current);

		//}.bind(this));

		//debug_internals('_apply_filters->doc %o', doc);

		//return doc;
	}

});
