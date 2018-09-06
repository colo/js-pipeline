
'use strict'

const	mootools = require('mootools'),
			cradle = require('cradle-pouchdb-server');

var debug = require('debug')('Server:App:Pipeline:Output:Cradle');
var debug_events = require('debug')('Server:App:Pipeline:Output:Cradle:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Output:Cradle:Internals');

/**
 * CradleOutput
 *
 * */
module.exports = new Class({
  Implements: [Options, Events],

  dbs: [],
  buffer: [],
  buffer_expire: 0,

  ON_DOC: 'onDoc',
	//ON_DOC_ERROR: 'onDocError',

	ON_ONCE_DOC: 'onOnceDoc',
	//ON_ONCE_DOC_ERROR: 'onOnceDocError',

	ON_PERIODICAL_DOC: 'onPeriodicalDoc',
  //ON_PERIODICAL_DOC_ERROR: 'onPeriodicalDocError',

  ON_SAVE_DOC: 'onSaveDoc',
  ON_SAVE_MULTIPLE_DOCS: 'onSaveMultipleDocs',

  options: {
		id: null,
		conn: [
			{
				host: '127.0.0.1',
				port: 5984,
				db: '',
				opts: {
					cache: true,
					raw: false,
					forceSave: false,
				},
			},
		],

		buffer:{
			size: 5,//-1 =will add until expire | 0 = no buffer | N > 0 = limit buffer no more than N
			expire: 5000, //miliseconds until saving
			periodical: 1000 //how often will check if buffer timestamp has expire
		}
	},

	initialize: function(options){
		//console.log('---CradleOutput->init---');
		//throw new Error();

		this.setOptions(options);

		if(typeOf(this.options.conn) != 'array'){
			var conn = this.options.conn;
			this.options.conn = [];
			this.options.conn.push(conn);
		}

		Array.each(this.options.conn, function(conn, index){
			this.dbs.push( new(cradle.Connection)(conn.host, conn.port, conn.opts).database(conn.db) );
		}.bind(this));



		this.addEvent(this.ON_SAVE_DOC, function(doc){
			debug_events('this.ON_SAVE_DOC %o', doc);

			this.save(doc);
		}.bind(this));

		this.addEvent(this.ON_SAVE_MULTIPLE_DOCS, function(docs){
			debug_events('this.ON_SAVE_MULTIPLE_DOCS %o', docs);

			this.save(docs);
		}.bind(this));

		this.buffer_expire = Date.now() + this.options.buffer.expire;
		this._expire_buffer.periodical(this.options.buffer.periodical, this);

	},
	save: function(doc){
		debug('save %o', doc);

		if(this.options.buffer.size == 0){

			this._save_to_dbs(doc)

			// Array.each(this.dbs, function(db, index){
      //
			// 	db.exists(function (err, exists) {
			// 		if (err) {
			// 			debug_internals('db.exists error %o', err);
			// 		}
			// 		else if (exists) {
			// 			////console.log('the force is with you.');
			// 			this._save_docs(doc);
			// 		}
			// 		else {
			// 			////console.log('database does not exists.');
			// 			this.db.create(function(err){
			// 				if(!err){
			// 					this._save_docs(doc);
			// 				}
			// 				else{
			// 					debug_internals('db.create error %o', err);
			// 				}
			// 			}.bind(this));
			// 		}
			// 	}.bind(this));
      //
			// }.bind(this));
		}
		// else if( this.buffer.length < this.options.buffer.size && this.buffer_expire > Date.now()){
		// 	this.buffer.push(doc);
		// }
		else{
			if((typeof(doc) == 'array' || doc instanceof Array || Array.isArray(doc)) && doc.length > 0){
        Array.each(doc, function(d){
          this.buffer.push(d)
          if(this.options.buffer.size > 0 && this.buffer.length >= this.options.buffer.size){
            this._save_buffer()
          }
        }.bind(this))
      }
      else{
  			this.buffer.push(doc)
      }
      //
			// this.buffer.push(doc);
      //
			// let buffer = this.buffer;
      //
			// Array.each(this.dbs, function(db, index){
      //
			// 	db.exists(function (err, exists) {
			// 		if (err) {
			// 			debug_internals('db.exists error %o', err);
			// 		}
			// 		else if (exists) {
			// 			////console.log('the force is with you.');
			// 			this._save_docs(buffer);
			// 		}
			// 		else {
			// 			////console.log('database does not exists.');
			// 			this.db.create(function(err){
			// 				if(!err){
			// 					this._save_docs(buffer);
			// 				}
			// 				else{
			// 					debug_internals('db.create error %o', err);
			// 				}
			// 			}.bind(this));
			// 		}
			// 	}.bind(this));
      //
			// }.bind(this));
      //
      //
      //
			// this.buffer = [];
			// this.buffer_expire = Date.now() + this.options.buffer.expire;

		}
	},
	_save_to_dbs: function(doc){

		Array.each(this.dbs, function(db, index){

			db.exists(function (err, exists) {
				if (err) {
					debug_internals('db.exists error %o', err);
				}
				else if (exists) {
					////console.log('the force is with you.');
					this._save_docs(doc, db);
				}
				else {
					////console.log('database does not exists.');
					this.db.create(function(err){
						if(!err){
							this._save_docs(doc, db);
						}
						else{
							debug_internals('db.create error %o', err);
						}
					}.bind(this));
				}
			}.bind(this));

		}.bind(this))

  },
	// _save_docs: function(doc, db){
	// 	debug_internals('_save_docs %o %s', doc,db);
  //
  //   if((typeof(doc) == 'array' || doc instanceof Array || Array.isArray(doc)) && doc.length > 0){
  //     try{
  //       db = this.conns[db].use(db)
  //       db.bulk({docs: doc }, (err, data, headers) => {
  //         if(err)
  //           debug_internals('db.bulk err %o', err)
  //       })
  //     }
  //     catch(e){
  //       console.log(e)
  //     }
  //
  //   }
  //   else{
  //     try{
  //       db = this.conns[db].use(db)
  //       db.insert(doc, (err, data, headers) => {
  //         if(err)
  //           debug_internals('db.insert err %o', err)
  //       })
  //     }
  //     catch(e){
  //       console.log(e)
  //     }
  //   }
  //
	// },

	_save_docs: function(doc, db){
		debug_internals('_save_docs %o', doc);

		// Array.each(this.dbs, function(db, index){

			if((typeof(doc) == 'array' || doc instanceof Array || Array.isArray(doc)) && doc.length > 0){
				db.save(doc, function (err, res) {
					if(err){
						debug_internals('BULK db.save err %o', err);
					}
					else{
						debug_internals('BULK db.save %o', res);
					}
					//throw new Error();
				});

			}
			//else if(typeOf(doc) != 'array'){
			else{
				db.save(doc._id, doc, function (err, res) {
					if(err){
						debug_internals('db.save err %o', err);
					}
					else{
						debug_internals('db.save %o', res);
					}
					//throw new Error();
				});
			}

		// }.bind(this));


	},
	_expire_buffer: function(){

		if(this.buffer_expire <= Date.now() && this.buffer.length > 0){
			debug_internals('_expire_buffer %o', this.buffer_expire);
			this._save_buffer()
		}

	},
	_save_buffer: function(){
		// if(this.buffer_expire <= Date.now() && this.buffer.length > 0){
      debug_internals('_save_buffer %o', this.buffer);
			// let doc = this.buffer;
			// this._save_docs(Array.clone(this.buffer));
      this._save_to_dbs(Array.clone(this.buffer));
			this.buffer = [];
			this.buffer_expire = Date.now() + this.options.buffer.expire;

			// debug_internals('_save_buffer %o', doc);
		// }

	}
	// _save_buffer: function(){
	// 	debug_internals('_save_buffer');
  //
	// 	// if(this.buffer_expire <= Date.now() && this.buffer.length > 0){
	// 		var doc = this.buffer;
	// 		this._save_docs(doc);
	// 		this.buffer = [];
	// 		this.buffer_expire = Date.now() + this.options.buffer.expire;
  //
	// 		debug_internals('_save_buffer %o', doc);
	// 	// }
  //
	// }
});
