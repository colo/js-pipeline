
'use strict'

const	mootools = require('mootools')

let r = require('rethinkdb')


var debug = require('debug')('Server:App:Pipeline:Output:RethinkDB');
var debug_events = require('debug')('Server:App:Pipeline:Output:RethinkDB:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Output:RethinkDB:Internals');

/**
 * RethinkDBOutput
 *
 * */
module.exports = new Class({
  Implements: [Options, Events],

  // dbs: [],
  conns: [],
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
				port: 28015,
				db: undefined,
        table: undefined,
        rethinkdb: {
    			// 'user': undefined, //the user account to connect as (default admin).
    			// 'password': undefined, // the password for the user account to connect as (default '', empty).
    			// 'timeout': undefined, //timeout period in seconds for the connection to be opened (default 20).
    			// /**
    			// *  a hash of options to support SSL connections (default null).
    			// * Currently, there is only one option available,
    			// * and if the ssl option is specified, this key is required:
    			// * ca: a list of Node.js Buffer objects containing SSL CA certificates.
    			// **/
    			// 'ssl': undefined,
    		},
			},
		],

		buffer:{
      size: 5,//-1 =will add until expire | 0 = no buffer | N > 0 = limit buffer no more than N
			expire: 5000, //miliseconds until saving
			periodical: 1000 //how often will check if buffer timestamp has expire
		}
	},
  connect(err, conn, params){
		debug_events('connect %o', err, conn)
		if(err){
			this.fireEvent(this.ON_CONNECT_ERROR, { error: err, params: params });
			throw err
		}
		else {
			this.conn = conn
			this.fireEvent(this.ON_CONNECT, { conn: conn,  params: params});
		}
	},
	initialize: function(options, connect_cb){
		//console.log('---RethinkDBOutput->init---');
		//throw new Error();

		this.setOptions(options);

		if(typeOf(this.options.conn) != 'array'){
			var conn = this.options.conn;
			this.options.conn = [];
			this.options.conn.push(conn);
		}

		Array.each(this.options.conn, function(conn, index){
			// this.dbs.push( new(couchdb.Connection)(conn.host, conn.port, conn.opts).database(conn.db) );

      let opts = {
  			host: conn.host,
  			port: conn.port,
  			db: conn.db
  		};

      let _cb = function(err, conn){
        connect_cb = (typeOf(connect_cb) ==  "function") ? connect_cb.bind(this) : this.connect.bind(this)
        connect_cb(err, conn, opts)
      }

      this.r = require('rethinkdb')

  		this.conns[index] = this.r.connect(Object.merge(opts, conn.rethinkdb), _cb)
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
		// debug('save %o', doc);

		if(this.options.buffer.size == 0){

			this._save_to_dbs(doc)
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


		}
	},
  _save_to_dbs: function(doc){

    Array.each(this.conns, function(conn, index){
      // let table = this.options.conn[index].table

      try{
        this.r.dbCreate(this.options.conn[index].db).run(conn, function(result){
          this._save_docs(doc, index);
        }.bind(this));


        // this.r.dbList().run(conn, function(list){
        //   let exist = false
        //   Array.each(list, function(db){
        //     if(db == this.options.conn[index].db)
        //       exist = true
        //   })
        //
        //   if(exist === false){
        //     this.r.dbCreate(this.options.conn[index].db).run(conn, callback);
        //   }
        // })
        // conn.db.get(name, (err, data, headers) => {
        //   if (err) {
        //     debug_internals('db.get error %o', err);
        //     if(err.statusCode == 404){
        //       try{
        //         conn.db.create(name, (err, data, headers) => {
        //           if (err) {
        //             debug_internals('db.create error %o', err);
        //           }
        //           else{
        //             this._save_docs(doc, name);
        //           }
        //         })
        //       }
        //       catch(e){}
        //     }
        //   }
        //   else {
        //     this._save_docs(doc, index);
        //   }
        // })
      }
      catch(e){
        // console.log(e)
        debug_internals('dbCreate error %o', err);
        this._save_docs(doc, index);
      }



    }.bind(this));
  },
	_save_docs: function(doc, index){
		debug_internals('_save_docs %o %s', doc, index);

    let db = this.options.conn[index].db
    let table = this.options.conn[index].table

    try{
      this.r.db(db).tableCreate(table).run(this.conns[index], function(result){
        this.r.db(db).table(table).insert(doc).run(this.conns[index], function(result){
          debug_internals('insert result %o', result);
        })
      })
    }
    catch(e){
      this.r.db(db).table(table).insert(doc).run(this.conns[index], function(result){
        debug_internals('insert result %o', result);
      })
      debug_internals('tableCreate error %o', err);
    }
    // if((typeof(doc) == 'array' || doc instanceof Array || Array.isArray(doc)) && doc.length > 0){
    //   try{
    //     db = this.conns[index].use(db)
    //     db.bulk({docs: doc }, (err, data, headers) => {
    //       if(err)
    //         debug_internals('db.bulk err %o', err)
    //     })
    //   }
    //   catch(e){
    //     console.log(e)
    //   }
    //
    // }
    // else{
    //   try{
    //     db = this.conns[db].use(db)
    //     db.insert(doc, (err, data, headers) => {
    //       if(err)
    //         debug_internals('db.insert err %o', err)
    //     })
    //   }
    //   catch(e){
    //     console.log(e)
    //   }
    // }

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
});
