'use strict'

const	mootools = require('mootools')

const Telegram = require('telegraf/telegram')


var debug = require('debug')('Server:App:Pipeline:Output:Telegram');
var debug_events = require('debug')('Server:App:Pipeline:Output:Telegram:Events');
var debug_internals = require('debug')('Server:App:Pipeline:Output:Telegram:Internals');

/**
 * TelegramOutput
 *
 * */
module.exports = new Class({
  Implements: [Options, Events],

  // dbs: [],
  accept: false,
  conns: [],
  buffer: [],
  buffer_expire: 0,

  ON_CONNECT: 'onConnect',
  ON_CONNECT_ERROR: 'onConnectError',
  ON_ACCEPT: 'onAccept',

  ON_DOC: 'onDoc',
	//ON_DOC_ERROR: 'onDocError',

	ON_ONCE_DOC: 'onOnceDoc',
	//ON_ONCE_DOC_ERROR: 'onOnceDocError',

	ON_PERIODICAL_DOC: 'onPeriodicalDoc',
  //ON_PERIODICAL_DOC_ERROR: 'onPeriodicalDocError',

  ON_SAVE_DOC: 'onSaveDoc',
  ON_SAVE_MULTIPLE_DOCS: 'onSaveMultipleDocs',

  ON_DOC_SAVED: 'onDocSaved',

  options: {
		id: null,
    conn: [
      {
        token: undefined,
        chatId: undefined,
      }
		// 	{
    //     host: '127.0.0.1',
		// 		port: 28015,
		// 		db: undefined,
    //     table: undefined,
    //     rethinkdb: {
    // 			// 'user': undefined, //the user account to connect as (default admin).
    // 			// 'password': undefined, // the password for the user account to connect as (default '', empty).
    // 			// 'timeout': undefined, //timeout period in seconds for the connection to be opened (default 20).
    // 			// /**
    // 			// *  a hash of options to support SSL connections (default null).
    // 			// * Currently, there is only one option available,
    // 			// * and if the ssl option is specified, this key is required:
    // 			// * ca: a list of Node.js Buffer objects containing SSL CA certificates.
    // 			// **/
    // 			// 'ssl': undefined,
    // 		},
		// 	},
		],

		buffer:{
      size: 5,//-1 =will add until expire | 0 = no buffer | N > 0 = limit buffer no more than N
			expire: 5000, //miliseconds until saving
			periodical: 1000 //how often will check if buffer timestamp has expire
		},

    // message: {parse_mode: 'Markdown'}
    // insert: {durability: 'soft', returnChanges: false, conflict: 'replace'},
	},
  // connect(err, conn, params){
	// 	debug_events('connect %o', err, conn)
	// 	if(err){
	// 		this.fireEvent(this.ON_CONNECT_ERROR, { error: err, params: params });
	// 		// throw err
	// 	}
	// 	else {
	// 		// this.conn = conn
	// 		this.fireEvent(this.ON_CONNECT, { conn: conn,  params: params});
  //
  //     try{
  //       let index = params.index
  //       let db = this.options.conn[index].db
  //       let table = this.options.conn[index].table
  //
  //       this.r.dbList().run(conn, function(err, dbs){
  //         debug_internals('connect-> setting dbs %o %s', dbs, db);
  //         let exist = false
  //         Array.each(dbs, function(d){
  //           if(d == db)
  //             exist = true
  //         })
  //
  //         if(exist === false){
  //           debug_internals('connect-> setting db/table %o', this.options.conn[index]);
  //           this.r.dbCreate(db).run(conn, function(err, result){
  //             // this._save_docs(doc, index);
  //             try{
  //               this.r.db(db).tableCreate(table).run(conn, function(err, result){
  //                 debug_internals('connect-> setting db/table %o create', err, result);
  //                 this.options.conn[index].accept = true
  //
  //                 this.fireEvent(this.ON_ACCEPT)
  //               }.bind(this))
  //             }
  //             catch(e){
  //               // this._save_docs(doc, index);
  //               debug_internals('tableCreate error %o', e);
  //             }
  //           }.bind(this));
  //         }
  //         else {
  //           this.options.conn[index].accept = true
  //
  //           this.fireEvent(this.ON_ACCEPT)
  //         }
  //       }.bind(this))
  //
  //
  //     }
  //     catch(e){
  //       // console.log(e)
  //       debug_internals('dbCreate error %o', err);
  //       // this._save_docs(doc, index);
  //     }
	// 	}
	// },
	initialize: function(options, connect_cb){
		//console.log('---RethinkDBOutput->init---');
		//throw new Error();

		this.setOptions(options);
    this.fireEvent(this.ON_CONNECT);
    this.fireEvent(this.ON_ACCEPT)
		if(typeOf(this.options.conn) != 'array'){
			var conn = this.options.conn;
			this.options.conn = [];
			this.options.conn.push(conn);
		}

		Array.each(this.options.conn, function(conn, index){
      this.conns[index] = { telegram: new Telegram(conn.token), chatId: conn.chatId }
			// // this.dbs.push( new(couchdb.Connection)(conn.host, conn.port, conn.opts).database(conn.db) );
      //
      // let opts = {
  		// 	host: conn.host,
  		// 	port: conn.port,
  		// 	db: conn.db
  		// };
      //
      // let _cb = function(err, conn){
      //   this.options.conn[index] = conn
      //   connect_cb = (typeOf(connect_cb) ==  "function") ? connect_cb.bind(this) : this.connect.bind(this)
      //   connect_cb(err, conn, Object.merge(opts, {index: index}))
      // }.bind(this)
      //
      // this.r = require('rethinkdb')
      //
  		// this.r.connect(Object.merge(opts, conn.rethinkdb), _cb)
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
		debug_internals('save %o', doc);

		if(this.options.buffer.size == 0){
      // if((typeof(doc) == 'array' || doc instanceof Array || Array.isArray(doc)) && doc.length > 0){
      //   this._save_to_tokens([doc])
      // }
      // else{
        this._save_to_tokens(doc)
      // }
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
  _save_to_tokens: function(doc){
    debug_internals('_save_to_tokens %o', this.conns)
    Array.each(this.conns, function(conn, index){
      // // let table = this.options.conn[index].table
      // let token = this.options.conn[index].token
      // // let table = this.options.conn[index].table
      // // let accept = this.options.conn[index].accept

      // debug_internals('_save_to_tokens %s %s %o', token)
      this._save_docs(doc, index);
      // if(accept === true){
      //   this._save_docs(doc, index);
      // }
      // else{
      //   let _save = function(){
      //     this._save_docs(doc, index);
      //     this.removeEvent(this.ON_ACCEPT, _save)
      //   }.bind(this)
      //   this.addEvent(this.ON_ACCEPT, _save)
      // }


    }.bind(this));
  },
	_save_docs: function(doc, index){
		// let token = this.options.conn[index].token
    let telegram = this.conns[index].telegram
    let chatId = this.conns[index].chatId
    if(!Array.isArray(doc)) doc = [doc]

    debug_internals('_save_docs %o %s %o', doc, index, this.options.message, chatId);
    // process.exit(1)

    Array.each(doc, function(message){
      telegram.sendMessage(chatId, message, this.options.message).then((resp) => {debug_internals('_save_docs resp %o', resp)})
    }.bind(this))


    // let table = this.options.conn[index].table
    // let conn = this.options.conn[index]
    //
    // this.r.db(db).table(table).insert(doc, this.options.insert).run(conn, function(err, result){
    //   debug_internals('insert result %o', err, result);
    //   this.fireEvent(this.ON_DOC_SAVED, [err, result])
    // }.bind(this))

	},
  _expire_buffer: function(){
		if(this.buffer_expire <= Date.now() && this.buffer.length > 0){
      debug_internals('_expire_buffer %o', this.buffer_expire);
			this._save_buffer()
		}

	},
	_save_buffer: function(){
		// if(this.buffer_expire <= Date.now() && this.buffer.length > 0){
      // debug_internals('_save_buffer %o', this.buffer);
			// let doc = this.buffer;
			// this._save_docs(Array.clone(this.buffer));

      // if(this.accept === true){
        this._save_to_tokens(Array.clone(this.buffer));
  			this.buffer = [];
  			this.buffer_expire = Date.now() + this.options.buffer.expire;
      // }

			// debug_internals('_save_buffer %o', doc);
		// }

	}
});
