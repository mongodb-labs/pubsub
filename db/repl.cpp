// repl.cpp

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "stdafx.h"
#include "jsobj.h"
#include "../util/goodies.h"
#include "repl.h"
#include "../grid/message.h"
#include "dbclient.h"
#include "pdfile.h"
#include "query.h"
#include "json.h"
#include "db.h"

extern JSObj emptyObj;
extern boost::mutex dbMutex;
auto_ptr<Cursor> findTableScan(const char *ns, JSObj& order);
bool userCreateNS(const char *ns, JSObj& j, string& err);
int _updateObjects(const char *ns, JSObj updateobj, JSObj pattern, bool upsert, stringstream& ss);
bool _runCommands(const char *ns, JSObj& jsobj, stringstream& ss, BufBuilder &b, JSObjBuilder& anObjBuilder);

OpTime last(0, 0);

OpTime OpTime::now() { 
	unsigned t = (unsigned) time(0);
	if( last.secs == t ) {
		last.i++;
		return last;
	}
	last = OpTime(t, 1);
	return last;
}

struct TestOpTime { 
	TestOpTime() {
		OpTime t;
		for( int i = 0; i < 10; i++ ) { 
			OpTime s = OpTime::now();
			assert( s != t );
			t = s;
		}
		OpTime q = t;
		assert( q == t );
		assert( !(q != t) );
	}
} testoptime;

int test2() { 
	return 0;
}


/* Cloner -----------------------------------------------------------
   makes copy of existing database.
*/

class Cloner: boost::noncopyable { 
	DBClientConnection conn;
	void copy(const char *collection);
public:
	Cloner() { }
	bool go(const char *masterHost, string& errmsg);
};

void Cloner::copy(const char *collection) {


	auto_ptr<DBClientCursor> c( conn.query(collection, emptyObj) );
	assert( c.get() );
	while( c->more() ) { 
		JSObj js = c->next();
		theDataFileMgr.insert(collection, (void*) js.objdata(), js.objsize());
	}
}

extern int port;
bool Cloner::go(const char *masterHost, string& errmsg) { 
	if( (string("localhost") == masterHost || string("127.0.0.1") == masterHost) && port == DBPort ) { 
		errmsg = "can't clone from self (localhost).  sources configuration may be wrong.";
		return false;
	}
	if( !conn.connect(masterHost, errmsg) )
		return false;

	string ns = client->name + ".system.namespaces";

	auto_ptr<DBClientCursor> c( conn.query(ns.c_str(), emptyObj) );
	if( c.get() == 0 ) {
		errmsg = "query failed system.namespaces";
		return false;
	}

	while( c->more() ) { 
		JSObj collection = c->next();
		Element e = collection.findElement("name");
		assert( !e.eoo() );
		assert( e.type() == String );
		const char *name = e.valuestr();
		if( strstr(name, ".system.") || strchr(name, '$') )
			continue;
		JSObj options = collection.getObjectField("options");
		if( !options.isEmpty() ) {
			string err;
			userCreateNS(name, options, err);
		}
		copy(name);
	}

	// now build the indexes
	string system_indexes = client->name + ".system.indexes";
	copy(system_indexes.c_str());

	return true;
}

bool cloneFrom(const char *masterHost, string& errmsg)
{
	Cloner c;
	return c.go(masterHost, errmsg);
}

/* --------------------------------------------------------------*/

Source::Source(JSObj o) {
	hostName = o.getStringField("host");
	sourceName = o.getStringField("source");
	uassert( !hostName.empty() );
	uassert( !sourceName.empty() );
	Element e = o.getField("syncedTo");
	if( !e.eoo() ) {
		uassert( e.type() == Date );
		syncedTo.asDate() = e.date();
	}

	JSObj dbsObj = o.getObjectField("dbs");
	if( !dbsObj.isEmpty() ) {
		JSElemIter i(dbsObj);
		while( 1 ) { 
			Element e = i.next();
			if( e.eoo() ) 
				break;
			dbs.insert( e.fieldName() );
		}
	}
}

/* Turn our C++ Source object into a JSObj */
JSObj Source::jsobj() {
	JSObjBuilder b;
	b.append("host", hostName);
	b.append("source", sourceName);
	b.appendDate("syncedTo", syncedTo.asDate());

	JSObjBuilder dbs_builder;
	for( set<string>::iterator i = dbs.begin(); i != dbs.end(); i++ ) {
		dbs_builder.appendBool(i->c_str(), 1);
	}
	b.append("dbs", dbs_builder.done());

	return b.doneAndDecouple();
}

void Source::save() { 
	JSObjBuilder b;
	b.append("host", hostName);
	b.append("source", sourceName);
	JSObj pattern = b.done();

	JSObj o = jsobj();

	stringstream ss;
	setClient("local.sources");
	//cout << o.toString() << endl;
	//cout << pattern.toString() << endl;
	int u = _updateObjects("local.sources", o, pattern, false, ss);
	assert( u == 1 );
	client = 0;
}

void Source::cleanup(vector<Source*>& v) { 
	for( vector<Source*>::iterator i = v.begin(); i != v.end(); i++ )
		delete *i;
}

static void addSourceToList(vector<Source*>&v, Source& s, vector<Source*>&old) { 
	for( vector<Source*>::iterator i = old.begin(); i != old.end();  ) {
		if( s == **i ) {
			v.push_back(*i);
			old.erase(i);
			return;
		}
		i++;
	}

	v.push_back( new Source(s) );
}

/* we reuse our existing objects so that we can keep our existing connection 
   and cursor in effect. 
*/
void Source::loadAll(vector<Source*>& v) { 
	vector<Source *> old = v;
    v.erase(v.begin(), v.end());

	setClient("local.sources");
	auto_ptr<Cursor> c = findTableScan("local.sources", emptyObj);
	while( c->ok() ) { 
		Source tmp(c->current());	
		addSourceToList(v, tmp, old);
		c->advance();
	}
	client = 0;

    for( vector<Source*>::iterator i = old.begin(); i != old.end(); i++ )
        delete *i;
}

JSObj opTimeQuery = fromjson("{getoptime:1}");

bool Source::resync(string db) {
	{
		log() << "resync: dropping database " << db << endl;
		string dummyns = db + ".";
		assert( client->name == db );
		dropDatabase(dummyns.c_str());
		setClientTempNs(dummyns.c_str());
	}

	{
		log() << "resync: cloning database " << db << endl;
		Cloner c;
		string errmsg;
		bool ok = c.go(hostName.c_str(), errmsg);
		if( !ok ) { 
			problem() << "resync of " << db << " from " << hostName << " failed " << errmsg << endl;
			throw SyncException();
		}
	}

	log() << "resync: done " << db << endl;
	dbs.insert(db);
	return true;
}

/* { ts: ..., op: <optype>, ns: ..., o: <obj> , o2: <extraobj>, b: <boolflag> } 
   You must lock dbMutex before calling.
*/
void Source::applyOperation(JSObj& op) { 
	stringstream ss;
	const char *ns = op.getStringField("ns");
	setClientTempNs(ns);

	if( client->justCreated || /* datafiles were missing.  so we need everything, no matter what sources object says */
	    !dbs.count(client->name) ) /* if not in dbs, we've never synced this database before, so we need everything */
	{
		resync(client->name);
		client->justCreated = false;
	}

	const char *opType = op.getStringField("op");
	JSObj o = op.getObjectField("o");
	if( *opType == 'i' ) { 
		// do upserts for inserts as we might get replayed more than once
		OID *oid = o.getOID();
		if( oid == 0 ) {
			_updateObjects(ns, o, o, true, ss);
		}
		else { 
			JSObjBuilder b;
			b.appendOID("_id", oid);
			_updateObjects(ns, o, b.done(), true, ss);
		}
		// theDataFileMgr.insert(ns, (void*) o.objdata(), o.objsize());
	}
	else if( *opType == 'u' ) { 
		_updateObjects(ns, o, op.getObjectField("o2"), op.getBoolField("b"), ss);
	}
	else if( *opType == 'd' ) { 
		deleteObjects(ns, o, op.getBoolField("b"));
	}
	else { 
		BufBuilder bb;
		JSObjBuilder ob;
		assert( *opType == 'c' );
		_runCommands(ns, o, ss, bb, ob);
	}
	client = 0;
}

/* note: not yet in mutex at this point. */
void Source::pullOpLog() { 
	JSObjBuilder q;
	q.appendDate("$gte", syncedTo.asDate());
	JSObjBuilder query;
	query.append("ts", q.done());
	// query = { ts: { $gte: syncedTo } }

	string ns = string("local.oplog.$") + sourceName;
	auto_ptr<DBClientCursor> c = 
		conn->query(ns.c_str(), query.done());
	if( !c->more() ) { 
		problem() << "pull:   " << ns << " empty?\n";
		sleepsecs(3);
		return;
	}

    int n = 0;
	JSObj op = c->next();
	Element ts = op.findElement("ts");
	assert( ts.type() == Date );
	OpTime t;
	t.asDate() = ts.date();
	bool initial = syncedTo.isNull();
	if( initial ) { 
		log() << "pull:   initial run\n";
        {
            dblock lk;
            applyOperation(op);
            n++;
        }
	}
	else if( t != syncedTo ) { 
		log() << "pull:   t " << t.toString() << " != syncedTo " << syncedTo.toString() << '\n';
        log() << "pull:    data too stale, halting replication" << endl;
		assert( syncedTo < t );
		throw SyncException();
	}
    else { 
        /* t == syncedTo, so the first op was applied previously, no need to redo it. */
    }

	// apply operations
	{
		dblock lk;
		while( 1 ) {
			if( !c->more() ) {
				log() << "pull:   applied " << n << " operations" << endl;
				syncedTo = t;
				save(); // note how far we are synced up to now
				break;
			}
			/* todo: get out of the mutex for the next()? */
			JSObj op = c->next();
			ts = op.findElement("ts");
			assert( ts.type() == Date );
			OpTime last = t;
			t.asDate() = ts.date();
			if( !( last < t ) ) { 
				problem() << "sync error: last " << last.toString() << " >= t " << t.toString() << endl;
				uassert(false);
			}

			applyOperation(op);
			n++;
		}
	}
}

/* note: not yet in mutex at this point. 
   returns true if everything happy.  return false if you want to reconnect.
*/
bool Source::sync() { 
	log() << "pull: from " << sourceName << '@' << hostName << endl;

	if( (string("localhost") == hostName || string("127.0.0.1") == hostName) && port == DBPort ) { 
        log() << "pull:   can't sync from self (localhost). sources configuration may be wrong." << endl;
		sleepsecs(5);
        return false;
    }

	if( conn.get() == 0 ) {
		conn = auto_ptr<DBClientConnection>(new DBClientConnection());
		string errmsg;
		if( !conn->connect(hostName.c_str(), errmsg) ) {
			resetConnection();
			log() << "pull:   cantconn " << errmsg << endl;
			return false;
		}
	}

/*
	// get current mtime at the server.
	JSObj o = conn->findOne("admin.$cmd", opTimeQuery);
	Element e = o.findElement("optime");
	if( e.eoo() ) {
		log() << "pull:   failed to get cur optime from master" << endl;
		log() << "        " << o.toString() << endl;
		return false;
	}
	uassert( e.type() == Date );
	OpTime serverCurTime;
	serverCurTime.asDate() = e.date();
*/
	pullOpLog();
	return true;
}

/* -- Logging of operations -------------------------------------*/

// cached copies of these...
NamespaceDetails *localOplogMainDetails = 0;
Client *localOplogClient = 0;

/* we write to local.opload.$main:
     { ts : ..., op: ..., ns: ..., o: ... }
   ts: an OpTime timestamp
   op: 
     'i' = insert
*/
void _logOp(const char *opstr, const char *ns, JSObj& obj, JSObj *o2, bool *bb) {
	if( strncmp(ns, "local.", 6) == 0 )
		return;

	Client *oldClient = client;
	if( localOplogMainDetails == 0 ) { 
		setClientTempNs("local.");
		localOplogClient = client;
		localOplogMainDetails = nsdetails("local.oplog.$main");
	}
	client = localOplogClient;

	/* we jump through a bunch of hoops here to avoid copying the obj buffer twice -- 
	   instead we do a single copy to the destination position in the memory mapped file.
    */

	JSObjBuilder b;
	b.appendDate("ts", OpTime::now().asDate());
	b.append("op", opstr);
	b.append("ns", ns);
	if( bb ) 
		b.appendBool("b", *bb);
	if( o2 )
		b.append("o2", *o2);
	JSObj partial = b.done();
	int posz = partial.objsize();
	int len = posz + obj.objsize() + 1 + 2 /*o:*/;

	Record *r = theDataFileMgr.fast_oplog_insert(localOplogMainDetails, "local.oplog.$main", len);

	char *p = r->data;
	memcpy(p, partial.objdata(), posz);
	*((unsigned *)p) += obj.objsize() + 1 + 2;
	p += posz - 1;
	*p++ = (char) Object;
	*p++ = 'o';
	*p++ = 0;
	memcpy(p, obj.objdata(), obj.objsize());
	p += obj.objsize();
	*p = EOO;

	client = oldClient;
}

/* --------------------------------------------------------------*/

/*
TODO:
_ source has autoptr to the cursor
_ reuse that cursor when we can
*/

void replMain() { 
	vector<Source*> sources;

	while( 1 ) { 
		{	
			dblock lk;
			Source::loadAll(sources);
		}
		
		if( sources.empty() )
			sleepsecs(20);
		
		for( vector<Source*>::iterator i = sources.begin(); i != sources.end(); i++ ) {
			Source *s = *i;	
			bool ok = false;	
			try {
				ok = s->sync();
			}
			catch( SyncException ) {
				log() << "caught SyncException, sleeping 1 minutes" << endl;
				sleepsecs(60);
			}
            catch( AssertionException ) { 
                log() << "replMain caught AssertionException, sleeping 1 minutes" << endl;
                sleepsecs(60);
            }
			if( !ok ) 
				s->resetConnection();
		}

        sleepsecs(3);
	}

	Source::cleanup(sources);
}

int debug_stop_repl = 0;

void replSlaveThread() { 
    sleepsecs(3);
	while( 1 ) { 
		try { 
			replMain();
			if( debug_stop_repl )
				break;
			sleepsecs(5);
		}
		catch( AssertionException ) { 
			problem() << "Assertion in replSlaveThread(): sleeping 5 minutes before retry" << endl;
			sleepsecs(300);
		}
	}
}

void startReplication() { 
	if( slave ) {
		log() << "slave=true" << endl;
		boost::thread repl_thread(replSlaveThread);
	}

	if( master ) {  
		log() << "master=true" << endl;
		dblock lk;
		/* create an oplog collection, if it doesn't yet exist. */
		JSObjBuilder b;
		b.append("size", 254.0 * 1000 * 1000);
		b.appendBool("capped", 1);
		setClientTempNs("local.oplog.$main");
		string err;
		JSObj o = b.done();
		userCreateNS("local.oplog.$main", o, err);
		client = 0;
	}
}
