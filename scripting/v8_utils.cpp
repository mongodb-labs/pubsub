// v8_utils.cpp

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "v8_utils.h"
#include <iostream>
#include <map>
#include <sstream>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <boost/smart_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>

using namespace std;
using namespace v8;

namespace mongo {

    Handle<v8::Value> Print(const Arguments& args) {
        bool first = true;
        for (int i = 0; i < args.Length(); i++) {
            HandleScope handle_scope;
            if (first) {
                first = false;
            } else {
                printf(" ");
            }
            v8::String::Utf8Value str(args[i]);
            printf("%s", *str);
        }
        printf("\n");
        return v8::Undefined();
    }

    std::string toSTLString( const Handle<v8::Value> & o ){
        v8::String::Utf8Value str(o);    
        const char * foo = *str;
        std::string s(foo);
        return s;
    }

    std::string toSTLString( const v8::TryCatch * try_catch ){
        
        stringstream ss;
        
        //while ( try_catch ){ // disabled for v8 bleeding edge
            
            v8::String::Utf8Value exception(try_catch->Exception());
            Handle<v8::Message> message = try_catch->Message();
            
            if (message.IsEmpty()) {
                ss << *exception << endl;
            } 
            else {
                
                v8::String::Utf8Value filename(message->GetScriptResourceName());
                int linenum = message->GetLineNumber();
                ss << *filename << ":" << linenum << " " << *exception << endl;
                
                v8::String::Utf8Value sourceline(message->GetSourceLine());
                ss << *sourceline << endl;
                
                int start = message->GetStartColumn();
                for (int i = 0; i < start; i++)
                    ss << " ";
                
                int end = message->GetEndColumn();
                for (int i = start; i < end; i++)
                    ss << "^";
                
                ss << endl;
            }    
            
            //try_catch = try_catch->next_;
        //}
        
        return ss.str();
    }


    std::ostream& operator<<( std::ostream &s, const Handle<v8::Value> & o ){
        v8::String::Utf8Value str(o);    
        s << *str;
        return s;
    }

    std::ostream& operator<<( std::ostream &s, const v8::TryCatch * try_catch ){
        HandleScope handle_scope;
        v8::String::Utf8Value exception(try_catch->Exception());
        Handle<v8::Message> message = try_catch->Message();
    
        if (message.IsEmpty()) {
            s << *exception << endl;
        } 
        else {

            v8::String::Utf8Value filename(message->GetScriptResourceName());
            int linenum = message->GetLineNumber();
            cout << *filename << ":" << linenum << " " << *exception << endl;

            v8::String::Utf8Value sourceline(message->GetSourceLine());
            cout << *sourceline << endl;

            int start = message->GetStartColumn();
            for (int i = 0; i < start; i++)
                cout << " ";

            int end = message->GetEndColumn();
            for (int i = start; i < end; i++)
                cout << "^";

            cout << endl;
        }    

        //if ( try_catch->next_ ) // disabled for v8 bleeding edge
        //    s << try_catch->next_;

        return s;
    }


    Handle<v8::Value> Version(const Arguments& args) {
        return v8::String::New(v8::V8::GetVersion());
    }

    void ReportException(v8::TryCatch* try_catch) {
        cout << try_catch << endl;
    }
    
    v8::Handle< v8::Context > baseContext_;
    v8::Locker *locker_;
    
    class JSThreadConfig {
    public:
        JSThreadConfig( const Arguments &args ) : started_(), done_() {
            jsassert( args.Length() > 0, "need at least one argument" );
            jsassert( args[ 0 ]->IsFunction(), "first argument must be a function" );
            Local< Function > f = Function::Cast( *args[ 0 ] );
            f_ = Persistent< Function >::New( f );
            for( int i = 1; i < args.Length(); ++i )
                args_.push_back( Persistent< Value >::New( args[ i ] ) );
        }
        ~JSThreadConfig() {
            f_.Dispose();
            for( vector< Persistent< Value > >::iterator i = args_.begin(); i != args_.end(); ++i )
                i->Dispose();
            returnData_.Dispose();
        }
        void start() {
            jsassert( !started_, "Thread already started" );
            JSThread jt( *this );
            thread_.reset( new boost::thread( jt ) );
            started_ = true;
        }
        void join() {
            jsassert( started_ && !done_, "Thread not running" );
            Unlocker u;
            thread_->join();
            done_ = true;
        }
        Local< Value > returnData() {
            if ( !done_ )
                join();
            return Local< Value >::New( returnData_ );
        }
    private:
        class JSThread {
        public:
            JSThread( JSThreadConfig &config ) : config_( config ) {}
            void operator()() {
                Locker l;
                Context::Scope context_scope( baseContext_ );
                HandleScope handle_scope;
                boost::scoped_array< Persistent< Value > > argv( new Persistent< Value >[ config_.args_.size() ] );
                for( unsigned int i = 0; i < config_.args_.size(); ++i )
                    argv[ i ] = Persistent< Value >::New( config_.args_[ i ] );
                Local< Value > ret = config_.f_->Call( Context::GetCurrent()->Global(), config_.args_.size(), argv.get() );
                for( unsigned int i = 0; i < config_.args_.size(); ++i )
                    argv[ i ].Dispose();
                config_.returnData_ = Persistent< Value >::New( ret );
            }
        private:
            JSThreadConfig &config_;
        };
        
        bool started_;
        bool done_;
        Persistent< Function > f_;
        vector< Persistent< Value > > args_;
        auto_ptr< boost::thread > thread_;
        Persistent< Value > returnData_;
    };
    
    Handle< Value > ThreadInit( const Arguments &args ) {
        Handle<Object> it = args.This();
        // NOTE I believe the passed JSThreadConfig will never be freed.  If this
        // policy is changed, JSThread may no longer be able to store JSThreadConfig
        // by reference.
        it->Set( String::New( "_JSThreadConfig" ), External::New( new JSThreadConfig( args ) ) );
        return Undefined();
    }
    
    JSThreadConfig *thisConfig( const Arguments &args ) {
        Local< External > c = External::Cast( *(args.This()->Get( String::New( "_JSThreadConfig" ) ) ) );
        JSThreadConfig *config = (JSThreadConfig *)( c->Value() );
        return config;
    }
    
    Handle< Value > ThreadStart( const Arguments &args ) {
        thisConfig( args )->start();
        return Undefined();
    }
    
    Handle< Value > ThreadJoin( const Arguments &args ) {
        thisConfig( args )->join();
        return Undefined();
    }
    
    Handle< Value > ThreadReturnData( const Arguments &args ) {
        return thisConfig( args )->returnData();
    }

    Handle< Value > ThreadInject( const Arguments &args ) {
        jsassert( args.Length() == 1 , "threadInject takes exactly 1 argument" );
        jsassert( args[0]->IsObject() , "threadInject needs to be passed a prototype" );
        
        Local<v8::Object> o = args[0]->ToObject();
        
        o->Set( String::New( "init" ) , FunctionTemplate::New( ThreadInit )->GetFunction() );
        o->Set( String::New( "start" ) , FunctionTemplate::New( ThreadStart )->GetFunction() );
        o->Set( String::New( "join" ) , FunctionTemplate::New( ThreadJoin )->GetFunction() );
        o->Set( String::New( "returnData" ) , FunctionTemplate::New( ThreadReturnData )->GetFunction() );
        
        return v8::Undefined();    
    }

    void installFork( Handle<v8::Object>& global, Handle<v8::Context> &context ) {
        locker_ = new v8::Locker;
        baseContext_ = context; // only expect to use  this in shell
        global->Set( v8::String::New( "_threadInject" ), FunctionTemplate::New( ThreadInject )->GetFunction() );
    }

}
