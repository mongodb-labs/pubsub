/**
 * For testing SynchronizedRunner and SynchronizedJob
 */
var test = function(){
    var runner = new SynchronizedRunner();

    var preSync = 'print("preSync done!");';
    var postSync = 'print("postSync done!"); $"im the return value from job1"$';
    var host = 'localhost';
    var port = 27017;

    var job1 = new SynchronizedJob(host, port, preSync, postSync);
    runner.addJob(job1);

    runner.start();

    for(var i=0; i<runner.returnVals.length; i++){
        printjson(runner.returnVals[i]);
    }
}

var LONGPOLL = 1000 * 60 * 10 // 10 minutes

/**
 * SynchronizedJob
 * Contains a job to be run by SynchronizedRunner 
 */
SynchronizedJob = function(host, port, preSync, postSync){
    this.host = host || "localhost";
    this.port = port || 27017;
    this.preSync = preSync || "";
    this.postSync = postSync || "";
    this.runnerId = null;
    this.jobId = null;
    this.toReturn = {ok:1};
    
    if (postSync.lastIndexOf("$") > 0) {
        this.postSync = this.postSync.substring(0, this.postSync.lastIndexOf("$"));
        this.toReturn = this.postSync.substring(this.postSync.lastIndexOf("$")+1, this.postSync.length);
        this.postSync = this.postSync.substring(0, this.postSync.lastIndexOf("$")); 
    }
    else if (preSync.lastIndexOf("$") > 0) {
        this.preSync = this.preSync.substring(0, this.preSync.lastIndexOf("$"));
        this.toReturn = this.preSync.substring(this.preSync.lastIndexOf("$")+1, this.preSync.length);
        this.preSync = this.preSync.substring(0, this.preSync.lastIndexOf("$")); 
    }

}

SynchronizedJob.prototype.run = function() {

    var init = 'var ps = db.PS();';

    var sync =
        'var subStartSignal = ps.subscribe("startSignal_' + this.runnerId.str + '");' +
        'ps.publish("readySignal_' + this.runnerId.str + '", {ok:1});' +
        'while (!ps.poll(subStartSignal, 1000 * 60 * 10)["messages"][subStartSignal.str]) {};'; 

    var end =
        'var subTerminateSignal = ps.subscribe("terminateSignal_' + this.runnerId.str + '");' +
        // send an end signal every second until we are told to terminate
//        'while (!ps.poll(subTerminateSignal, 1000)["messages"][subTerminateSignal.str]) {' +
            'printjson(ps.publish("endSignal_' + this.runnerId.str + '", ' + 
                                 '{ return :' + this.toReturn + ',' +
                                   'jobId :' + this.jobId + '}));' +
            'print("sent end signal from' + this.jobId + '");' +
 //       '}' +
        'print("got terminate signal");' + 
        'ps.unsubscribeAll();';

    var eval = init + this.preSync + sync + this.postSync + end;
    var pid = startMongoProgramNoConnect("mongo",
                                         "--host", this.host,
                                         "--port", this.port,
                                         "--eval", eval);
    return pid;
}



/**
 * SynchronizedRunner
 * Runs a set of SynchronizedJobs 
 */
SynchronizedRunner = function() {
    this.jobs = [];
    this.pids = [];
    this.runnerId = new ObjectId();
    this.returnVals = [];
}

SynchronizedRunner.prototype.addJob = function(job) {
    job.runnerId = this.runnerId;
    job.jobId = this.jobs.length;
    this.jobs.push(job);
}

SynchronizedRunner.prototype.start = function() {

    if (this.jobs.length < 1){
        print("Error, must add at least 1 job to SynchronizedRunner");
        return;
    }

    var ps = db.PS();

    // prepare to collect signals from each job 
    var subReadySignal = ps.subscribe("readySignal_" + this.runnerId.str);
    var subEndSignal = ps.subscribe("endSignal_" + this.runnerId.str);

    // start each job
    for (var i=0; i<this.jobs.length; i++)
        this.pids[i] = this.jobs[i].run();

    // collect all the jobs ready signals
    var count = 0;
    while (count < this.jobs.length) {
        var res = ps.poll(subReadySignal, LONGPOLL);
        if (res["messages"][subReadySignal.str]){
            count += res["messages"][subReadySignal.str]["readySignal_" + this.runnerId.str].length;
        }
    }

    // send start signal to all jobs
    ps.publish("startSignal_" + this.runnerId.str, {ok:1});

    // collect all the jobs end signals
     while (this.unfinishedJobs()) {
        print(">>>>>>>>>>>>>>>>>> waiting for unfinished jobs <<<<<<<<<<<<<<<<<<<<");
        var res = ps.poll(subEndSignal, LONGPOLL);
        if (res["messages"][subEndSignal.str]){
            var messages = res["messages"][subEndSignal.str]["endSignal_" + this.runnerId.str];
            for (var i=0; i<messages.length; i++) {
                this.returnVals[messages[i]["jobId"]] = messages[i]["return"]; 
                this.jobs[messages[i]["jobId"]] = 1; // mark job as finished
                print("got end signal for" + messages[i]["jobId"] );
            }
        }
    }

    ps.publish("terminateSignal_" + this.runnerId.str, {ok:1});
    print("sent terminate signal\n\n"); 

    // terminate all jobs   
    for (var i=0; i<this.jobs.length; i++) {
        waitProgram(this.pids[i]);
        print("collected pid " + this.pids[i] +
              " for job " + i +
              " on " +  this.jobs[i].host +
              ":" + this.jobs[i].port);
    } 

    print("terminated synchronized runner");

}

SynchronizedRunner.prototype.unfinishedJobs = function() {
    for (var i=0; i<this.jobs.length; i++) {
        if (this.jobs[i] != 1)
            return true;
    }
    return false;
}

