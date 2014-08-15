load("jstests/pubsub/benchmark/helpers.js");
load("src/mongo/shell/synchronized.js");

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

/**
 * Use benchRun to measure publishes/second with an increasing number of publishing clients
 */
var publish = function(_messageSize, _host, _port) {

    var host = _host || "localhost";
    var port = _port || 27017;
    var messageSize = messageSize || "light";

    // set up preSync and postSync functions for the publish SynchronizedJobs
    var preSync = 'load("jstests/pubsub/benchmark/helpers.js");' +
        'var ops = [{ op: "command", ns: "test", command: { publish: "A" } }];';
    if (messageSize == "light")
        preSync += 'ops[0]["command"]["message"] = lightMessage;';
    else if (messageSize == "heavy")
        preSync += 'ops[0]["command"]["message"] = heavyMessage;';
    else{
        print("unknown message size " + messageSize);
        return;
    }
    preSync += 'var benchArgs = {ops: ops, host: db.getMongo().host, seconds: timeSecs, parallel: 1};';
    var postSync = 'var res = (benchRun(benchArgs));' +
                   '$res["averageCommandsPerSecond"]$';

    // run the tests
    for (var numParallel = 1; numParallel <= maxClients; numParallel++) {
        var runner = new SynchronizedRunner();
        for (var i=0; i<numParallel; i++)
            runner.addJob(new SynchronizedJob(host, port, preSync, postSync));
        runner.start();
        stats["server"]["" + numParallel] = sum(runner.returnVals);
        stats["client"]["" + numParallel] = average(runner.returnVals); 
    }

    printStats(stats);
}

/**
 * Use benchRun to measure polls/second with an increasing number of polling clients
 */
var poll = function(_messageSize, _host, _port) {

    var host = _host || "localhost";
    var port = _port || 27017;

    // set up preSync and postSync functions for the publish SynchronizedJob
    if(_messageSize == "light" || _messageSize == "heavy"){
        var publishPreSync = 'load("jstests/pubsub/benchmark/helpers.js");' +
            'var ops = [{ op: "command", ns: "test", command: { publish: "A" } }];';
        if (messageSize == "light")
            publishPreSync += 'ops[0]["command"]["message"] = lightMessage;';
        else if (messageSize == "heavy")
            publishPreSync += 'ops[0]["command"]["message"] = heavyMessage;';
        else{
            print("unknown message size " + messageSize);
            return;
        }
        publishPreSync += 'var benchArgs = {ops: ops, host: db.getMongo().host, seconds: timeSecs, parallel: 1};';
        var publishPostSync = 'var res = (benchRun(benchArgs));' +
                       '$res["averageCommandsPerSecond"]$';
    }

    // set up preSync and postSync functions for the poll SynchronizedJobs
    var pollPreSync = 'load("jstests/pubsub/benchmark/helpers.js");' +
                      'var ps = db.PS();' +
                      'var subA = ps.subscribe("A");' + 
                      'var ps = [{ op: "command", ns: "test", command: { poll: subA } }];';
    var pollPostSync = 'var res = (benchRun(benchArgs));' +
                       '$res["averageCommandsPerSecond"]$';

    // run the tests
    for (var numParallel = 1; numParallel <= maxClients; numParallel++) {
        var runner = new SynchronizedRunner();
        for (var i=0; i<numParallel; i++)
            runner.addJob(new SynchronizedJob(host, port, pollPreSync, pollPostSync));
        if (publishPreSync)
            runner.addJob(new SynchronizedJob(host, port, publishPreSync, publishPostSync));

        runner.start();
        stats["server"]["" + numParallel] = sum(runner.returnVals);
        stats["client"]["" + numParallel] = average(runner.returnVals); 
    }

    printStats(stats);
}
