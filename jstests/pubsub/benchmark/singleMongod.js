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

var sum = function(array) {
    var sum = 0;
    for (var i=0; i<array.length; i++)
        sum += array[i]; 
    return sum;
}

var average = function(array) {
    return sum(array)/array.length; 
}


/**
 * Use benchRun to measure polls/second with an increasing number of polling clients
 **/ 
var poll = function(messageSize) {
    var subA = ps.subscribe("A"); 
    var ops = [{op : "command",
                ns : "test",
                command : { poll : subA }
              }];
    var benchArgs = {ops : ops, host : db.getMongo().host, seconds : timeSecs, parallel : 1};
    var subReadySignal = ps.subscribe("readySignal"); 

    // set up parallel shells to be simultaneously polling
    // cannot be done through benchRun itself because requires
    // each polling client to use its own subscriptionId
    for (var numOtherClients = 0; numOtherClients < maxClients; numOtherClients++) {
        var shells = [];
        var otherClients = numOtherClients;
        for (var i=0; i<otherClients; i++) {
            shells[i] = startPollShell();
        }

        // if we want to be simultaneously publishing messages,
        // set up a parallel shell to do so
        if (messageSize) {
            shells[otherClients] = startPublishShell(messageSize, 1);
            otherClients++;
        }

        // wait for ready signals from all shells
        var readyCount = 0;
        while (readyCount < otherClients) { 
            var res = ps.poll(subReadySignal, 1000 * 60 * 10);
            if (res["messages"][subReadySignal.str])
                readyCount += res["messages"][subReadySignal.str]["readySignal"].length;
        }

        // signal to all shells that they can start their benchrun tests
        ps.publish("startSignal", { start : 1 });

        // start our own benchRun test
        before = db.serverStatus()["opcounters"]["command"];
        stats["client"]["" + numOtherClients] = benchRun(benchArgs)["averageCommandsPerSecond"];
        after = db.serverStatus()["opcounters"]["command"];
        stats["server"]["" + numOtherClients] = (after - before)/timeSecs;

        // wait for all shells to terminate before starting next batch of tests
        for (var i=0; i<otherClients; i++)
            shells[i]();
    }

    printStats(stats);
}
