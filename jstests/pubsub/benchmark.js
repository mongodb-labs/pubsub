load("jstests/pubsub/helpers.js");
load("src/mongo/shell/synchronized.js");

var parseAddresses = function(_addrs) {
    var addrs = [];
    for (var i=0; i<_addrs.length; i++) {
        var host = _addrs[i].substring(0, _addrs[i].indexOf(":")); 
        var port = _addrs[i].substring(_addrs[i].indexOf(":")+1, _addrs[i].length);
        addrs[i] = { host : host, port : port }; 
    }
    return addrs;
}

/**
 * Use benchRun to measure publishes/second with an increasing number of publishing clients
 */
var publish = function(_messageSize, _addrs) {

    var addrs = parseAddresses(_addrs);
    var messageSize = messageSize || "light";

    // set up preSync and postSync functions for the publish SynchronizedJobs
    var preSync = 'load("jstests/pubsub/helpers.js");' +
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
        for (var i=1; i<=numParallel; i++) {
            // round robin the jobs across the hosts
            var addr = addrs[i%addrs.length];
            runner.addJob(new SynchronizedJob(addr.host, addr.port, preSync, postSync));
        }
        runner.start();
        stats["server"]["" + numParallel] = sum(runner.returnVals);
        stats["client"]["" + numParallel] = average(runner.returnVals); 
  
        printStats(stats);
    }

    printStats(stats);
}

/**
 * Use benchRun to measure polls/second with an increasing number of polling clients
 */
var poll = function(_messageSize, _host, _port) {

    var addrs = parseAddresses(_addrs);
    var messageSize = messageSize || "light";

    // set up preSync and postSync functions for the publish SynchronizedJob
    if(_messageSize == "light" || _messageSize == "heavy"){
        var publishPreSync = 'load("jstests/pubsub/helpers.js");' +
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
    var pollPreSync = 'load("jstests/pubsub/helpers.js");' +
                      'var ps = db.PS();' +
                      'var subA = ps.subscribe("A");' + 
                      'var ps = [{ op: "command", ns: "test", command: { poll: subA } }];';
    var pollPostSync = 'var res = (benchRun(benchArgs));' +
                       '$res["averageCommandsPerSecond"]$';

    // run the tests
    for (var numParallel = 1; numParallel <= maxClients; numParallel++) {
        var runner = new SynchronizedRunner();

        for (var i=1; i<=numParallel; i++) {
            // round robin the jobs across the hosts
            var addr = addrs[i%addrs.length];
            runner.addJob(new SynchronizedJob(addr.host, addr.port, preSync, postSync));
        }

        if (publishPreSync) {
            var addr = addrs[0];
            runner.addJob(new SynchronizedJob(addr.host, addr.port, publishPreSync, publishPostSync));
        }

        runner.start();
        stats["server"]["" + numParallel] = sum(runner.returnVals);
        stats["client"]["" + numParallel] = average(runner.returnVals); 
    }

    printStats(stats);
}
