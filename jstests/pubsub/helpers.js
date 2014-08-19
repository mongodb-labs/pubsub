// use shell helper for pubsub calls
var ps = db.PS();

// print helper                                                                                     
var printStats = function(stats) {                                                                  
    print("numClients\tcommands/second/client\tcommands/second/server");                                                      
    for (var numClients in stats["client"]) {                                                       
        if (stats["client"].hasOwnProperty(numClients)) {                                           
            print(numClients + "\t" +                                                               
                  stats["client"][numClients] + "\t" +                                              
                  stats["server"][numClients]                                                       
                 );                                                                                 
        }                                                                                           
    }                                                                                               
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
                                                                                                    
// maximum number of concurrent clients (aka parallel threads in benchRun)                          
var maxClients = 20;
                                                                                                    
// time to run each parallel thread in benchRun                                                     
var timeSecs = 10; 
                                                                                                    
// used to compare number of server commands seen before and after benchRun for load stats          
var before; var after;                                                                              
                                                                                                    
// used to store and print stats from each test                                                     
var stats = { client : {}, server : {} };                                                           
                                                                                                    
// test commands under light and heavy message loads                                                
var lightMessage = {a : 1};                                                                         
var heavyMessage = {};                                                                              
for (var i=0; i<1000; i++){                                                                          
    heavyMessage[i] = lightMessage;                                                                 
}

var publishLight = function() {                                                                     
    publish("light"); 
}                                                                                                   
                                                                                                    
var publishHeavy = function() {                                                                     
    publish("heavy");                                                                          
}                                                                                                   
                                                                                                    
var pollEmpty = function() {                                                                        
    poll(null);                                                                                     
}                                                                                                   
                                                                                                    
var pollLight = function() {                                                                        
    poll("light");                                                                             
}                                                                                                   
                                                                                                    
var pollHeavy = function() {                                                                        
    poll("heavy");                                                                             
}

