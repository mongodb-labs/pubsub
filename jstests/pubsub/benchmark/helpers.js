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
                                                                                                    
                                                                                                    
// maximum number of concurrent clients (aka parallel threads in benchRun)                          
var maxClients = 25;                                                                                 
                                                                                                    
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
    publish(lightMessage);                                                                          
}                                                                                                   
                                                                                                    
var publishHeavy = function() {                                                                     
    publish(heavyMessage);                                                                          
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


function sizeof(_1){
    var _2=[_1];
    var _3=0;
    for(var _4=0;_4<_2.length;_4++){
        switch(typeof _2[_4]){
            case "boolean":
                _3+=4;
                break;
            case "number":
                _3+=8;
                break;
            case "string":
                _3+=2*_2[_4].length;
                break;
            case "object":
                if(Object.prototype.toString.call(_2[_4])!="[object Array]"){
                    for(var _5 in _2[_4]){
                        _3+=2*_5.length;
                    }
                }
            for(var _5 in _2[_4]){
                var _6=false;
                for(var _7=0;_7<_2.length;_7++){
                    if(_2[_7]===_2[_4][_5]){
                        _6=true;
                        break;
                    }
                }
                if(!_6){
                    _2.push(_2[_4][_5]);
                }
            }
        }
    }
    return _3;
};
