/*
Zone A 9 miles
Zone B 6 miles
Zone C 3  miles
Zone D 1.5 miles (2,410  m)
Zone E 0.78 miles (1,260 m)
*/

/*
Time Period that users stuck along for 24 hours in zone A,B,C,D,E
Time Period that users stuck along for 8 hours in zone A,B,C,D,E
Time Period that users stuck along for 2.5 hours in zone A,B,C,D,E
Time Period that users stuck along for (48 min)0.8 hours in zone A,B,C,D,E
Time Period that users stuck along for (15 min)0.26 hours in zone A,B,C,D,E
Time Period that users stuck along for (5 min) in zone A,B,C,D,E
Time Period that users stuck along for (1 min) in zone A,B,C,D,E
Time Period that users stuck along for (20 sec) in zone A,B,C,D,E
*/
console.log('Loading function');

const doc = require('dynamodb-doc');
const date = require('date-and-time')
const geolocation = require('geolocation-utils')

const dynamo = new doc.DynamoDB();


var imeisMap = new Map();
var zones = [
    {'zone':'A','radius':14484,'points':1},
    {'zone':'B','radius':9656,'points':2},
    {'zone':'C','radius':4828,'points':3},
    {'zone':'D','radius':2414,'points':4},
    {'zone':'E','radius':1255,'points':5}
];

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });
    console.log("pullRaveltieData");



    pullRaveltieData(function callback() {
        console.log("pullRaveltieData callback");

        processRaveltieData();
        //@TODO once above function returns ImeiMaps should have latest score
        //@TODO push update to database
    });
    // var locations = [{lat: "51", lon: "4", etc:"asdf"},{lat: "51.001", lon: "4.001" , etc:"asdf"}]
    // console.log(geolocation.getBoundingBox(locations));

    

    // done(null,{"example":"asdf"});
    

};
function processRaveltieData() {
    imeisMap.forEach(function(mainImei, mainImeiKey) {
        // console.log("precheck-mainImei");
        var boundingBox = geolocation.getBoundingBox(mainImei.locations, zones[0].radius);//at least zone A
        var PrecheckBreakException = {};
        try {
            imeisMap.forEach(function(secondaryImei,secondaryImeiKey){
                if(mainImei.imei === secondaryImei.imei) return;
                // console.log("precheck-secondaryImei");
                secondaryImei.locations.forEach(function(secondaryLocation,index,array) {
                    // console.log("precheck-secondaryLocation");
                    var inside = geolocation.insideBoundingBox(secondaryLocation,boundingBox);
                    if(inside) {
                        mainImei.overlapping.push({'imei':secondaryImei.imei});
                        console.log("PrecheckBreakException");
                        throw PrecheckBreakException;//skip to next secondaryImei, not yet time to do final processing
                    }
                });
                

            });
        }catch(e){}

        //once all secondary Imeis are processed we can calculate new score for mainImei
        mainImei.overlapping.forEach(function(overlapping, index, array) { 
            console.log("mainImei.overlapping");
            console.log("locations array length"+mainImei.locations.length);
            var overlappingImei = imeisMap.get(overlapping.imei);

            var timestampOffset = 30 * 1000;//30 seconds
            var mainTimestamp = 0;
            var secondaryTimestamp = 0;
            var lastSecondaryIndexUsed = 0;
            

            mainImei.locations.sort(function(a,b){return a.timestamp - b.timestamp;});
            mainImei.locations.forEach(function(mainLocation,index,array) {
                console.log("mainImei.locations");

                mainTimestamp = mainLocation.timestamp;
                var SkipMainBreakException = {};
                var matchingSecondaryLocation;
            
                try {
                    overlappingImei.locations.sort(function(a,b){return a.timestamp - b.timestamp;});
                    overlappingImei.locations.forEach(function(secondaryLocation, secIndex, secArray) {
                        console.log("overlappingImei.locations");

                        if(secIndex <= lastSecondaryIndexUsed)return;//@todo efficiency

                        secondaryTimestamp = secondaryLocation.timestamp;

                        //find closest matching timestamp for main and secondary locations
                        if(secondaryTimestamp >= mainTimestamp) {
                            var rewindIndex = secIndex;
                            var rewindTimestamp;
                            do {
                                rewindTimestamp = secArray[rewindIndex].timestamp;
                                if(rewindTimestamp < mainTimestamp) {
                                    var forwardIndex = rewindTimestamp + 1;
                                    var forwardTimestamp = secArray[forwardIndex].timestamp;

                                    //figure which is closer
                                    var rewindOffset = mainTimestamp - rewindTimestamp;
                                    var forwardOffset = forwardTimestamp - mainTimestamp;

                                    if(forwardOffset < rewindOffset &&
                                     forwardOffset < timestampOffset) {
                                        //use forward secondary
                                        matchingSecondaryLocation = secArray[forwardIndex];
                                        lastSecondaryIndexUsed = forwardIndex;
                                    } else if(rewindOffset < forwardOffset &&
                                     rewindOffset < timestampOffset) {
                                        //use rewind secondary
                                        matchingSecondaryLocation = secArray[rewindIndex];
                                        lastSecondaryIndexUsed = rewindIndex;
                                    } else {
                                        throw SkipMainBreakException;//no close timestamps found
                                    }
                                }
                                rewindIndex = rewindIndex - 1;
                                // index > 0 || index > lastSecondaryIndexUsed
                            } while (rewindTimestamp > (mainTimestamp - timestampOffset));

                        } else if(secondaryTimestamp <= mainTimestamp) {
                           var forwardIndex = secIndex;
                            var forwardTimestamp;
                            do {
                                forwardTimestamp = secArray[forwardIndex].timestamp;
                                if(forwardTimestamp > mainTimestamp) {
                                    var rewindIndex = forwardTimestamp - 1;
                                    var rewindTimestamp = secArray[rewindIndex].timestamp;

                                    //figure which is closer
                                    var forwardOffset = forwardTimestamp - mainTimestamp;
                                    var rewindOffset = mainTimestamp - rewindTimestamp;

                                    if(forwardOffset < rewindOffset &&
                                     forwardOffset < timestampOffset) {
                                        //use forward secondary
                                        matchingSecondaryLocation = secArray[forwardIndex];
                                        lastSecondaryIndexUsed = forwardIndex;
                                    } else if(rewindOffset < forwardOffset &&
                                     rewindOffset < timestampOffset) {
                                        //use rewind secondary
                                        matchingSecondaryLocation = secArray[rewindIndex];
                                        lastSecondaryIndexUsed = rewindIndex;
                                    } else {
                                        throw SkipMainBreakException;//no close timestamps found
                                    }
                                }
                                forwardIndex = forwardIndex + 1;
                                // index > 0 || index > lastSecondaryIndexUsed
                            } while (forwardTimestamp < (mainTimestamp + timestampOffset));

                        }
                        var ZoneBreakException = {};
                        try {
                            // for now use zones but it's very expensive, so do a pre-Zone check
                            zones.forEach(function(zoneValue,zI,zA) {
                                //add attribute of location accuracy and sutract it from distance calculation
                                //geolocation
                                var distanceTo = geolocation
                                    .headingDistanceTo(mainLocation, matchingSecondaryLocation);

                                if((distanceTo - 
                                    mainLocation.accuracy - 
                                        matchingSecondaryLocation.accuracy) < zoneValue.radius) {
                                    //score points
                                    mainLocation.points += zoneValue.points;
                                    matchingSecondaryLocation.points += zoneValue.points;



                                } else {
                                    //no points
                                    throw ZoneBreakException;
                                    //when there aren't any matching big zones,
                                    //least will there be matching smaller zones
                                }
                            });//end zones

                        }catch(zonesBreakException) {}
                    });//end overlapping Imei Locations

                }catch(skipMainBreakException) {}
            });//end main Imei Locations

        });//end main Imei Overlapping

        //once finish processing delete overlapping imei's for current zone
        mainImei.overlapping = [];
            
        mainImei.locations = null;

        console.log(JSON.stringify(mainImei));
        //discard mainImei and update to database but increase secondaryImei score too
        imeisMap.delete(mainImeiKey);

    });//end imeisMap

};

function pullRaveltieData(callback) {

    const now = new Date();
    var last24Hours = date.addDays(now,-1);

    //get all locations/scores of all imeis for last 24 hours
    var scan = {
      TableName : 'raveltie',
      Limit : 30,
      //FilterExpression: '',//'#ts > :greatherthan',
      //ExpressionAttributeValues: {
        //':greatherthan': last24Hours.getTime().toString()
      //},
      //ExpressionAttributeNames : {}//'#ts':'timestamp'}
    };
    
    scanning(scan, callback);

};
function scanning(scan,callback) {

    dynamo.scan(scan, function(err, data) {
        console.log("dynamo.scan");
        if (err) {
            console.log(err);
        } else {
            // console.log("last evaluated: "+JSON.stringify(data.LastEvaluatedKey));
            // console.log(JSON.stringify(data));
            var imeisArray = data.Items;
            imeisArray.forEach(function(value, index, array) {
                // console.log("imeisArray.forEach");
                var imeiMapItem = null;
                if(imeisMap.has(value.imei)) {
                    imeiMapItem = imeisMap.get(value.imei);
                } else {
                    imeisMap.set(
                        value.imei,{'imei':value.imei,'score':0,'locations':[],'overlapping':[]});
                    imeiMapItem = imeisMap.get(value.imei);
                }

                if(value.timestamp === 'score') {
                    imeiMapItem.score = value.score;
                } else {
                    imeiMapItem.locations.push(
                        {'lat':Number(value.lat), 'lon':Number(value.lon),
                        'accuracy':Number(value.accuracy),
                        'timestamp':Number(value.timestamp)});
                    // console.log( JSON.stringify(imeiMapItem.locations));
                }
            });
            if(typeof data.LastEvaluatedKey != "undefined") {
                scan.ExclusiveStartKey = data.LastEvaluatedKey;
                scanning(scan,callback);
            } else {
                callback();
            }
        }
    });
};



    //calculation equals to distance from user A time x to user B time x minus GPS accuracy minus zone
    // result less than 0 is inside zone, greater than 0 is outside zone.


    //scan all locations by imei, create a inside bounds, and check distance to 
    //locations and match their IMEI, stop iterating that IMEI and keep it for later processing
    //check distance for all zones so total period score can be calculated?
    //the bounding box matches another IMEI's location, plus margin of zone A.
    //keep IMEI's
    //do that for every bounding box of every IMEI
    //then cross reference every locations on both IMEI's collected.
    //check time spent inside the zone or average it?
    //take into account chronological crosses of the fence times?
    //average boost original iterating IMEI with score from second IMEI if bigger?
    //result score for IMEI
    //possible algorithm glitch is starting score of IMEI's and ending score
    //which one is used? for calculating? previous period? 24 hours?