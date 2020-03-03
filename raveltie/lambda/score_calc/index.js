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
    {'zone':'A','radius':14484},
    {'zone':'B','radius':9656},
    {'zone':'C','radius':4828},
    {'zone':'D','radius':2414},
    {'zone':'E','radius':1255}
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
            var overlappingImei = imeisMap.get(overlapping.imei);
            console.log("locations array length"+mainImei.locations.length);
            mainImei.locations.sort(function(a,b){return a.timestamp - b.timestamp;});
            mainImei.locations.forEach(function(location,index,array) {
                console.log("mainImei.locations");
                var LocationBreakException = {};
                var previousTimestamp = 0;
                try {
                    overlappingImei.locations.sort(function(a,b){return a.timestamp - b.timestamp;});
                    overlappingImei.locations.forEach(function(secondaryLocation, secIndex, secArray) {
                        // console.log("overlappingImei.locations");

                        //match timestamp
                        if(previousTimestamp < secondaryLocation.timestamp) {
                            // console.log("right");
                            previousTimestamp = secondaryLocation.timestamp;
                        } else {
                            console.log("wrongss");
                            throw LocationBreakException;
                        }
                        //@TODO find closest matching timestamp for main and secondary locations
                        //@TODO add attribute of location accuracy and sutract it from distance calculation
                        //geolocation
                        // console.log(geolocation.headingDistanceTo(location, secondaryLocation));
                        // throw LocationBreakException;
                        
                        // var ZoneBreakException = {};
                        // try {
                        //     //@TODO for now use zones but it's very expensive, so do a pre-Zone check
                        //     zones.forEach(function(zoneValue,zI,zA) {



                        //         throw ZoneBreakException;
                        //     });
                        // }catch(e) {

                        // }
                    });
                }catch(e) {

                }
            });


        });
        //once finish processing delete overlapping imei's for current zone
        mainImei.overlapping = [];
            
        //discard mainImei and delete from database but increase secondaryImei score too
        imeisMap.delete(mainImeiKey);

    });
}

// function calculateScoreForZone(mainImei,locations, zone) { 

// };          


function pullRaveltieData(callback) {
    const now = new Date();
    var last24Hours = date.addDays(now,-1);

    //get all locations/scores of all imeis for last 24 hours
    var scan = {
      TableName : 'raveltie',
      Limit : 30
      //FilterExpression: '',//'#ts > :greatherthan',
      //ExpressionAttributeValues: {
        //':greatherthan': last24Hours.getTime().toString()
      //},
      //ExpressionAttributeNames : {}//'#ts':'timestamp'}
    };
    var LastEvaluatedKey = {};
    var maxPages = 1;

    while(typeof LastEvaluatedKey != "undefined") {
        
        dynamo.scan(scan, function(err, data) {
            console.log("dynamo.scan");
           if (err) {
            console.log(err);
           } else {
            LastEvaluatedKey = data.LastEvaluatedKey;
            console.log("last evaluated: "+JSON.stringify(LastEvaluatedKey));
            // LastEvaluatedKey = data.LastEvaluatedKey;

            // console.log(JSON.stringify(data));
            var imeisArray = data.Items;

            // imeisArray.forEach(function(value, index, array) {
            //     // console.log("imeisArray.forEach");
            //     var imeiMapItem = null;
            //     if(imeisMap.has(value.imei)) {
            //         imeiMapItem = imeisMap.get(value.imei);
            //     } else {
            //         imeisMap.set(
            //             value.imei,{'imei':value.imei,'score':0,'locations':[],'overlapping':[]});
            //         imeiMapItem = imeisMap.get(value.imei);
            //     }

            //     if(value.timestamp === 'score') {
            //         imeiMapItem.score = value.score;
            //     } else {
            //         imeiMapItem.locations.push(
            //             {'lat':Number(value.lat), 'lon':Number(value.lon),
            //             'accuracy':Number(value.accuracy),
            //             'timestamp':Number(value.timestamp)});
            //         // console.log( JSON.stringify(imeiMapItem.locations));
            //     }
            // });
            // callback();
           }
        });
    }

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