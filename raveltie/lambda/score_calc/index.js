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


var imeisMap = new Map()

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });

    const now = new Date();
    var last24Hours = date.addDays(now,-1);

    //get all locations/scores of all imeis for last 24 hours
    var scan = {
      TableName : 'raveltie',
      FilterExpression: '#ts > :greatherthan',
      ExpressionAttributeValues: {
        ':greatherthan': last24Hours.getTime().toString()
      },
      ExpressionAttributeNames : {'#ts':'timestamp'}
    };

    dynamo.scan(scan, function(err, data) {
       if (err) {
        console.log(err);
       } else {
        //console.log(data);
        var imeisArray = data.Items;

        imeisArray.forEach(
            function(value, index, array) {
                var imeiMapItem = null;
                if(imeisMap.has(value.imei)) {
                    imeiMapItem = imeisMap.get(value.imei);
                } else {
                    imeisMap.set(
                        value.imei,{'imei':value.imei,'score':0,'locations':[]});
                    imeiMapItem = imeisMap.get(value.imei);
                }

                if(value.timestamp === 'score') {
                    imeiMapItem.score = value.score;
                } else {
                    imeiMapItem.locations.push(
                        {'lat':value.lat, 'lon':value.lon, 'timestamp':value.timestamp});
                    console.log( JSON.stringify(imeiMapItem.locations));
                }
            });
        
       }
    });

    
    //geolocation
    const location1 = {lat: 51, lon: 4}
    const location2 = {lat: 51.001, lon: 4.001 }
    console.log(geolocation.headingDistanceTo(location1, location2)) 
    

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