console.log('Loading function');

const doc = require('dynamodb-doc');
const datetime = require('date-and-time')
const geolocation = require('geolocation-utils')

const dynamo = new doc.DynamoDB();


/**
 * Demonstrates a simple HTTP endpoint using API Gateway. You have full
 * access to the request and response payload, including headers and
 * status code.
 *
 * To scan a DynamoDB table, make a GET request with the TableName as a
 * query string parameter. To put, update, or delete an item, make a POST,
 * PUT, or DELETE request respectively, passing in the payload to the
 * DynamoDB API as a JSON body.
 */
exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });


    //Zone A 9 miles
    //Zone B 6 miles
    //Zone C 3  miles
    //Zone D 1.5 miles (2,410  m)
    //Zone E 0.78 miles (1,260 m)


    //high score - 
    //find out users who stuck along other users for 24 hours in zone A,B,C,D,E
    //find out users who stuck along other users for 8 hours in zone A,B,C,D,E
    //find out users who stuck along other users for 2.5 hours in zone A,B,C,D,E
    //find out users who stuck along other users for (48 min)0.8 hours in zone A,B,C,D,E
    //find out users who stuck along other users for (15 min)0.26 hours in zone A,B,C,D,E
    //find out users who stuck along other users for (5 min) in zone A,B,C,D,E
    //find out users who stuck along other users for (1 min) in zone A,B,C,D,E
    //find out users who stuck along other users for (20 sec) in zone A,B,C,D,E
    //low score - 

    //calculation equals to distance from user A time x to user B time x minus GPS accuracy minus zone
    // result less than 0 is inside zone, greater than 0 is outside zone.


    //scan all locations by imei, create a inside bounds, and check distance to 
    //locations and match their IMEI and keep it for later processing
    //geofence distance for different zones? 
    //(A, B, C, D) (1600 4hr, 800, 400, 200)
    //the zone matches a location inside of another IMEI, plus margin of zone A.
    //keep IMEI's
    //do that for every bounding box of every IMEI
    //then cross reference every locations on both IMEI's
    //check time spent inside the zone or average it?
    //take into account chronological crosses of the fence times?
    //average boost original iterating IMEI with score from second IMEI if bigger?
    //result score for IMEI
    //possible algorithm glitch is starting score of IMEI's and ending score
    //which one is used? for calculating? previous period? 24 hours?

    
    //geolocation
    const location1 = {lat: 51, lon: 4}
    const location2 = {lat: 51.001, lon: 4.001 }
    console.log(geolocation.headingDistanceTo(location1, location2)) 
    
    

    switch (event.httpMethod) {
        case 'DELETE':
            dynamo.deleteItem(JSON.parse(event.body), done);
            break;
        case 'GET':
            dynamo.scan({ TableName: event.queryStringParameters.TableName }, done);
            break;
        case 'POST':
            dynamo.putItem(JSON.parse(event.body), done);
            break;
        case 'PUT':
            dynamo.updateItem(JSON.parse(event.body), done);
            break;
        default:
            done(new Error(`Unsupported method "${event.httpMethod}"`));
    }
};
