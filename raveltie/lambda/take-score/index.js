console.log('Loading function');

const doc = require('dynamodb-doc');

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
    console.log(event)
    const done = (err, res) => {
      var response = {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
      }
      console.log(response)
      callback(null, response);
    }

    pullRaveltieScore(event,done);

};

function pullRaveltieScore(event,done) {

    //get all locations/scores of all imeis for last 24 hours
    var query = {
      TableName : 'raveltie',
      KeyConditionExpression: '#imei = :imei and #ts = :score',
      ExpressionAttributeValues: {
        ':score': 'score',
        ':imei': event.queryStringParameters.imei
      },
      ExpressionAttributeNames : {
      	'#ts':'timestamp',
      	'#imei':'imei'}
    };

    dynamo.query(query, function(err, data) {
       if (err) {
        console.err(err);
       	done(new Error(`Generic Error`))
       } else {
       	console.log(data.Items)
        if(data.Items.length > 0) {
          done(null,data.Items[0])
        } else {
          done(new Error("Not Found"))
        }
        
       }
    });
};
