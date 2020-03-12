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

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });


    pullRaveltieScore(done);

};

function pullRaveltieScore(done) {

    //get all locations/scores of all imeis for last 24 hours
    var query = {
      TableName : 'raveltie',
      KeyConditionExpression: '#imei = :imei and #ts = :score',
      ExpressionAttributeValues: {
        ':score': 'score',
        ':imei': '9198473441482201'
      },
      ExpressionAttributeNames : {
      	'#ts':'timestamp',
      	'#imei':'imei'}
    };

    dynamo.query(query, function(err, data) {
       if (err) {
       	done(new Error(`Generic Error`))
        console.log(err);
       } else {
        //console.log(data);
        var imeisArray = data.Items;

        if(imeisArray.length < 1) {
        	done(new Error(`Generic Error`))
        	return;
        }
        imeisArray.forEach(
            function(value, index, array) {
            	console.log(value);
            	console.log(index);
            	console.log(array);
            	done(null,value)
            });
        
       }
    });
};
