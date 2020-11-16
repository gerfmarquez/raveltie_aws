/** This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 * Copyright 2020, Gerardo Marquez.
 */

console.log('Loading function');

const doc = require('dynamodb-doc');

const dynamo = new doc.DynamoDB();

exports.handler = (event, context, callback) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });
    console.log(event);

	dynamo.putItem(	
	{
	    'TableName':'raveltie',
	    'Item': {
	        'imei': event.queryStringParameters.imei,
	        // 'timestamp': event.queryStringParameters.timestamp,
	        'timestamp': new Date().getTime().toString(),
	        'lat': event.queryStringParameters.lat,
	        'lon': event.queryStringParameters.lon,
	        'accuracy': event.queryStringParameters.accuracy
	    }
	}, done);

};



