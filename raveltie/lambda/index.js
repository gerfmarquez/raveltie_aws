
/** This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 * Copyright 2020, Gerardo Marquez.
 */

console.log('Loading function');

const doc = require('dynamodb-doc');

const dynamo = new doc.DynamoDB();

exports.handler = (event, context, callback) => {
    console.log('Received event:', JSON.stringify(event, null, 2));

    const done = (err, res) => callback(null, {
        statusCode: err ? '400' : '200',
        body: err ? err.message : JSON.stringify(res),
        headers: {
            'Content-Type': 'application/json',
        },
    });
    
    dynamo.putItem(event, done);

    // switch (event.httpMethod) {
    //     case 'DELETE':
    //         dynamo.deleteItem(JSON.parse(event.body), done);
    //         break;
    //     case 'GET':
    //         dynamo.scan({ TableName: event.queryStringParameters.TableName }, done);
    //         break;
    //     case 'POST':
    //         dynamo.putItem(JSON.parse(event.body), done);
    //         break;
    //     case 'PUT':
    //         dynamo.updateItem(JSON.parse(event.body), done);
    //         break;
    //     default:
    //         done(new Error(`Unsupported method "${event.httpMethod}"`));
    // }
};
