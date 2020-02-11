exports.handler = (event, context, callback) => {
    // TODO implement
    var config = {"killswitch":false,"minimumRequiredVersion":1};

    callback(null,config);
};