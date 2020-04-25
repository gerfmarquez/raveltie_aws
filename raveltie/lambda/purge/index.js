
/**
* Zone A 9 miles
* Zone B 6 miles
* Zone C 3  miles
* Zone D 1.5 miles (2,410  m)
* Zone E 0.78 miles (1,260 m) 

* Time Period that users stuck along for 24 hours in zone A,B,C,D,E
* Time Period that users stuck along for 8 hours in zone A,B,C,D,E
* Time Period that users stuck along for 2.5 hours in zone A,B,C,D,E
* Time Period that users stuck along for (48 min)0.8 hours in zone A,B,C,D,E
* Time Period that users stuck along for (15 min)0.26 hours in zone A,B,C,D,E
* Time Period that users stuck along for (5 min) in zone A,B,C,D,E
* Time Period that users stuck along for (1 min) in zone A,B,C,D,E
* Time Period that users stuck along for (20 sec) in zone A,B,C,D,E
**/

 /*
  * Most Value to Least Value
  * Formula :  { % Overlapping Points } * { % Zone Multiplier } * { % Period Reward } * { % Period Boost }
  * 
  * Example E24hr: { % 100 (overlap points)} * { % 5 (zone E )} * { % 72 (24hr prd reward)} * { % 2 boost (24 hr) } == 7.2
  * Example E1min: { % 100 (overlap points)} * { % 5 (zone E )} * { % 0.25 (1 min prd reward)} * { % 0.5 boost (1 min) } == 0.00625
  * Example C24hr: { % 100 (overlap points)} * { % 3 (zone C )} * { % 72 (24hr prd reward)} * { % 2 boost (24 hr) } == 4.32
  * Example C1min: { % 100 (overlap points)} * { % 3 (zone C )} * { % 0.25 (1 min prd reward)} * { % 0.5 boost (1 min) } == 0.00375
  * Example A24hr: { % 100 (overlap points)} * { % 1 (zone A )} * { % 72 (24hr prd reward)} * { % 2 boost (24 hr) } == 1.44
  * Example A1min: { % 100 (overlap points)} * { % 1 (zone A )} * { % 0.25 (1 min prd reward)} * { % 0.5 boost (1 min) } == 0.00125
  * 
  */

{
  var doc = require('dynamodb-doc')
  var date = require('date-and-time')
  var geolocation = require('geolocation-utils')
  var stackimpact = require('stackimpact')
  var dynamo = new doc.DynamoDB()
  var promisify = require('util').promisify
  var inspect = require('util').inspect
  var agent = stackimpact.start({
    agentKey: "706fa37259ad936a69bb20d85798c52e941cb55b",
    appName: "MyNodejsApp",
    autoProfiling: false,
    debug: false
  })
}

{
  var PrecheckBreakException = {}
  var SkipMainBreakException = {}
  var timestampOffset = 30 * 1000//30 seconds
  var zones = [
    {'zone':'A','radius':12400,'multiplier':0.01},/* 1% */
    {'zone':'B','radius':8280,'multiplier':0.02},/* 2% */
    {'zone':'C','radius':4140,'multiplier':0.03},/* 3% */
    {'zone':'D','radius':1030,'multiplier':0.04},/* 4% */
    {'zone':'E','radius':130,'multiplier':0.05}/* 5% */
  ]
  //no point in having periods unless there are exponential score point rewards
  var periods = {
    'min1':{'coverage': 1.0, 'reward': 0.0025, 'boost': 0.5, 'min30sec':2, 'minutes':1, 'hours':0},// 0.5 equivalent 20 minutes
    'min7':{'coverage': 0.95, 'reward': 0.005, 'boost': 0.75, 'min30sec':14, 'minutes':7, 'hours':0},// 0.14 equivalent 20 minutes
    'min20':{'coverage': 0.90, 'reward': 0.01, 'boost': 1, 'min30sec':40, 'minutes':20, 'hours':0},// 1 equivalent 20 minutes
    'hr2':{'coverage': 0.85, 'reward': 0.06, 'boost': 1.25, 'min30sec':240, 'minutes':0, 'hours':2},// 6 equivalent 20 minutes
    'hr8':{'coverage': 0.80, 'reward': 0.24, 'boost': 1.5, 'min30sec':960, 'minutes':0, 'hours':8},// 24 equivalent 20 minutes
    'hr16':{'coverage': 0.75, 'reward': 0.48, 'boost': 1.75, 'min30sec':1920, 'minutes':0, 'hours':16},//48 equivalent 20 minutes
    'hr24':{'coverage': 0.70, 'reward': 0.72, 'boost': 2, 'min30sec':2880, 'minutes':0, 'hours':24}//72 equivalent 20 minutes
  }
  var period = periods.hr2
  var tableName = 'raveltie'
  var purgeCount = 0
}

let sleep =async (ms)=> {
  return new Promise(resolve => setTimeout(resolve, ms)) 
}
exports.handler = async (event)=> {

	console.log("purge")

	// await sleep(4 * 1000)


  if(typeof event.period != "undefined")
    period = periods[event.period]
  console.log(period)

  try {
    var imeisMap = new Map()
    await pullRaveltieData(async(data)=> {
      await transformRaveltieData(imeisMap,data)

   })

    console.log("purged count: "+purgeCount)

    console.log("cleanup")
    
    await cleanup(imeisMap)

    console.log("cleanup end")


  } catch(promisifyError) {
    console.error(promisifyError)
  }

  return{"response":"200"}
}

let pullRaveltieData = async(done)=> {

  var now = new Date()

  var nowTimestamp = now.getTime().toString()
  var last = date.addHours(now, (period.hours * -1) ) 
  last = date.addMinutes(last, (period.minutes * -1))
  var lastTimestamp = last.getTime().toString()

  console.log("now : "+nowTimestamp)

  // get all locations/scores of all imeis for last 24 hours
  var scan = {
    TableName : tableName,
    Limit : 100,
    FilterExpression: '#ts < :greatherthan',
    ExpressionAttributeValues: {
      ':greatherthan':lastTimestamp
    },
    ExpressionAttributeNames : {'#ts':'timestamp'}
  }
  try {
    await scanning(scan,done,lastTimestamp,nowTimestamp)  
  }catch(error) {
    if(error instanceof Error && error.code === 'ResourceNotFoundException') {
      console.error("Dynamo DB Resource Empty")
    } else {
      throw error
    }
  }
}

let scanning =async (scan,done,last,now)=> {

  const data = await promisify(dynamo.scan.bind(dynamo))(scan)
  var scanResults = data.Items

  await scanResults.forEach(async (value, index, array)=> {
    // console.log(value)
    await done(value)
  })
  purgeCount += data.Count
  if(typeof data.LastEvaluatedKey != "undefined") {

    // console.log("last evaluated key: "+
      // inspect(data.LastEvaluatedKey,{showHidden: false, depth: null, maxArrayLength:5}))
    // console.log("scanned count: "+data.ScannedCount+" count: "+data.Count)

    // console.log("comparison: "+Number(data.LastEvaluatedKey.timestamp) +"  > "  +last)

    if(Number(data.LastEvaluatedKey.timestamp) >  last) {
        data.LastEvaluatedKey.timestamp =  now
        // console.log(now)
    }

    scan.ExclusiveStartKey = data.LastEvaluatedKey
    // scan.ExclusiveStartKey = {
    //   imei: '06290ff04ac52fdcdaaaffce4e75f11a7816f2b0f0df62aac09a0c2f0acb86f4',
    //   timestamp: '1587683519275'
    // }
    await scanning(scan,done,last,now)
    return
  } else {
    return 
  }
}
let transformRaveltieData =async (imeisMap,data)=> {
  var imeiMapItem = null
  if(imeisMap.has(data.imei)) {
      imeiMapItem = imeisMap.get(data.imei)
  } else {
    imeisMap.set(
      data.imei,{'imei':data.imei,'score':0,'newscore':0,'locations':[],'overlapping':[]})
    imeiMapItem = imeisMap.get(data.imei)
  }

  if(data.timestamp === 'score') {
    imeiMapItem.score = Number(data.score)
  } else {
    imeiMapItem.locations.push(
        {'lat':Number(data.lat), 'lon':Number(data.lon),
        'accuracy':Number(data.accuracy),
        'timestamp':Number(data.timestamp)})
  }
}
let cleanup =async (imeisMap)=> {
	var elapsed = new Date().getTime()

  var imeiPromiseMap = Array.from(imeisMap).map(([imeiKey,imei])=> {
		console.log("no cheating start")
	  var promiseMap = imei.locations.map(location=> {
	    var deleteRequest =  
	    {
	      TableName : tableName,
	      Key : {
	        imei : imeiKey,
	        timestamp : location.timestamp.toString()
	      }//,
	      // ReturnValues:"ALL_OLD"
	    }
      var promise = 
      	promisify(dynamo.deleteItem.bind(dynamo))(deleteRequest)
	  	return promise
	  })
	  return promiseMap
  })//end imeisMap


  await imeiPromiseMap.forEach(async(promises)=> {
  	await Promise.all(promises)
  	console.log("no cheating end")
  })
  
	
  elapsed = (new Date().getTime()) - elapsed
  console.log("elapsed delete: "+ elapsed )
}

Map.prototype.forEachAsync =async function (done) {
  for (let [key, value] of this) { 
    done(value, key)
  }
}
Array.prototype.forEachAsync =async function(done) {
  for (let index = 0; index < this.length; index++) {
    done(this[index], index, this)
  }
}

Map.prototype.forEach =async function (done) {
  for (let [key, value] of this) { 
    await done(value, key)
  }
}
Array.prototype.forEach =async function(done) {
  for (let index = 0; index < this.length; index++) {
    await done(this[index], index, this)
  }
}
/*
  calculation equals to distance from user A time x to user B time x minus GPS accuracy minus zone
  result less than 0 is inside zone, greater than 0 is outside zone.
  scan all locations by imei, create a inside bounds, and check distance to 
  locations and match their IMEI, stop iterating that IMEI and keep it for later processing
  check distance for all zones so total period score can be calculated?
  the bounding box matches another IMEI's location, plus margin of zone A.
  keep IMEI's
  do that for every bounding box of every IMEI
  then cross reference every locations on both IMEI's collected.
  check time spent inside the zone or average it?
  take into account chronological crosses of the fence times?
  average boost original iterating IMEI with score from second IMEI if bigger?
  result score for IMEI
  possible algorithm glitch is starting score of IMEI's and ending score
  which one is used? for calculating? previous period? 24 hours?
*/