
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

{
  var doc = require('dynamodb-doc')
  var date = require('date-and-time')
  var geolocation = require('geolocation-utils')
  var stackimpact = require('stackimpact')
  var dynamo = new doc.DynamoDB()
  var promisify = require('util').promisify
  var agent = stackimpact.start({
    agentKey: "706fa37259ad936a69bb20d85798c52e941cb55b",
    appName: "MyNodejsApp",
    autoProfiling: true,
    debug: true
  })
}

{
  var PrecheckBreakException = {}
  var SkipMainBreakException = {}
  var timestampOffset = 30 * 1000//30 seconds
  var zones = [
    {'zone':'A','radius':14484,'points':.01},/* 1% */
    {'zone':'B','radius':9656,'points':.02},/* 2% */
    {'zone':'C','radius':4828,'points':.03},/* 3% */
    {'zone':'D','radius':2414,'points':.04},/* 4% */
    {'zone':'E','radius':1255,'points':.05} /* 5% */
  ]
  var periods = [
    {'period':.15, 'coverage': .65, 'reward': 1.00},
    {'period':.48, 'coverage': .65, 'reward': 1.00},
    {'period':2.5, 'coverage': .65, 'reward': 1.00},
    {'period':24, 'coverage': .65, 'reward': 1.00},
    {'period':8, 'coverage': .65, 'reward': 1.00},
  ]
  var now = new Date()
  var last24Hours = date.addDays(now,-1)
  
}


exports.handler = async (event)=> {
  
  try {
    var imeisMap = new Map()
    await pullRaveltieData(async (data)=> {
      await transformRaveltieData(imeisMap,data)

    })
    await sortTimestamps(imeisMap)

    await detectOverlaps(imeisMap,async (imei,overlap)=> {
        imei.overlapping.push({'imei':overlap.imei})
    })
    
    await processRaveltieData(imeisMap)

  } catch(promisifyError) {
    console.error(promisifyError)
  }
 
  let response = {
    statusCode: 200,
    body: 'Done'
  }

  return{"response":"200"}
}

let pullRaveltieData =async (done)=> {
  //get all locations/scores of all imeis for last 24 hours
  var scan = {
    TableName : 'raveltie2',
    Limit : 100//,
    // FilterExpression: '#ts > :greatherthan',
    // ExpressionAttributeValues: {
    //   ':greatherthan': last24Hours.getTime().toString()
    // },
    // ExpressionAttributeNames : {'#ts':'timestamp'}
  }
  
  await scanning(scan,done)
}
let detectOverlaps =async (imeisMap,done)=> {
  
  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    if(mainImei.locations.length === 0) return

    // if(mainImei.imei === "9dd419498375d3b8ea10429670e432e80ecfa77697f6ecc943ad66de40425928")return

    var boundingBox = geolocation.getBoundingBox(mainImei.locations, zones[0].radius)//at least zone A
    try {
      await imeisMap.forEach(async (secondaryImei,secondaryImeiKey)=> {
        if(mainImei.imei === secondaryImei.imei) return
        // if(mainImei.overlapping.includes())

        await secondaryImei.locations.forEach(async (secondaryLocation,index,array)=> {

          var inside = geolocation.insideBoundingBox(secondaryLocation,boundingBox)
          if(inside) {
            await done(mainImei,secondaryImei)
            
            throw PrecheckBreakException//skip to next secondaryImei, not yet time to do final processing
          }
        }) 

      })
    }catch(precheckBreakException){
      if(precheckBreakException instanceof Error) {
        throw precheckBreakException
      } else {

      }
    }
  })

}
let processRaveltieData =async (imeisMap)=> {

  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    
    //once all secondary Imeis are processed we can calculate new score for mainImei
    await mainImei.overlapping.forEach(async (overlapping, index, array)=> {

      var overlappingImei = imeisMap.get(overlapping.imei)

      if(typeof overlappingImei == "undefined") return


      var trackingIndex = 0

      await mainImei.locations.forEach(async (mainLocation,index,array)=> {

        var matchingSecondaryLocation

        try {
          await overlappingImei.locations.forEach(async (secondaryLocation, secIndex, secArray)=> {
            if(secIndex <= trackingIndex) {
                return//@todo efficiency
            }

            var [index, skipMainBreakException] = 
              await rewindOrForward(
                mainLocation.timestamp, secondaryLocation.timestamp,  secIndex, secArray)

            if(skipMainBreakException) throw SkipMainBreakException

            trackingIndex = index
            var trackingLocation = secArray[trackingIndex]

            await fuseScore(mainLocation,trackingLocation,mainImei,overlappingImei)

          })//end overlapping Imei Locations)

        }catch(skipMainBreakException) {
          if(skipMainBreakException instanceof Error) {
              throw skipMainBreakException
          } else {

          }
        }
            
      })//end main Imei Locations

    })//end main Imei Overlapping

    await updateRaveltieScore(imeisMap,mainImei,mainImeiKey)

  })//end imeisMap

}
let updateRaveltieScore =async (imeisMap,mainImei,mainImeiKey)=> {
  //Update score and delete processed locations?
  var updateScore = {
    TableName : 'raveltie2',
    Item : {
      imei : mainImei.imei,
      timestamp : 'score',
      // score : (mainImei.score + mainImei.newscore)
      score : mainImei.score
    },
    // UpdateExpression: '',
    ReturnValues:"ALL_OLD"
  }
  
  var data = await promisify(dynamo.putItem.bind(dynamo))(updateScore)


  //maybe make sure that old score being replaced isn't higher


  await mainImei.locations.forEach(async (location,index,array)=> {
    var deleteRequest =  
    {
      TableName : 'raveltie2',
      Key : {
        imei : mainImei.imei,
        timestamp : location.timestamp.toString()
      }
    }
    // var data2 = await promisify(dynamo.deleteItem.bind(dynamo))(deleteRequest)

  })


  //discard mainImei and update to database but increase secondaryImei score too

  imeisMap.delete(mainImeiKey)



  //once finish processing delete overlapping imei's for current zone
  mainImei.overlapping = []
  mainImei.locations = []//free up some memory?
}
let rewindOrForward =async (timestamp,secondaryTimestamp,secIndex,secArray)=> {

  //find closest matching timestamp for main and secondary locations
  if(secondaryTimestamp >= timestamp) {
    
    return await rewind(secIndex,secArray,timestamp)

  } else if(secondaryTimestamp <= timestamp) {
    
    return await forward(secIndex,secArray,timestamp)

  } else {
    throw new Error("Serious Error")
  }

}
let fuseScore =async (mainLocation,matchingSecondaryLocation,mainImei,overlappingImei)=> {
  var ZoneBreakException = {}
  try {
    // for now use zones but it's very expensive, so do a pre-Zone check
    await zones.forEach(async (zoneValue,zI,zA)=> {
        //add attribute of location accuracy and sutract it from distance calculation
        //geolocation
        var distanceTo = geolocation
            .headingDistanceTo(mainLocation, matchingSecondaryLocation)
            .distance

        // console.log("distance: "+distanceTo+
        //     " accuracy1: "+mainLocation.accuracy+
        //     " accuracy2: "+matchingSecondaryLocation.accuracy+
        //     " radius: "+zoneValue.radius)

        if((distanceTo - 
            mainLocation.accuracy - 
                matchingSecondaryLocation.accuracy) < zoneValue.radius) {
            //score points
            mainImei.score +=  zoneValue.points
            overlappingImei.score +=  zoneValue.points

        } else {
            //no points
            throw ZoneBreakException
            //when there aren't any matching big zones,
            //least will there be matching smaller zones
        }
    })//end zones
    throw ZoneBreakException
  }catch(zonesBreakException) {
    if(zonesBreakException instanceof Error) {
        throw zonesBreakException
    } else {
        throw SkipMainBreakException
    }
  }
}

let rewind =async (secIndex,secArray,timestamp)=> {
  // console.log("Greater")
  var rewindIndex = secIndex
  var rewindTimestamp
  do {
    rewindTimestamp = secArray[rewindIndex].timestamp
    if(rewindTimestamp < timestamp) {
      // console.log("Rewind--")
      var forwardIndex = rewindIndex + 1
      var forwardTimestamp = secArray[forwardIndex].timestamp

      //figure which is closer
      var rewindOffset = timestamp - rewindTimestamp
      var forwardOffset = forwardTimestamp - timestamp

      // console.log("forwardOffset: "+forwardOffset+
      //     " rewindOffset: "+rewindOffset+" timestampOffset: "+timestampOffset)

      if(forwardOffset < rewindOffset &&
       forwardOffset < timestampOffset) {
          // console.log("Rewind--Rewind Offset")
          //use forward secondary
          return [forwardIndex,null]
      } else if(rewindOffset < forwardOffset &&
       rewindOffset < timestampOffset) {
          // console.log("Rewind--Forward Offset")
          //use rewind secondary
          return [rewindIndex,null]
      } else {
        return [null,SkipMainBreakException]//no close timestamps found
      }
    } else {
      // console.log("Greater-First")
    }
    if(rewindIndex > 0) {
      rewindIndex = rewindIndex - 1
    } else {
      return [null,SkipMainBreakException]
    }
  } while (rewindTimestamp > (timestamp - timestampOffset))
}

let forward =async (secIndex,secArray,timestamp)=> {
  // console.log("Lesser")
  var forwardIndex = secIndex
  var forwardTimestamp
  do {
    forwardTimestamp = secArray[forwardIndex].timestamp
    if(forwardTimestamp > timestamp) {
      // console.log("Forward--")
      var rewindIndex = forwardIndex - 1

      // console.log(JSON.stringify(secArray[rewindIndex].timestamp))
      var rewindTimestamp = secArray[rewindIndex].timestamp

      //figure which is closer
      var forwardOffset = forwardTimestamp - timestamp
      var rewindOffset = timestamp - rewindTimestamp

      // console.log("forwardOffset: "+forwardOffset+
      //     " rewindOffset: "+rewindOffset+" timestampOffset: "+timestampOffset)

      if(forwardOffset < rewindOffset &&
       forwardOffset < timestampOffset) {
          // console.log("Forward--Forward Offset")
          //use forward secondary5
          return [forwardIndex,null]
      } else if(rewindOffset < forwardOffset &&
       rewindOffset < timestampOffset) {
        // console.log("Forward--Rewind Offset")
        //use rewind secondary
        return [rewindIndex,null]
      } else {
        return [null,SkipMainBreakException]//no close timestamps found
      }
    } else {
        // console.log("Lesser-First")
    }
    
    if(forwardIndex < secArray.length-1) {
      forwardIndex = forwardIndex + 1
    } else {
      return [null,SkipMainBreakException]
    }
  } while (forwardTimestamp < (timestamp + timestampOffset))
}

let scanning =async (scan,done)=> {

  const data = await promisify(dynamo.scan.bind(dynamo))(scan)
  var scanResults = data.Items

  await scanResults.forEach(async (value, index, array)=> {
    done(value)
  })
  if(typeof data.LastEvaluatedKey != "undefined") {
    scan.ExclusiveStartKey = data.LastEvaluatedKey
    await scanning(scan,done)
    return
  } else {
    return 
  }
}
let sortTimestamps =async (imeisMap)=> {
  //sort once after extracting 
  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    if(typeof mainImei.locations != "undefined") {
      mainImei.locations.sort((a,b)=> {return a.timestamp - b.timestamp})
    }
  })
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