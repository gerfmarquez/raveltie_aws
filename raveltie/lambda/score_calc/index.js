
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
    autoProfiling: true,
    debug: true
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
    'min1':{'coverage': 1.0, 'reward': 0.0025, 'boost': 0.5},// 0.5 equivalent 20 minutes
    'min7':{'coverage': 0.95, 'reward': 0.005, 'boost': 0.75},// 0.14 equivalent 20 minutes
    'min20':{'coverage': 0.90, 'reward': 0.01, 'boost': 1},// 1 equivalent 20 minutes
    'hr2':{'coverage': 0.85, 'reward': 0.06, 'boost': 1.25},// 6 equivalent 20 minutes
    'hr8':{'coverage': 0.80, 'reward': 0.24, 'boost': 1.5},// 24 equivalent 20 minutes
    'hr16':{'coverage': 0.75, 'reward': 0.48, 'boost': 1.75},//48 equivalent 20 minutes
    'hr24':{'coverage': 0.70, 'reward': 0.72, 'boost': 2}//72 equivalent 20 minutes
  }
  var period = periods.hr24

  var now = new Date()
  var last24Hours = date.addHours(now,-24)
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
    
    var imeiFuses = new Map()
    var overlapFuses = new Map()
    await trackRaveltie(imeisMap, async (raveltie)=> {
      //@TODO currently this raveltie callback is executed per overlapping location.
      //@TODO should this be executed only after all overlapping callbacks are processed? but per overlapping imei round?
      var [zoneValue, imei, overlapImei, fusedStamp, distanceTo, overlapScore] = raveltie
      
      var element = await putMapItem(imeiFuses, imei, 
      {'overlapScore': overlapScore, 'overlap': 
      await putMapItem(overlapFuses, overlapImei, { 'stamp':fusedStamp, 'zone': zoneValue })
      })
      
      await fuseStamp(imeiFuses, overlapFuses, async()=> {

      })
      //@TODO reset overlapFuses after loop?
    })
    // console.log(inspect(imeiFuses,{showHidden: false, depth: null}))
    // console.log(inspect(overlapFuses,{showHidden: false, depth: null}))
    



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
let trackRaveltie =async (imeisMap,done)=> {

  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    
    //once all secondary Imeis are processed we can calculate new score for mainImei
    await mainImei.overlapping.forEach(async (overlapping, index, array)=> {

      var overlappingImei = imeisMap.get(overlapping.imei)

      if(typeof overlappingImei == "undefined") return


      var trackingIndex = 0

      await mainImei.locations.forEach(async (location,index,array)=> {

        try {
          await overlappingImei.locations.forEach(async (overlap, secIndex, secArray)=> {
            if(secIndex <= trackingIndex) {
                return//@todo efficiency
            }

            // var [index, skipMainBreakException] = 
            await rewindOrForward(location.timestamp, overlap.timestamp, secArray, secIndex, 
              async (track)=> { 
              
                var [index,skip] = track

                if(skip) 
                  throw SkipMainBreakException

                trackingIndex = index
                var trackingLocation = secArray[trackingIndex]

                var skipMainBreakException = 
                await fuseZone(mainImei.imei, overlappingImei.imei, overlappingImei.score, 
                location, trackingLocation, done)
                
                if(skipMainBreakException) throw SkipMainBreakException

              })//end rewindOrForward

          })//end overlapping Imei Locations)

        }catch(skipMainBreakException) {
          if(skipMainBreakException instanceof Error) {
              throw skipMainBreakException
          } else {
            
          }
        }
            
      })//end main Imei Locations

    })//end main Imei Overlapping


  })//end imeisMap

}
let fuseStamp =async (imeiFuses, overlapFuses, done)=> {

  var collectHalfMinutes = {'A':0,'B':0,'C':0,'D':0,'E':0}
  
  await imeiFuses.forEach(async (fuses, imei) => {

    // switch(fuses.)

    // console.log(inspect(fuses,{showHidden: false, depth: null}))

    // console.log(imei)
    // console.log(fuses)

    // await updateRaveltieScore(imeisMap,mainImei,mainImeiKey)
  })
  // var formula = (overlapScore ) * ( % Zone Multiplier ) * ( % Period Reward ) * ( % Period Boost )

}
let fuseZone =async (imei, overlapImei, overlapScore, mainLocation,fusedLocation,done)=> {
  var ZoneBreakException = {}
  try {
    // for now use zones but it's very expensive, so do a pre-Zone check
    await zones.forEach(async (zoneValue,zI,zA)=> {
        //add attribute of location accuracy and sutract it from distance calculation
        //geolocation
        var distanceTo = geolocation
            .headingDistanceTo(mainLocation, fusedLocation)
            .distance

        // console.log("distance: "+distanceTo+
        //     " accuracy1: "+mainLocation.accuracy+
        //     " accuracy2: "+fusedLocation.accuracy+
        //     " radius: "+zoneValue.radius)

        if((distanceTo - 
            mainLocation.accuracy - 
                fusedLocation.accuracy) < zoneValue.radius) {

          var synthesis = [zoneValue, imei, overlapImei, fusedLocation.timestamp, distanceTo,overlapScore]
          await done(synthesis)

        } else {
          //no points
          
          throw ZoneBreakException
          
          //when there aren't any matching big zones,
          //least will there be matching smaller zones
        }
    })//end zones
    // console.log("ZoneBreakException")
    throw ZoneBreakException
  }catch(zonesBreakException) {
    if(zonesBreakException instanceof Error) {
        throw zonesBreakException
    } else {
        return SkipMainBreakException
        
    }
  }
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
let rewindOrForward =async (timestamp, secTimestamp,secArray,secIndex, done)=> {

  //find closest matching timestamp for main and secondary locations
  if(secTimestamp >= timestamp) {
    
    var track = await rewind(secArray, secIndex, timestamp)
    await done(track)

  } else if(secTimestamp <= timestamp) {
    
    var track = await forward(secArray, secIndex, timestamp)
    await done(track)

  } else {
    throw new Error("Serious Error")
  }
}

let rewind =async (secArray, secIndex, timestamp)=> {
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
// Throws SkipMainBreakException
let forward =async (secArray,secIndex,timestamp)=> {
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
let putMapItem =async (map,key,object)=> {
  var element 
  if(map.has(key)) {
    element = map.get(key)
    element.push(object)
  } else {
    map.set(key,[object])
    element = map.get(key)
  }
  return element
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