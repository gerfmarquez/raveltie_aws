
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
    autoProfiling: false,
    debug: true
  })

}

{
  var PrecheckBreakException = {}
  var SkipMainBreakException = {}
  var timestampOffset = 30 * 1000//30 seconds
  var zones = [
    {'zone':'A','radius':14484,'points':1},
    {'zone':'B','radius':9656,'points':2},
    {'zone':'C','radius':4828,'points':3},
    {'zone':'D','radius':2414,'points':4},
    {'zone':'E','radius':1255,'points':5} 
  ]
  var now = new Date()
  var last24Hours = date.addDays(now,-1)
}

// let sleep =async (ms)=> {
//   return new Promise(resolve => setTimeout(resolve, ms))
// }
exports.handler = async (event)=> {

  // const span = await agent.profile()

  try {

    var imeisMap = new Map()
    await pullRaveltieData(imeisMap)

    await precheckRaveltie(imeisMap,async (mainImei,secondaryImei,secondaryLocation,boundingBox)=> {
      var inside = geolocation.insideBoundingBox(secondaryLocation,boundingBox)
      if(inside) {
        mainImei.overlapping.push({'imei':secondaryImei.imei})
        // console.log("secondaryImei: "+JSON.stringify(secondaryImei))
        return PrecheckBreakException//skip to next secondaryImei, not yet time to do final processing
      }
    })
    
    await processRaveltieData(imeisMap)

  } catch(promisifyError) {
    console.error(promisifyError)
  }
  

  //TODO make sure removing callback still blocks javascript thread
 
  let response = {
    statusCode: 200,
    body: 'Done'
  }
  // await span.stop(()=> {
  //   return response
  // })
  return response
}

let pullRaveltieData =async (imeisMap)=> {
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
  
  await scanning(imeisMap,scan)

  
}
let precheckRaveltie =async (imeisMap,done)=> {
  // console.log("precheckRaveltieData")
  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    if(mainImei.locations.length === 0) return

    // if(mainImei.imei === "9dd419498375d3b8ea10429670e432e80ecfa77697f6ecc943ad66de40425928")return
    // console.log("precheck key: "+JSON.stringify(mainImeiKey))
    // console.log("precheck-mainImei")
    // console.log("precheck value: "+JSON.stringify(mainImei.imei))

    var boundingBox = geolocation.getBoundingBox(mainImei.locations, zones[0].radius)//at least zone A
    try {
      await imeisMap.forEach(async (secondaryImei,secondaryImeiKey)=> {
        if(mainImei.imei === secondaryImei.imei) return
        // if(mainImei.overlapping.includes())
        // console.log("precheck-secondaryImei")
        await secondaryImei.locations.forEach(async (secondaryLocation,index,array)=> {
          // console.log("precheck-secondaryLocation"+JSON.stringify(secondaryLocation))
          // console.log("inside precheck")

          var precheckBreakException = await done(mainImei,secondaryImei,secondaryLocation,boundingBox)
          throw precheckBreakException
          
        }) 

      })
    }catch(precheckBreakException){
      if(precheckBreakException instanceof Error) {
        throw precheckBreakException
      } else {
        // console.log("precheckBreakException")
      }
    }
  })
  // console.log("before precheck done")
}
let processRaveltieData =async (imeisMap)=> {
  // console.log("processRaveltieData")
  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    
    // console.log("process imei: "+mainImei.imei+" overlapping: "+JSON.stringify(mainImei.overlapping))
    // console.log(imeisMap)
    //once all secondary Imeis are processed we can calculate new score for mainImei
    await mainImei.overlapping.forEach(async (overlapping, index, array)=> {

      var overlappingImei = imeisMap.get(overlapping.imei)

      if(typeof overlappingImei == "undefined") return


      var mainTimestamp = 0
      var secondaryTimestamp = 0
      var lastSecondaryIndexUsed = 0
      
      mainImei.locations.sort((a,b)=> {return a.timestamp - b.timestamp})
      await mainImei.locations.forEach(async (mainLocation,index,array)=> {

        mainTimestamp = mainLocation.timestamp
        var matchingSecondaryLocation

        try {
          overlappingImei.locations.sort((a,b)=> {return a.timestamp - b.timestamp})
          await overlappingImei.locations.forEach(async (secondaryLocation, secIndex, secArray)=> {
            if(secIndex <= lastSecondaryIndexUsed) {
                return//@todo efficiency
            }
            // console.log("secondaryLocation index: "+secIndex+" mainLocationIndex: "+index)

            secondaryTimestamp = secondaryLocation.timestamp

            var [matchingSecondaryLocation,lastSecondaryIndexUsed,SkipMainBreakException] = 
              await rewindOrForward(mainTimestamp,secondaryTimestamp,secIndex,secArray)

            if(SkipMainBreakException) throw SkipMainBreakException

            await fuseScore(mainLocation,matchingSecondaryLocation,mainImei,overlappingImei)

          })//end overlapping Imei Locations)

        }catch(skipMainBreakException) {
          if(skipMainBreakException instanceof Error) {
              throw skipMainBreakException
          } else {
              // console.log("skipMainBreakException")
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
      score : mainImei.score
    },
    // UpdateExpression: '',
    ReturnValues:"ALL_OLD"
  }
  
  var data = await promisify(dynamo.putItem.bind(dynamo))(updateScore)

  // console.log(data)
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
    // console.log("deleted location for imei"+JSON.stringify(data2))
  })


  //discard mainImei and update to database but increase secondaryImei score too
  // console.log("imei map keys: "+JSON.stringify(imeisMap))
  // console.log(imeisMap)
  imeisMap.delete(mainImeiKey)

  // console.log("delete imei key: "+JSON.stringify(imeisMap))

  //once finish processing delete overlapping imei's for current zone
  mainImei.overlapping = []
  mainImei.locations = []//free up some memory?

  // console.log(JSON.stringify(mainImei))

}
let rewindOrForward =async (mainTimestamp,secondaryTimestamp,secIndex,secArray)=> {
  // console.log("secondaryTimestamp: "+secondaryTimestamp+" MainTimestamp: "+mainTimestamp)
  //find closest matching timestamp for main and secondary locations
  if(secondaryTimestamp >= mainTimestamp) {
    
    return await rewind(secIndex,secArray,mainTimestamp)

  } else if(secondaryTimestamp <= mainTimestamp) {
    
    return await forward(secIndex,secArray,mainTimestamp)

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
            mainImei.score += zoneValue.points
            overlappingImei.score += zoneValue.points

            // console.log("Score1: "+mainImei.score+"Score2: "+overlappingImei.score)
            // throw SkipMainBreakException

        } else {
            //no points
            throw ZoneBreakException
            //when there aren't any matching big zones,
            //least will there be matching smaller zones
        }
        throw ZoneBreakException
    })//end zones

  }catch(zonesBreakException) {
    if(zonesBreakException instanceof Error) {
        throw zonesBreakException
    } else {
        // console.log("zonesBreakException")
        throw SkipMainBreakException
    }
  }

}

let rewind =async (secIndex,secArray,mainTimestamp)=> {
  // console.log("Greater")
  var rewindIndex = secIndex
  var rewindTimestamp
  do {
    rewindTimestamp = secArray[rewindIndex].timestamp
    if(rewindTimestamp < mainTimestamp) {
      // console.log("Rewind--")
      var forwardIndex = rewindIndex + 1
      var forwardTimestamp = secArray[forwardIndex].timestamp

      //figure which is closer
      var rewindOffset = mainTimestamp - rewindTimestamp
      var forwardOffset = forwardTimestamp - mainTimestamp

      // console.log("forwardOffset: "+forwardOffset+
      //     " rewindOffset: "+rewindOffset+" timestampOffset: "+timestampOffset)

      if(forwardOffset < rewindOffset &&
       forwardOffset < timestampOffset) {
          // console.log("Rewind--Rewind Offset")
          //use forward secondary
          matchingSecondaryLocation = secArray[forwardIndex]
          lastSecondaryIndexUsed = forwardIndex
          return [matchingSecondaryLocation,lastSecondaryIndexUsed,null]
      } else if(rewindOffset < forwardOffset &&
       rewindOffset < timestampOffset) {
          // console.log("Rewind--Forward Offset")
          //use rewind secondary
          matchingSecondaryLocation = secArray[rewindIndex]
          lastSecondaryIndexUsed = rewindIndex
          return [matchingSecondaryLocation,lastSecondaryIndexUsed,null]
      } else {
        return [null,null,SkipMainBreakException]//no close timestamps found
      }
    } else {
      // console.log("Greater-First")
    }
    if(rewindIndex > 0) {
      rewindIndex = rewindIndex - 1
    } else {
      return [null,null,SkipMainBreakException]
    }
  } while (rewindTimestamp > (mainTimestamp - timestampOffset))

}

let forward =async (secIndex,secArray,mainTimestamp)=> {
  // console.log("Lesser")
  var forwardIndex = secIndex
  var forwardTimestamp
  do {
    forwardTimestamp = secArray[forwardIndex].timestamp
    if(forwardTimestamp > mainTimestamp) {
      // console.log("Forward--")
      var rewindIndex = forwardIndex - 1

      // console.log(JSON.stringify(secArray[rewindIndex].timestamp))
      var rewindTimestamp = secArray[rewindIndex].timestamp

      //figure which is closer
      var forwardOffset = forwardTimestamp - mainTimestamp
      var rewindOffset = mainTimestamp - rewindTimestamp

      // console.log("forwardOffset: "+forwardOffset+
      //     " rewindOffset: "+rewindOffset+" timestampOffset: "+timestampOffset)

      if(forwardOffset < rewindOffset &&
       forwardOffset < timestampOffset) {
          // console.log("Forward--Forward Offset")
          //use forward secondary5
          matchingSecondaryLocation = secArray[forwardIndex]
          lastSecondaryIndexUsed = forwardIndex
          return [matchingSecondaryLocation,lastSecondaryIndexUsed,null]
      } else if(rewindOffset < forwardOffset &&
       rewindOffset < timestampOffset) {
        // console.log("Forward--Rewind Offset")
        //use rewind secondary
        matchingSecondaryLocation = secArray[rewindIndex]
        lastSecondaryIndexUsed = rewindIndex
        return [matchingSecondaryLocation,lastSecondaryIndexUsed,null]
      } else {
        return [null,null,SkipMainBreakException]//no close timestamps found
      }
    } else {
        // console.log("Lesser-First")
    }
    
    if(forwardIndex < secArray.length-1) {
      forwardIndex = forwardIndex + 1
    } else {
      throw SkipMainBreakException
    }
  } while (forwardTimestamp < (mainTimestamp + timestampOffset))

}

let scanning =async (imeisMap,scan)=> {
  // console.log("dynamo.scan")
  const data = await promisify(dynamo.scan.bind(dynamo))(scan)
  // console.log("data: "+JSON.stringify(data.Items))
  
  // console.log("last evaluated: "+JSON.stringify(data.LastEvaluatedKey))
  // console.log(JSON.stringify(data))
  var imeisArray = data.Items

  await imeisArray.forEach(async (value, index, array)=> {
    // console.log("imeisArray.forEach")
    var imeiMapItem = null
    if(imeisMap.has(value.imei)) {
        imeiMapItem = imeisMap.get(value.imei)
    } else {
      imeisMap.set(
        value.imei,{'imei':value.imei,'score':0,'locations':[],'overlapping':[]})
      imeiMapItem = imeisMap.get(value.imei)
    }

    if(value.timestamp === 'score') {
      imeiMapItem.score = Number(value.score)
    } else {
      imeiMapItem.locations.push(
          {'lat':Number(value.lat), 'lon':Number(value.lon),
          'accuracy':Number(value.accuracy),
          'timestamp':Number(value.timestamp)})
      // console.log( JSON.stringify(imeiMapItem.locations))
      // console.log(imeisMap)
    }
  })
  if(typeof data.LastEvaluatedKey != "undefined") {
    scan.ExclusiveStartKey = data.LastEvaluatedKey
    await scanning(imeisMap,scan)
    return
  } else {
    return //    await done(null,{})
  }
}
Map.prototype.forEach =async function (done) {
  for (let [key, value] of this) { 
    // console.log(key)
    // console.log("override map.prototype"+JSON.stringify(value))
    await done(value, key)
  }
}
Array.prototype.forEach =async function(done) {
  for (let index = 0; index < this.length; index++) {
    // console.log("override prototype: "+JSON.stringify(this[index]))
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