
/** This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 * Copyright 2020, Gerardo Marquez.
 */

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
  var dynamo = new doc.DynamoDB()
  var promisify = require('util').promisify
  var inspect = require('util').inspect
}

{
  var PrecheckBreakException = {}
  var SkipMainBreakException = {}
  var SkipFillingsException = {}
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
}

let sleep =async (ms)=> {
  return new Promise(resolve => setTimeout(resolve, ms)) 
}
exports.handler = async (event)=> {

	console.log("running")

	// await sleep(4 * 1000)


  if(typeof event.period != "undefined")
    period = periods[event.period]
  console.log(period)

  try {
    var imeisMap = new Map()
    await pullRaveltieData(async(data)=> {
      await transformRaveltieData(imeisMap,data)

   })
    // console.log(imeisMap)
    await sortTimestamps(imeisMap)

    await fillTimestamps(imeisMap)

    await detectOverlaps(imeisMap,async (imei,overlap)=> {
        imei.overlapping.push({'imei':overlap.imei})
    })
    
    var imeiFuses = new Map() 

    console.log("initial: "+inspect(imeisMap,{showHidden: false, depth: null, maxArrayLength:6}))

    await trackRaveltie(imeisMap, async (fused,overlappingImei,imei)=> {  
    	
      //@TODO currently this raveltie callback is executed per overlapping location.
      //@TODO should this be executed only after all overlapping callbacks are processed? but per overlapping imei round?
      console.log("imeiA "+inspect(imei,{showHidden: false, depth: null, maxArrayLength: 4}))
      console.log("imeiB"+inspect(overlappingImei,{showHidden: false, depth: null, maxArrayLength: 4})) 


      element2  = await putMapItem(imeiFuses.get(imei), overlappingImei.imei, { 'stamps':fused,'overlapScore':overlappingImei.score}) 
      imeiFuses = await putMapItem(imeiFuses, imei.imei, {'score':imei.score,'overlap': element2})
      
      // console.log("fuse: "+inspect(imeiFuses,{showHidden: false, depth: null, maxArrayLength: 4})) 
      //@TODO reset overlapFuses after loop?
    })
    // console.log("work...")
    console.log("fuses:"+inspect(imeiFuses,{showHidden: false, depth: null, maxArrayLength: 4}))

    await fuseStamp(imeiFuses, async(imei,transformedScore)=> {
      imeisMap.get(imei).newscore += transformedScore
      await updateRaveltieScore(imeisMap, imei)
    })
    // console.log(inspect(imeiFuses,{showHidden: false, depth: null}))
    // console.log(inspect(overlapFuses,{showHidden: false, depth: null}))


  } catch(promisifyError) {
    console.error(promisifyError)
  }
  // await sleep(4 * 1000)
 	


  return{"response":"200"}
}

let pullRaveltieData = async(done)=> {

  var now = new Date()
  var last = date.addHours(now, (period.hours * -1) ) 
  last = date.addMinutes(last, (period.minutes * -1))
  var lastTimestamp = last.getTime().toString()

  // console.log(date.format(now, 'hh:mm'))
  // console.log(date.format(lastHourly, 'hh:mm'))
  // console.log(date.format(lastMinutely, 'hh:mm'))

  // get all locations/scores of all imeis for last 24 hours
  var scan = {
    TableName : tableName,
    Limit : 100,
    FilterExpression: '#ts > :greatherthan',
    ExpressionAttributeValues: {
      ':greatherthan': lastTimestamp
    },
    ExpressionAttributeNames : {'#ts':'timestamp'}
  }
  try {
    await scanning(scan,done, lastTimestamp)  
  }catch(error) {
    if(error instanceof Error && error.code === 'ResourceNotFoundException') {
      console.error("Dynamo DB Resource Empty")
    } else {
      throw error
    }
  }
}
let detectOverlaps =async (imeisMap,done)=> {
  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    if(mainImei.locations.length === 0) return

    // if(mainImei.imei === "9dd419498375d3b8ea10429670e432e80ecfa77697f6ecc943ad66de40425928")return

    var boundingBox = geolocation.getBoundingBox(mainImei.locations, zones[0].radius)//at least zone A

    await imeisMap.forEach(async (secondaryImei,secondaryImeiKey)=> {
      if(mainImei.imei === secondaryImei.imei) {
      	// console.log("skip mainimei: "+mainImei.imei+" : "+secondaryImei.imei)
      	return
      }
      try {	
      	// if(mainImei.overlapping.includes())
        await secondaryImei.locations.forEach(async (secondaryLocation,index,array)=> {

          var inside = geolocation.insideBoundingBox(secondaryLocation,boundingBox)
          if(inside) {
            await done(mainImei,secondaryImei)
            
            throw PrecheckBreakException//skip to next secondaryImei, not yet time to do final processing
          }
        }) 
    	}catch(precheckBreakException){
	      if(precheckBreakException instanceof Error) throw precheckBreakException
  		}
    }) //end of secondary

    if(mainImei.overlapping.length === 0) {

    	await done(mainImei,mainImei)

    	console.log("Adding Pioneer Overlapping Imei By Reference"+mainImei.imei)

    }


  })//end of main

}
let trackRaveltie =async (imeisMap,done)=> {

  var overlappingImeis = []

  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {
    //once all secondary Imeis are processed we can calculate new score for mainImei
    await mainImei.overlapping.forEach(async (overlapping, index, array)=> {

    	if(overlappingImeis.includes(overlapping.imei)) {
    		console.log("one way fuse skipping imei: "+mainImei.imei+" overlapping : "+overlapping.imei)
      	return
    	}

      var overlappingImei = imeisMap.get(overlapping.imei)

      var trackingIndex = 0
      var fused = []

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

                // var {skipMainBreakException, fusedZones} = 
                var {fusedZones} = await fuseZone(location, trackingLocation)

                if(fusedZones.length > 0)
                  fused.push(fusedZones)
                
                // if(skipMainBreakException) throw SkipMainBreakException

                throw SkipMainBreakException

              })//end rewindOrForward

          })//end overlapping Imei Locations)

        }catch(skipMainBreakException) {
          if(skipMainBreakException instanceof Error) {
              throw skipMainBreakException
          } else {
            
          }
        }
            
      })//end main Imei Locations

      await done(fused, overlappingImei, mainImei)

      overlappingImeis.push(mainImei.imei)

    })//end main Imei Overlapping


  })//end imeisMap

}
let fuseZone =async (mainLocation,fusedLocation)=> {
  var ZoneBreakException = {}
  var fusedZones = []
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

          fusedZones.push({'zone':zoneValue,'timestamp':fusedLocation.timestamp})
          
        } else {
          //no points
          throw ZoneBreakException
          //when there aren't any matching big zones,
          //least will there be matching smaller zones
        }
    })//end zones

    return  {fusedZones}

  }catch(zonesBreakException) {
    if(zonesBreakException instanceof Error) {
        throw zonesBreakException
    } else {
        return  {fusedZones}
        
    }
  }
}
//output is score
let fuseStamp =async (fuses, done)=> {

  var collectHalfMinutes = {'A':0,'B':0,'C':0,'D':0,'E':0}

  var zonesA = []
  var zonesB = []
  var zonesC = []
  var zonesD = []
  var zonesE = []
  
  await fuses.forEach(async (fuse, imei) => { 

    await fuse.overlap.forEach(async (overlap, overlapImei) => {
      // console.log(overlap)

      await overlap.stamps.forEach(async (stamp, array, index) => {

        await stamp.forEach(async (zone, array, index) => {
          var z = zone.zone.zone
          switch(z) {
            case 'A':
              zonesA.push(z.timestamp)
              break;
            case 'B':
              zonesB.push(z.timestamp)
              break;
            case 'C':
              zonesC.push(z.timestamp)
              break;
            case 'D':
              zonesD.push(z.timestamp)
              break;
            case 'E':
              zonesE.push(z.timestamp)
              break;
          }
        })// end zones

      })// end stamps

      var minCoverage = (period.coverage * period.min30sec)

      // console.log("zones A Length: "+ zonesA.length)
      // console.log("zones B Length: "+ zonesB.length)
      // console.log("zones C Length: "+ zonesC.length)
      // console.log("zones D Length: "+ zonesD.length)
      // console.log("zones E Length: "+ zonesE.length)


      var letterA = zones.find(letter => letter.zone === 'A')
      var letterB = zones.find(letter => letter.zone === 'B')
      var letterC = zones.find(letter => letter.zone === 'C')
      var letterD = zones.find(letter => letter.zone === 'D')
      var letterE = zones.find(letter => letter.zone === 'E')

      var formulaA = 0
      var formulaB = 0
      var formulaC = 0
      var formulaD = 0
      var formulaE = 0

      if(zonesA.length >= minCoverage)
        formulaA = (overlap.overlapScore ) * ( letterA.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesB.length >= minCoverage)
        formulaB = (overlap.overlapScore ) * ( letterB.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesC.length >= minCoverage)
        formulaC = (overlap.overlapScore ) * ( letterC.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesD.length >= minCoverage)
        formulaD = (overlap.overlapScore ) * ( letterD.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesE.length >= minCoverage)
        formulaE = (overlap.overlapScore ) * ( letterE.multiplier ) * ( period.reward ) * ( period.boost )


      var transformedScore = formulaA + formulaB + formulaC + formulaD + formulaE

      if(transformedScore > 0) {
	      // console.log("formula A: "+ formulaA)
	      // console.log("formula B: "+ formulaB)
	      // console.log("formula C: "+ formulaC)
	      // console.log("formula D: "+ formulaD)
	      // console.log("formula E: "+ formulaE)


	      console.log("fuseA: "+imei+
	      	" effective fuse score: "+overlap.overlapScore+
	      	" transformed score fuse: "+transformedScore)
      } else {
      	// console.log("zero transformed fuse: "+imei)
      }

      await done(imei,transformedScore)


      if(imei === overlapImei) {
      	console.log("Ignoring Same Pioneer Imei Scoring")
      	return
      }

      formulaA = 0
      formulaB = 0
      formulaC = 0
      formulaD = 0
      formulaE = 0

      if(zonesA.length >= minCoverage)
        formulaA = ( fuse.score ) * ( letterA.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesB.length >= minCoverage)
        formulaB = ( fuse.score ) * ( letterB.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesC.length >= minCoverage)
        formulaC = ( fuse.score ) * ( letterC.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesD.length >= minCoverage)
        formulaD = ( fuse.score ) * ( letterD.multiplier ) * ( period.reward ) * ( period.boost )
      if(zonesE.length >= (period.coverage * period.min30sec))
        formulaE = ( fuse.score ) * ( letterE.multiplier ) * ( period.reward ) * ( period.boost )

      transformedScore = formulaA + formulaB + formulaC + formulaD + formulaE

      if(transformedScore > 0) {
	      // console.log("formula A: "+ formulaA)
	      // console.log("formula B: "+ formulaB)
	      // console.log("formula C: "+ formulaC)
	      // console.log("formula D: "+ formulaD)
	      // console.log("formula E: "+ formulaE)

	      console.log("fuseB: "+overlapImei+
	      	" effective overlap score: "+fuse.score+
	      	" transformed score overlap: "+transformedScore)
      } else {
				// console.log("zero transformed overlap: "+overlapImei)
      }

      await done(overlapImei,transformedScore)

    })//end overlaps
    
  })//end fuses

}
let updateRaveltieScore =async (imeisMap,imeiKey)=> {

  var  imei = imeisMap.get(imeiKey)
  //Update score and delete processed locations?
  var updateScore = {
    TableName : tableName,
    Item : {
      imei :imeiKey,
      timestamp : 'score',
      score : (imei.newscore + imei.score)
      // score : mainImei.score
    },
    // UpdateExpression: '',
    ReturnValues:"ALL_OLD"
  }
  
  var data = await promisify(dynamo.putItem.bind(dynamo))(updateScore)

  //maybe make sure that old score being replaced isn't higher

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

let scanning =async (scan,done,last)=> {

  const data = await promisify(dynamo.scan.bind(dynamo))(scan)
  var scanResults = data.Items

  await scanResults.forEach(async (value, index, array)=> {
    // console.log(value)
    await done(value)
  })
  if(typeof data.LastEvaluatedKey != "undefined") {

    // console.log("last evaluated key: "+
    //   inspect(data.LastEvaluatedKey,{showHidden: false, depth: null, maxArrayLength:5}))
    // console.log("scanned count: "+data.ScannedCount+" count: "+data.Count)

    if(Number(data.LastEvaluatedKey.timestamp) <  last) {
        data.LastEvaluatedKey.timestamp =  last
        // console.log(now)
    }

    scan.ExclusiveStartKey = data.LastEvaluatedKey
    await scanning(scan,done, last)
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
    if(mainImei.score === 0) {
      mainImei.score = 100//Default Score
    }
  })
}
let fillTimestamps =async (imeisMap)=> {

  //sort once after extracting 
  await imeisMap.forEach(async (mainImei, mainImeiKey)=> {


    try {//Skip Fillings Exception

      var length = mainImei.locations.length
      var last = mainImei.locations[0]
      await mainImei.locations.forEach(async (location, locIndex, locArray)=> {

        if(locIndex >= length) {
          throw SkipFillingsException
        }

        var delay = location.timestamp - last
        if(delay > 0) {

          var missing = (delay - timestampOffset) / timestampOffset
          if(missing > 1) {
            for(i =1; i < 1+missing; i++ ) {

              var filling = last + (i * timestampOffset)

              var locationCopy = Object.assign({},location)
              locationCopy.timestamp = filling

              locArray.push(locationCopy)
            }

            last = location.timestamp + (missing * timestampOffset)
            return
          }
        }
        
        last = location.timestamp
      })

    } catch(skipFillingsException) {
      if(skipFillingsException instanceof Error) {
          throw skipFillingsException
      } 
    }
  })
  sortTimestamps(imeisMap)
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
let putMapItem =async (map,key,object)=> {
	if(typeof map == "undefined") {
		map = new Map()
	}
  if(map.has(key)) {
  	if(typeof object.overlap == "undefined") {
  		throw new Error("Abort due to new bug on map merging, not supported on top level, only on overlaps")
  	}
  	// console.log("error imei"+key+" inspect: "+inspect(map.get(key).overlap,{showHidden: false, depth: null, maxArrayLength: 4}))
  	// console.log("error2 imei"+key+" inspect: "+inspect(object.overlap,{showHidden: false, depth: null, maxArrayLength: 4}))
		var nested = new Map([...map.get(key).overlap, ...object.overlap])
		map.get(key).overlap = nested
		// console.log("nested imei"+key+" inspect: "+inspect(nested,{showHidden: false, depth: null, maxArrayLength: 4}))
    
  } else {
    map.set(key,object)
  }
  return map// return null for Top Level
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
