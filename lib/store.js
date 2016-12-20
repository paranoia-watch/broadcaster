/*
 * @package broadcaster
 * @copyright Copyright(c) 2016 Paranoia Watch
 * @author Wouter Vroege <wouter AT woutervroege DOT nl>
 */

var mongoose = require('mongoose');
var events = require('events');
var async = require('async');
//var moment = require('moment');
var moment = require('moment-timezone');
var Store = new events.EventEmitter();
var isConnectedBefore = false;

var Publication = mongoose.model('Publication', {
  medium: String,
  mediumPublicationId: Number,
  publisherLocation: String,
  date: Date,
  weight: Number,
  collectionAverageAfterInsert: Number,
  locationAverageAfterInsert: Number
});

var Peilingwijzer = mongoose.model('Peilingwijzer', {
  date: Date,
  seats: Object
});

function main (databaseUri) {
  Store.databaseUri = databaseUri;
  Store.connect = getDBConnection;
  Store.getGrowthSinceLastHour = getGrowthSinceLastHour;
  Store.getDeviationOfLastHour = getDeviationOfLastHour;
  Store.getAveragesPerDay = getAveragesPerDay;
  Store.getPeilingwijzerData = getPeilingwijzerData;
  Store.getPeilingwijzerGrowth = getPeilingwijzerGrowth;
  Store.getAllTopInfluencers = getAllTopInfluencers;
  Store.getTopPoliticalInfluencers = getTopPoliticalInfluencers;
  return Store;
}

function getDBConnection () {
  mongoose.connect(Store.databaseUri, {server: {
    connectTimeoutMS: 3600000,
    poolSize: 10,
    reconnectTries: -1,
    reconnectInterval: 5000
  }})
  return mongoose.connection;
}

/*
Get Peilingwijzer Growth
*/
function getPeilingwijzerGrowth(callback) {

  var self = this;

  var q = Peilingwijzer.find().sort({date: -1}).limit(1);
  q.exec(function(error, result) {
    if(error) {
      Store.emit('peilingwijzer-growth-error', error);
      return callback && callback(error);
    } else {
      var endDateStart = moment(result[0].date).tz("Europe/Amsterdam").format('YYYY-MM-DDT00:00:00.000Z');
      var endDateEnd = moment(endDateStart).tz("Europe/Amsterdam").format('YYYY-MM-DDT23:59:59.000Z');

      var startDateStart = moment(result[0].date).tz("Europe/Amsterdam").subtract(14, 'days').format('YYYY-MM-DDT00:00:00.000Z');
      var startDateEnd = moment(startDateStart).tz("Europe/Amsterdam").format('YYYY-MM-DDT23:59:59.000Z');

      var query = {
        "$or": [
            {
           "date": {
              "$gte": new Date(startDateStart),
              "$lt": new Date(startDateEnd),
           }
            },
            {
           "date": {
              "$gte": new Date(endDateStart),
              "$lt": new Date(endDateEnd),
           }
            }
        ]
      }

      Peilingwijzer.find(query, function(error, results) {
        if(error) {

          Store.emit('peilingwijzer-growth-error', error);
          return callback && callback(error);
        } else {

          var results = JSON.parse(JSON.stringify(results));
          var items = {
            startDate: moment(startDateStart).tz("Europe/Amsterdam").format('D MMM'),
            endDate: moment(endDateStart).tz("Europe/Amsterdam").format('D MMM'),
            items: []
          };

          for(var i in results[0]) {
            if(! (i == "_id" || i == "date" || i == "__v")) {
              if(results[0][i] && results[1][i]) {
                items.items.push({
                  party: i,
                  startWeight: results[0][i],
                  endWeight: results[1][i],
                  growth: (results[1][i] - results[0][i]).toFixed(1)
                })
              }
            }
          }

          items.items.sort(function(a, b) {
            return b.growth - a.growth;
          })

          Store.emit('peilingwijzer-growth', items);
          callback && callback(null, items);

        }

      })

    }

  })

  
}

/*
Peilingwijzer Data
Currently limited to 365 days
*/

function getPeilingwijzerData(callback) {

  var query = [
    {
      "$match": {
        "date": {
          "$gte": new Date("2016-01-01")
        }
      }
    },
    {
      "$group": {
        _id: {
          "month": {
            "$month": "$date"
          },
          "year": {
            "$year": "$date"
          },
          "week": {
            "$week": "$date"
          },
        }
      }
    },
    {
      "$project": {
        "seats": "$seats",
        "date": "$date"
      }
    },
    
  ]

  Peilingwijzer.aggregate(query, function(error, data) {
    if(error) {
      Store.emit('peilingwijzer-data-error', error);
    } else {
      Store.emit('peilingwijzer-data', data);
    }

    callback && callback(error, data);

  });

}

/*
Historical Deviation Percentage
Exposes the change between the publication average of today between now and last hour, versus all the publications published within that same timeframe older than last hour divided by the amount of days in the timeframe. E.g:
The average publication weight between now and one hour ago = 5
The average publication weight between between first publication and one hour ago = 4
The deviation percentage = (5 / 4 * 100) = 125%
*/

function getDeviationOfLastHour (callback) {

  var now = moment().tz("Europe/Amsterdam").subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss'); //change 'hours' to 'days' when using dev database

  var oneHourAgo = moment(now).tz("Europe/Amsterdam").subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss');
  var startOfTime = moment('2016-01-01').tz("Europe/Amsterdam").format('YYYY-MM-DDTHH:mm:ss');

  async.series({
    lastHour: function(itemCallback) {
      getAveragePublicationWeightWithinTimeframe(oneHourAgo, now, itemCallback);      
    },
    allTime: function(itemCallback) {
      getAveragePublicationWeightWithinTimeframe(startOfTime, oneHourAgo, itemCallback);      
    }
  }, function(error, results) {

    if(error) {
      Store.emit('last-hour-deviation-error', error);
    } else {
      Store.emit('last-hour-deviation', results);
    }

    callback && callback(error, results);

  })

} 

function getAveragePublicationWeightWithinTimeframe(startTime, endTime, callback) {

  var start = new Date(startTime);
  var end = new Date(endTime);

  var endHHmm = moment().tz("Europe/Amsterdam").subtract(1, 'hours').format('HH') + moment().tz("Europe/Amsterdam").format('mm');
  var startHHmm = moment().tz("Europe/Amsterdam").subtract(2, 'hours').format('HH') + moment().tz("Europe/Amsterdam").format('mm');

  var query = [
  {
    "$match": {
      "date": {
        "$gte": start,
        "$lte": end
      }
    }
  },
  {
    "$project": {
      "weight": "$weight",
        "publisherLocation": "$publisherLocation",
      "time": {
        "$concat": [
          {
            "$cond": [
              {
                "$gt": [
                  {
                    "$hour": "$date"
                  },
                  9
                ]
              },
              {
                "$substr": [
                  {
                    "$hour": "$date"
                  },
                  0,
                  2
                ]
              },
              {
                "$concat": [
                  "0",
                  {
                    "$substr": [
                      {
                        "$hour": "$date"
                      },
                      0,
                      1
                    ]
                  }
                ]
              }
            ]
          },
          {
            "$cond": [
              {
                "$gt": [
                  {
                    "$minute": "$date"
                  },
                  9
                ]
              },
              {
                "$substr": [
                  {
                    "$minute": "$date"
                  },
                  0,
                  2
                ]
              },
              {
                "$concat": [
                  "0",
                  {
                    "$substr": [
                      {
                        "$minute": "$date"
                      },
                      0,
                      1
                    ]
                  }
                ]
              }
            ]
          }
        ]
      }
    }
  },
  {
    "$match": {
      "time": {
        "$gt": startHHmm,
        "$lt": endHHmm
      }
    }
  },
  {
    "$group": {
      "_id": {
        "publisherLocation": "$publisherLocation"
      },
      "averageWeight": {
        "$avg": "$weight"
      },
      "totalWeight": {
        "$sum": "$weight"
      },
    }
  },
  {
    "$project": {
      "publisherLocation": "$_id.publisherLocation",
      "averageWeight": "$averageWeight",
      "totalWeight": "$totalWeight"
    }
  }
];

  Publication.aggregate(query, function(error, results) {
    if(error) return callback(error);
    if(! results[0] ) return callback('nothing found...');

    var data = {};

    results.map(function(result) {
      data[result.publisherLocation] = {
        averageWeight: result.averageWeight,
        totalWeight: result.totalWeight,
        startDate: startTime,
        endDate: endTime,
        startTime: startHHmm,
        endTime: endHHmm,
      }
    })

    callback(null, data);
  });
}

/*
Deviation Percentage
Exposes the change between the cumulative publication weight of the last hour and the cumulative publication weight of the hour before last hour, as percentage. E.g.:
The cumulative publication weight between 19:00 and 20:00 = 100
The cumulative publication weight between 18:00 and 19:00 = 80
The deviation percentage = (100 / 80 * 100) = 125%
*/

function getGrowthSinceLastHour (callback) {

  var now = moment().tz("Europe/Amsterdam").subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss'); //change 'hours' to 'days' when using dev database
  var oneHourAgo = moment(now).tz("Europe/Amsterdam").subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss');
  var twoHoursAgo = moment(now).tz("Europe/Amsterdam").subtract(2, 'hours').format('YYYY-MM-DDTHH:mm:ss');

  async.series({
    now: function(itemCallback) {
      getCumulativePublicationWeightWithinTimeframe(oneHourAgo, now, itemCallback);      
    },
    lastHour: function(itemCallback) {
      getCumulativePublicationWeightWithinTimeframe(twoHoursAgo, oneHourAgo, itemCallback);      
    }
  }, function(error, results) {

    if(error) {
      Store.emit('last-hour-growth-error', error);
    } else {
      Store.emit('last-hour-growth', results);      
    }

    callback && callback(error, results);

  })

}

function getCumulativePublicationWeightWithinTimeframe(startTime, endTime, callback) {

  var start = new Date(startTime);
  var end = new Date(endTime);

  var query = [
    {
      "$match": {
        "date": {
          "$gte": start,
          "$lte": end,
        }
      }
    },
    {
      "$group": {
        "_id": {
          "publisherLocation": "$publisherLocation"
        },
        "averageWeight": {
          "$avg": "$weight"
        },
        "totalWeight": {
          "$sum": "$weight"
        },
      }
    },
    {
      "$project": {
        "publisherLocation": "$_id.publisherLocation",
        "averageWeight": "$averageWeight",
        "totalWeight": "$totalWeight"
      }
    }
  ];

  Publication.aggregate(query, function(error, results) {
    if(error) return callback(error);
    if(! (results[0]) ) return callback('nothing found...');

    var data = {};

    results.map(function(result) {
      data[result.publisherLocation] = {
        averageWeight: result.averageWeight,
        totalWeight: result.totalWeight,
        startDate: startTime,
        endDate: endTime,
      }
    })

    callback(null, data);
  });
}

/*
Historical Development
Represents a graph of day by day publication averages. Eg:
On Jan 1st 2016, the average publication weight = 5.4
On Jan 2nd 2016, the average publication weight = 5.7
On Jan 3rd 2016, the average publication weight = 5.12
Etcetera…
*/

function getAveragesPerDay(callback) {

  async.series({
    Amsterdam: function(itemCallback) {
      getLocationAveragesPerDay('Amsterdam', function(error, results) {
        if(error) return itemCallback(error);
        var items = results.map(function(result) {
          return {
            date: moment(result._id.year + '-' + result._id.month + '-' + result._id.day, 'YYYY-M-D').tz("Europe/Amsterdam").format('YYYYMMDD'),
            week: result._id.week,
            month: result._id.month,
            year: result._id.year,
            averageWeight: result.averageWeight
          }
        })
        items.sort(function(a, b) {
          return a.date - b.date
        })
        return itemCallback(null, items);
      });      
    },
    Paris: function(itemCallback) {
      getLocationAveragesPerDay('Paris', function(error, results) {
        if(error) return itemCallback(error);
        var items = results.map(function(result) {
          return {
            date: moment(result._id.year + '-' + result._id.month + '-' + result._id.day, 'YYYY-M-D').tz("Europe/Amsterdam").format('YYYYMMDD'),
            week: result._id.week,
            month: result._id.month,
            year: result._id.year,
            averageWeight: result.averageWeight
          }
        })
        items.sort(function(a, b) {
          return a.date - b.date
        })
        return itemCallback(null, items);
      });      
    },
    Berlin: function(itemCallback) {
      getLocationAveragesPerDay('Berlin', function(error, results) {
        if(error) return itemCallback(error);
        var items = results.map(function(result) {
          return {
            date: moment(result._id.year + '-' + result._id.month + '-' + result._id.day, 'YYYY-M-D').tz("Europe/Amsterdam").format('YYYYMMDD'),
            week: result._id.week,
            month: result._id.month,
            year: result._id.year,
            averageWeight: result.averageWeight
          }
        })
        items.sort(function(a, b) {
          return a.date - b.date
        })
        return itemCallback(null, items);
      });      
    },
  }, function(error, results) {

    if(error) {
      Store.emit('location-averages-per-day-error', error);
    } else {
      Store.emit('location-averages-per-day', results);      
    }

    callback && callback(error, results);

  })
}

function getLocationAveragesPerDay(locationName, callback) {
  var query = [
    {
      "$match": {
        "publisherLocation": locationName,
        "date": {
          "$gte": new Date("2016-01-01")
        }
      }
    },
    {
      "$group": {
        _id: {
          "day": {
            "$dayOfMonth": "$date"
          },
          "month": {
            "$month": "$date"
          },
          "year": {
            "$year": "$date"
          },
          "week": {
            "$week": "$date"
          }
        },
        "averageWeight": {
          "$avg": "$weight"
        }
      }
    }
  ]

  Publication.aggregate(query, function(error, data) {
    if(error) return callback(error);
    if(! (data[0] && data[0]._id) ) return callback('nothing found...');
    callback(null, data);
  });

}

/*
*/

function getAllTopInfluencers(callback) {
  async.series({
    hour: function(itemCallback) {
      getTopInfluencersLastHour(null, itemCallback)
    },
    day: function(itemCallback) {
      getTopInfluencersLastDay(null, itemCallback)
    },
    week: function(itemCallback) {
      getTopInfluencersLastWeek(null, itemCallback)
    },
    month: function(itemCallback) {
      getTopInfluencersLastMonth(null, itemCallback)
    },
    allTime: function(itemCallback) {
      getTopInfluencersAllTime(null, itemCallback)
    },
  }, function(error, data) {
    if(error) {
      Store.emit('top-influencers-error', error);
    } else {
      Store.emit('top-influencers', data);
    }

    callback && callback(error, data);

  })
}

function getTopPoliticalInfluencers(callback) {

  var politicians = [
    "LodewijkA",
    "mariannethieme",
    "jesseklaver",
    "APechtold",
    "sybrandbuma",
    "keesvdstaaij",
    "ncilla",
    "gertjansegers",
    "markrutte",
    "MinPres",
    "emileroemer",
    "tunahankuzu",
    "LavieJanRoos",
    "jndkgrf",
    "thierrybaudet",
    "HenkKrol",
    "hendriktenhoeve",
    "JacquesMonasch",
    "geertwilderspvv",
    "RegSprecher",
    "sigmargabriel",
    "b_riexinger",
    "katjakipping",
    "peter_simone",
    "cem_oezdemir",
    "BerndLucke",
    "c_lindner",
    "FraukePetry",
    "sekor",
    "FrankFranz",
    "MartinSonneborn",
    "jccambadelis",
    "fhollande",
    "FrancoisFillon",
    "bayrou",
    "JLMelenchon",
    "plaurent_pcf",
    "yjadot",
    "dupontaignan",
    "PhilippePoutou",
    "MLP_officiel",
    "n_arthaud",
    "jclagarde",
    "SylviaPinel",
    "D66",
    "PvdA",
    "SPnl",
    "PartijvdDieren",
    "VVD",
    "groenlinks",
    "DenkNL",
    "VoorNederland",
    "GeenPeil",
    "fvdemocratie",
    "christenunie",
    "SGPnieuws",
    "cdavandaag",
    "_pvv",
    "50pluspartij",
    "OSFractie",
    "Piratenpartij",
    "NieuwewegenNu",
    "AfD_Bund",
    "CDU",
    "spdde",
    "dieLinke",
    "GrueneLTBB",
    "CSU",
    "fdp",
    "Piratenpartei",
    "FW_aktuell",
    "ALFA_Partei",
    "BVBFW",
    "FAMILIEth",
    "BIWaktuell",
    "npdde",
    "oedpPresse",
    "DiePARTEI",
    "partisocialiste",
    "lesRepublicains",
    "EELV",
    "FDG",
    "LePG",
    "NPA_officiel",
    "CNPCF",
    "DLF_Officiel",
    "FN_officiel",
    "enmarchefr",
    "LutteOuvriere",
    "jlm_2017",
    "UDI_off",
    "GaucheUnitaire",
    "La_Fase",
    " republicetsocia",
    " CetA",
    "LesAlternatifs",
    " gauche_antiK"
  ];

  async.series({
    hour: function(itemCallback) {
      getTopInfluencersLastHour(politicians, itemCallback)
    },
    day: function(itemCallback) {
      getTopInfluencersLastDay(politicians, itemCallback)
    },
    week: function(itemCallback) {
      getTopInfluencersLastWeek(politicians, itemCallback)
    },
    month: function(itemCallback) {
      getTopInfluencersLastMonth(politicians, itemCallback)
    },
    allTime: function(itemCallback) {
      getTopInfluencersAllTime(politicians, itemCallback)
    },
  }, function(error, data) {
    if(error) {
      Store.emit('top-political-influencers-error', error);
    } else {
      Store.emit('top-political-influencers', data);
    }

    callback && callback(error, data);

  })
}

function getTopInfluencersLastHour(politicians, callback) {
  var startTime = moment().tz("Europe/Amsterdam").subtract(2, 'hours').format('YYYY-MM-DDTHH:mm:ss');
  getTopInfluencers(politicians, startTime, callback)
}

function getTopInfluencersLastDay(politicians, callback) {
  var startTime = moment().tz("Europe/Amsterdam").subtract(2, 'hours').subtract(1, 'days').format('YYYY-MM-DDTHH:mm:ss');
  getTopInfluencers(politicians, startTime, callback)
}

function getTopInfluencersLastWeek(politicians, callback) {
  var startTime = moment().tz("Europe/Amsterdam").subtract(2, 'hours').subtract(1, 'weeks').format('YYYY-MM-DDTHH:mm:ss');
  getTopInfluencers(politicians, startTime, callback)
}

function getTopInfluencersLastMonth(politicians, callback) {
  var startTime = moment().tz("Europe/Amsterdam").subtract(2, 'hours').subtract(1, 'month').format('YYYY-MM-DDTHH:mm:ss');
  getTopInfluencers(politicians, startTime, callback)
}

function getTopInfluencersAllTime(politicians, callback) {
  var startTime = moment('2016-01-01').tz("Europe/Amsterdam").format('YYYY-MM-DDTHH:mm:ss');
  getTopInfluencers(politicians, startTime, callback)
}

function getTopInfluencers(politicians, start, callback) {
  var query = [
    {
      "$match": {
        "date": {
          "$gte": new Date(start)
        },
        "publisherShortName": {
          "$exists": true
        }
      }
    },
    {
      "$project": {
        "publisherShortName": "$publisherShortName",
        "publisherName": "$publisherName",
        "weight": "$weight",
        "date": "$date",
        "publisherLocation": "$publisherLocation",
      }
    },
    {
      "$group": {
        _id: {
          "publisherLocation": "$publisherLocation",
          "publisherShortName": "$publisherShortName",
            "publisherName": "$publisherName",
        },
        "totalWeight": {
          "$sum": "$weight"
        },
      }
    },
    {
      "$sort": {
          "totalWeight": -1
       }
    },
    {
      "$limit": 100
    }
  ]

  if(politicians) query[0]["$match"].publisherShortName = { "$in": politicians }

  Publication.aggregate(query, function(error, results) {
    if(error) return callback(error);

    var data = {};

    results.map(function(result) {
      var resultLocation = result._id.publisherLocation;
      if(!data[resultLocation]) data[resultLocation] = [];
      data[resultLocation].push(result);
    })

    callback(null, data);
  });

}

mongoose.connection.on('error', function(error) {
  Store.emit('connection-error', error);
});

mongoose.connection.on('disconnected', function(){
  Store.emit('disconnected');
  if (!isConnectedBefore) return Store.connect();
});
mongoose.connection.on('connected', function() {
  isConnectedBefore = true;
  Store.emit('connected');
});

mongoose.connection.on('reconnected', function() {
  Store.emit('reconnected');
});

module.exports = main;
