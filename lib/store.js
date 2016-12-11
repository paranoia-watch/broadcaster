/*
 * @package tweet-crawler
 * @copyright Copyright(c) 2016 Paranoia Watch
 * @author Boris van Hoytema <boris AT newatoms DOT com>
 * @author Wouter Vroege <wouter AT woutervroege DOT nl>
 */

var mongoose = require('mongoose')
var events = require('events')
var async = require('async')
var moment = require('moment')
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
Peilingwijzer Data
Currently limited to 365 days
*/

function getPeilingwijzerData() {
  var q = Peilingwijzer.find({}).sort({_id: -1}).limit(365)
  q.exec(function(error, data) {
    if(error) {
      return Store.emit('peilingwijzer-data-error', error);
    }
    Store.emit('peilingwijzer-data', data);
  })
}

/*
Historical Deviation Percentage
Exposes the change between the publication average of today between now and last hour, versus all the publications published within that same timeframe older than last hour divided by the amount of days in the timeframe. E.g:
The average publication weight between now and one hour ago = 5
The average publication weight between between first publication and one hour ago = 4
The deviation percentage = (5 / 4 * 100) = 125%
*/

function getDeviationOfLastHour () {

  var now = moment().subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss'); //change 'hours' to 'days' when using dev database

  async.series({
    Amsterdam: function(itemCallback) {
      getLocationDeviationOfLastHour('Amsterdam', now, itemCallback);      
    },
    Paris: function(itemCallback) {
      getLocationDeviationOfLastHour('Paris', now, itemCallback);      
    },
    Berlin: function(itemCallback) {
      getLocationDeviationOfLastHour('Berlin', now, itemCallback);      
    },
  }, function(error, results) {

    if(error) {
      return Store.emit('last-hour-deviation-error', error);
    }

    Store.emit('last-hour-deviation', results);

  })

}

function getLocationDeviationOfLastHour (locationName, now, callback) {

  var oneHourAgo = moment(now).subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss');
  var startOfTime = moment('2016-01-01');

  async.series({
    now: function(itemCallback) {
      getAveragePublicationWeightWithinTimeframe(locationName, oneHourAgo, now, itemCallback);      
    },
    allTime: function(itemCallback) {
      getAveragePublicationWeightWithinTimeframe(locationName, startOfTime, oneHourAgo, itemCallback);      
    }
  }, function(error, results) {

    if(error) {
      return callback(error);
    }

    callback(null, {
      allTime: results.allTime,
      now: results.now,
      deviation: (results.now / results.allTime * 100) - 100
    })

  })
}

function getAveragePublicationWeightWithinTimeframe(locationName, startTime, endTime, callback) {

  var start = new Date(startTime);
  var end = new Date(endTime);

  var query = [
    {
      "$match": {
        "publisherLocation": locationName,
        "date":{
          "$gte": start,
          "$lte": end,
        }
      }
    },
    {
      "$group": {
        "_id": "average",
        "average": {
          "$avg": "$weight" 
        }
      }
    },
  ];

  Publication.aggregate(query, function(error, data) {
    if(error) return callback(error);
    if(! (data[0] && data[0].average) ) return callback('nothing found...');
    callback(null, data[0].average);
  });
}

/*
Deviation Percentage
Exposes the change between the cumulative publication weight of the last hour and the cumulative publication weight of the hour before last hour, as percentage. E.g.:
The cumulative publication weight between 19:00 and 20:00 = 100
The cumulative publication weight between 18:00 and 19:00 = 80
The deviation percentage = (100 / 80 * 100) = 125%
*/

function getGrowthSinceLastHour () {

  var now = moment().subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss'); //change 'hours' to 'days' when using dev database

  async.series({
    Amsterdam: function(itemCallback) {
      getLocationGrowthSinceLastHour('Amsterdam', now, itemCallback);      
    },
    Paris: function(itemCallback) {
      getLocationGrowthSinceLastHour('Paris', now, itemCallback);      
    },
    Berlin: function(itemCallback) {
      getLocationGrowthSinceLastHour('Berlin', now, itemCallback);      
    },
  }, function(error, results) {

    if(error) {
      return Store.emit('last-hour-growth-error', error);
    }

    Store.emit('last-hour-growth', results);

  })

}

function getLocationGrowthSinceLastHour (locationName, now, callback) {

  var oneHourAgo = moment(now).subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss');
  var twoHoursAgo = moment(now).subtract(2, 'hours').format('YYYY-MM-DDTHH:mm:ss');

  async.series({
    now: function(itemCallback) {
      getCumulativePublicationWeightWithinTimeframe(locationName, oneHourAgo, now, itemCallback);      
    },
    lastHour: function(itemCallback) {
      getCumulativePublicationWeightWithinTimeframe(locationName, twoHoursAgo, oneHourAgo, itemCallback);      
    }
  }, function(error, results) {

    if(error) {
      return callback(error);
    }

    callback(null, {
      lastHour: results.lastHour,
      now: results.now,
      growth: (results.now / results.lastHour * 100) - 100
    })

  })
}

function getCumulativePublicationWeightWithinTimeframe(locationName, startTime, endTime, callback) {

  var start = new Date(startTime);
  var end = new Date(endTime);

  var query = [
    {
      "$match": {
        "publisherLocation": locationName,
        "date":{
          "$gte": start,
          "$lte": end,
        }
      }
    },
    {
      "$group": {
        "_id": "total",
        "total": {
          "$sum": "$weight" 
        }
      }
    },
  ];

  Publication.aggregate(query, function(error, data) {
    if(error) return callback(error);
    if(! (data[0] && data[0].total) ) return callback('nothing found...');
    callback(null, data[0].total);
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

function getAveragesPerDay() {

  async.series({
    Amsterdam: function(itemCallback) {
      getLocationAveragesPerDay('Amsterdam', function(error, results) {
        if(error) return itemCallback(error);
        var items = results.map(function(result) {
          return {
            date: moment(result._id.year + '-' + result._id.month + '-' + result._id.day, 'YYYY-M-D').format('YYYYMMDD'),
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
            date: moment(result._id.year + '-' + result._id.month + '-' + result._id.day, 'YYYY-M-D').format('YYYYMMDD'),
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
            date: moment(result._id.year + '-' + result._id.month + '-' + result._id.day, 'YYYY-M-D').format('YYYYMMDD'),
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
      return Store.emit('location-averages-per-day-error', error);
    }

    Store.emit('location-averages-per-day', results);

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
