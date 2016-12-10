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

function main (databaseUri) {
  Store.databaseUri = databaseUri;
  Store.getGrowthSinceLastHour = getGrowthSinceLastHour;
  Store.connect = getDBConnection;
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

function getGrowthSinceLastHour () {

  var now = moment().subtract(1, 'hours').format('YYYY-MM-DDTHH:mm:ss');

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
      growth: results.now/results.lastHour*100
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