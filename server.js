/*
 * @package broadcaster
 * @copyright Copyright(c) 2016 Paranoia Watch
 * @author Wouter Vroege <wouter AT woutervroege DOT nl>
 */

require('dotenv').config({silent: true})
var async = require('async');

var http = require('http')
var socketio = require('socket.io')

var httpServer = http.createServer()
var io = socketio(httpServer)

httpServer.listen(4000, function() {
  console.log('httpServer started!');
})

var TOP_INFLUENCERS = {};
var TOP_POLITICAL_INFLUENCERS = {};
var LAST_HOUR_GROWTH = {};
var LAST_HOUR_DEVIATION = {};
var LOCATION_AVERAGES_PER_DAY = {};
var PEILINGWIJZER_DATA = {};

var MOST_INFLUENCIAL_TWITTER_USERS = {Amsterdam: null, Berlin: null, Paris: null};
var MOST_INFLUENCIAL_POLITICIANS = {Amsterdam: null, Berlin: null, Paris: null};

io.sockets.on("connection", function (s) {
  io.emit('top-influencers', TOP_INFLUENCERS);
  io.emit('top-political-influencers', TOP_POLITICAL_INFLUENCERS);
  io.emit('last-hour-growth', LAST_HOUR_GROWTH);
  io.emit('last-hour-deviation', LAST_HOUR_DEVIATION);
  io.emit('location-averages-per-day', LOCATION_AVERAGES_PER_DAY);
  io.emit('peilingwijzer-data', PEILINGWIJZER_DATA);
});

var store = require('./lib/store');
var Store = new store(process.env.DBURI);

Store.on('connected', function() {
  console.info('store connected');
  runAll();
})

Store.on('reconnected', function() {
  console.info('store reconnected');
})

Store.on('connection-error', function(error) {
  console.warn('store connection error', error);
})

Store.on('disconnected', function() {
  console.warn('store disconnected');
})

Store.on('last-hour-growth', function(data) {
  LAST_HOUR_GROWTH = data;
  io.sockets.emit('last-hour-growth', LAST_HOUR_GROWTH);
  //console.info('last-hour-growth', JSON.stringify(LAST_HOUR_GROWTH));
})

Store.on('last-hour-growth-error', function(error) {
  console.error('last-hour-growth-error', error);
})

Store.on('last-hour-deviation', function(data) {
  LAST_HOUR_DEVIATION = data;
  io.sockets.emit('last-hour-deviation', LAST_HOUR_DEVIATION);
  //console.info('last-hour-deviation', JSON.stringify(LAST_HOUR_DEVIATION));
})

Store.on('last-hour-deviation-error', function(error) {
  console.error('last-hour-deviation-error', error);
})

Store.on('location-averages-per-day', function(data) {
  LOCATION_AVERAGES_PER_DAY = data;
  io.sockets.emit('location-averages-per-day', LOCATION_AVERAGES_PER_DAY);
  //console.info('location-averages-per-day', JSON.stringify(LOCATION_AVERAGES_PER_DAY));
})

Store.on('location-averages-per-day-error', function(error) {
  console.error('location-averages-per-day-error', error);
})

Store.on('peilingwijzer-data', function(data) {
  PEILINGWIJZER_DATA = data;
  io.sockets.emit('peilingwijzer-data', PEILINGWIJZER_DATA);
  //console.info('peilingwijzer-data', JSON.stringify(PEILINGWIJZER_DATA));
})

Store.on('peilingwijzer-data-error', function(error) {
  console.error('peilingwijzer-data-error', error);
})

Store.on('top-influencers', function(data) {
  TOP_INFLUENCERS = data;
  io.sockets.emit('top-influencers', TOP_INFLUENCERS);
  //console.info('top-influencers', JSON.stringify(TOP_INFLUENCERS));
  setMostInfluencialTwitterUsers();
})

Store.on('top-influencers-error', function(error) {
  console.error('top-influencers-error', error);
})

Store.on('top-political-influencers', function(data) {
  TOP_POLITICAL_INFLUENCERS = data;
  io.sockets.emit('top-political-influencers', TOP_POLITICAL_INFLUENCERS);
  //console.info('top-political-influencers', JSON.stringify(TOP_POLITICAL_INFLUENCERS));
  setMostInfluencialPoliticians();
})

Store.on('top-political-influencers-error', function(error) {
  console.error('top-political-influencers-error', error);
})

Store.connect();

function runAll() {
  async.series([
    // function(itemCallback) {
    //   console.time('getGrowthSinceLastHour');
    //   Store.getGrowthSinceLastHour(function() {
    //     console.timeEnd('getGrowthSinceLastHour');
    //     itemCallback();
    //   });
    // },
    // function(itemCallback) {
    //   console.time('getDeviationOfLastHour');
    //   Store.getDeviationOfLastHour(function() {
    //     console.time('getDeviationOfLastHour');        
    //     itemCallback();
    //   });
    // },
    // function(itemCallback) {
    //   console.time('getAveragesPerDay');
    //   Store.getAveragesPerDay(function() {
    //     console.timeEnd('getAveragesPerDay');
    //     itemCallback();
    //   });
    // },
    // function(itemCallback) {
    //   console.time('getPeilingwijzerData');
    //   Store.getPeilingwijzerData(function() {
    //     console.timeEnd('getPeilingwijzerData');
    //     itemCallback();
    //   });
    // },
    // function(itemCallback) {
    //   console.time('getAllTopInfluencers');
    //   Store.getAllTopInfluencers(function() {
    //     console.timeEnd('getAllTopInfluencers');
    //     itemCallback();
    //   });
    // },
    function(itemCallback) {
      console.time('getTopPoliticalInfluencers');
      Store.getTopPoliticalInfluencers(function() {
        console.timeEnd('getTopPoliticalInfluencers');
        itemCallback();
      });
    }
  ], function(error, data) {
    if(error)
    console.info('done doing series...');
    runAll();
  })
}

var tweeter = require('./lib/tweeter');
var Tweeter = new tweeter({
  consumerKey: process.env.TWITTER_CONSUMER_KEY,
  consumerSecret: process.env.TWITTER_CONSUMER_SECRET,
  accessToken: process.env.TWITTER_ACCESS_TOKEN,
  accessSecret: process.env.TWITTER_ACCESS_SECRET
});

Tweeter.on('post-error', function(error) {
  console.error('Error while tweeting post', error);
})

Tweeter.on('post', function(post) {
  console.error('Tweeted Post', post);
})

function setMostInfluencialTwitterUsers() {
  var dayInfluencers = TOP_INFLUENCERS.day;
  for(var i in dayInfluencers) {
    var locationInfluencer = dayInfluencers[i][0]._id.publisherShortName;
    if(MOST_INFLUENCIAL_TWITTER_USERS[i] !== locationInfluencer) {
      MOST_INFLUENCIAL_TWITTER_USERS[i] = locationInfluencer;

      var msg = 'The most influencial twitter user of the last 24 hours in ' + [i] + ' is @' + locationInfluencer;
      //Tweeter.post(msg)
      console.info(msg);

    }
  }
}

function setMostInfluencialPoliticians() {
  var dayInfluencers = TOP_POLITICAL_INFLUENCERS.day;
  for(var i in dayInfluencers) {
    var locationInfluencer = dayInfluencers[i][0]._id.publisherShortName;
    if(MOST_INFLUENCIAL_POLITICIANS[i] !== locationInfluencer) {
      MOST_INFLUENCIAL_POLITICIANS[i] = locationInfluencer;

      var msg = 'The most influencial political twitter user of the last 24 hours in ' + [i] + ' is @' + locationInfluencer;
      //Tweeter.post(msg)
      console.info(msg);

    }
  }
}