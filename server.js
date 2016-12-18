require('dotenv').config({silent: true})

var http = require('http')
var socketio = require('socket.io')

var httpServer = http.createServer()
var io = socketio(httpServer)

httpServer.listen(4000, function() {
  console.log('httpServer started!');
})

io.sockets.on("connection", function (s) {
  io.emit('top-influencers', TOP_INFLUENCERS);
});

var store = require('./lib/store');
var Store = new store(process.env.DBURI);

var TOP_INFLUENCERS = {};

Store.on('connected', function() {
  console.info('store connected');
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
  console.info('last-hour-growth', JSON.stringify(data));
})

Store.on('last-hour-growth-error', function(error) {
  console.error('last-hour-growth-error', error);
})

Store.on('last-hour-deviation', function(data) {
  console.info('last-hour-deviation', JSON.stringify(data));
})

Store.on('last-hour-deviation-error', function(error) {
  console.error('last-hour-deviation-error', error);
})

Store.on('location-averages-per-day', function(data) {
  console.info('location-averages-per-day', JSON.stringify(data));
})

Store.on('location-averages-per-day-error', function(error) {
  console.error('location-averages-per-day-error', error);
})

Store.on('peilingwijzer-data', function(data) {
  console.info('peilingwijzer-data', JSON.stringify(data));
})

Store.on('peilingwijzer-data-error', function(error) {
  console.error('peilingwijzer-data-error', error);
})

Store.on('top-influencers', function(data) {
  TOP_INFLUENCERS = data;
  io.sockets.emit('top-influencers', TOP_INFLUENCERS);
  console.info('top-influencers', JSON.stringify(TOP_INFLUENCERS));
})

Store.on('top-influencers-error', function(error) {
  console.error('top-influencers-error', error);
})

Store.connect();
//Store.getGrowthSinceLastHour();
//Store.getDeviationOfLastHour();
Store.getAllTopInfluencers();
//Store.getPeilingwijzerData();
//Store.getAveragesPerDay();