require('dotenv').config({silent: true})
var store = require('./lib/store'),
  Store = new store(process.env.DBURI);

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
  console.info('last-hour-growth', data);
})

Store.on('last-hour-growth-error', function(error) {
  console.error('last-hour-growth-error', error);
})

Store.on('location-averages-per-day', function(data) {
  console.info('location-averages-per-day', JSON.stringify(data));
})

Store.on('location-averages-per-day-error', function(error) {
  console.error('location-averages-per-day-error', error);
})

Store.connect();
//Store.getGrowthSinceLastHour();
Store.getAveragesPerDay();