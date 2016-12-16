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

Store.on('last-hour-deviation', function(data) {
  console.info('last-hour-deviation', data);
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
  console.info('top-influencers', JSON.stringify(data));
})

Store.on('top-influencers-error', function(error) {
  console.error('top-influencers-error', error);
})

Store.connect();
//Store.getGrowthSinceLastHour();
//Store.getDeviationOfLastHour();
Store.getTopInfluencersLastMonth();
//Store.getPeilingwijzerData();
//Store.getAveragesPerDay();