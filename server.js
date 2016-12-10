require('dotenv').config({silent: true})
var store = require('./lib/store'),
  Store = new store(process.env.DBURI);

Store.on('last-hour-growth', function(data) {
  console.info('last-hour-growth', data);
})

Store.on('last-hour-growth-error', function(error) {
  console.error('last-hour-growth-error', error);
})

Store.connect();
Store.getGrowthSinceLastHour();