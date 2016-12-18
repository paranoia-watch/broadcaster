/*
 * @package broadcaster
 * @copyright Copyright(c) 2016 Paranoia Watch
 * @author Wouter Vroege <wouter AT woutervroege DOT nl>
 */

var events = require('events');
var Twit = require('twit');
var Tweeter = new events.EventEmitter();

function main (settings) {
  Tweeter._instance = _init(settings);
  Tweeter.post = postTweet;
  return Tweeter;
}

function _init(settings) {
  return Twit({
  consumer_key: settings.consumerKey,
  consumer_secret: settings.consumerSecret,
  access_token: settings.accessToken,
  access_token_secret: settings.accessSecret
  })
}

function postTweet(message) {
  Tweeter._instance.post('statuses/update', { status: message }, function(error, data, response) {
    if(error) Tweeter.emit('post-error', error);
    Tweeter.emit('post', data);
  })
}

module.exports = main;