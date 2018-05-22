
const pg = require('pg');

/*
 * Subscribe to a Postgres LISTEN/NOTIFY channel with reconnection and backoff
 *   Even with docker, the volume of connections are low enough we don't need variable retires/jitter.
 *   At a scale where that is necessary - we should be using a more advanced pub/sub system instead. 
*/
const defaultBackoff = 500;
const subscribeToChannel = (pgConnectionString, channelName, callback, count, backoff) => {

    let pgConnection = new pg.Client(pgConnectionString);
    pgConnection.connect();

    pgConnection.on('notification',function(msg) {
        // if a successful message than any backoff goes back to default
        backoff = defaultBackoff;

        // callback is the actual business logic handler for receiving a message
        callback(msg,function(err) { if(err) client.end(); });
    });

    pgConnection.on('error', function(error) {
      console.log('LISTEN/NOTIFY error: ', error);
      if(errorActive[channelName] === true) {
          return;
      } else {
        backoff *= 2;
        if (backoff > 8000) backoff = 8000;

        // the errorActive handles the nodejs eventEmitter flooding with parallel errors
        //   which occurs when resuming from sleep on windows 
        if (errorActive[channelName] != true) {
            errorActive[channelName] = true;
            pgConnection.end();
            pgConnection = null;
            setTimeout(function () {
                subscribeToChannel(pgConnectionString, channelName, callback, count++, backoff);
                errorActive[channelName] = false;
            }, backoff);
        }
      }
    });

    pgConnection.query("LISTEN " + channelName,function(err) {
       // this happens one-time so does not flood like the error listener above
       if(err) {
         pgConnection.end();
         pgConnection = null;
         setTimeout(function() {
             subscribeToChannel(pgConnectionString, channelName, callback, count++, backoff);
         }, backoff);
       };
    });

}

// scope errorActive and backoff to specific channel usage
let errorActive = {};
const subscribeToChannelWrapper = (pgConnectionString, channelName, callback, count = 0) => {
  let backoff = defaultBackoff;
  errorActive[channelName] = false;
  subscribeToChannel(pgConnectionString, channelName, callback, count, backoff);
}

module.exports = subscribeToChannelWrapper;

