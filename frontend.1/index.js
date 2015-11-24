var express = require('express');
var app = express();
/*app.get('/', function(req, res){
  res.send('Home');
  console.log('GET /');
});*/
app.use('/', express.static(__dirname+'/client'));
var server = app.listen(8001, function(){
  var host = server.address().address;
  var port = server.address().port;
  console.log('App listening at port %s', port);
});

