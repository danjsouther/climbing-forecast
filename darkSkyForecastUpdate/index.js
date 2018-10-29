const aws = require('aws-sdk');
const request = require('request');

aws.config.update({region: 'us-east-2'});
aws.config.apiVersions = {
  dynamodb: '2012-08-10'
};

const dynamodb = new aws.DynamoDB();
const docClient = new aws.DynamoDB.DocumentClient();

const metrics = [
  "sunriseTime",
  "sunsetTime",
  "moonPhase",
  "precipIntensity",
  "precipIntensityMax",
  "precipIntensityMaxTime",
  "precipProbability",
  "precipAccumulation",
  "precipType",
  "temperatureHigh",
  "temperatureHighTime",
  "temperatureLow",
  "temperatureLowTime",
  "apparentTemperatureHigh",
  "apparentTemperatureHighTime",
  "apparentTemperatureLow",
  "apparentTemperatureLowTime",
  "dewPoint",
  "humidity",
  "pressure",
  "windSpeed",
  "windBearing",
  "cloudCover",
  "uvIndex",
  "uvIndexTime",
  "visibility"
]

exports.handler = function(event, contect, callback){

  // Get climbing list of climbing locations
  getLocations().then(function(locations){
    return Promise.all( locations.map(getForecast) );

  }).then(function(forecasts){
    return Promise.all( forecasts.map(saveForecast) );

  }).then(() => callback(null, "success"))
  .catch(callback);
};

function getLocations(){
  return new Promise(function(resolve, reject){
    const params = { TableName: 'ClimbingLocations' };
    docClient.scan(params, function(error, results){
      if (error) {
        reject(error);
      } else {
        resolve(results.Items);
      }
    });
  });
}

function getForecast(location){
  return new Promise(function(resolve,reject){
    const exclude = ['currently','minutely','alerts','flags','hourly'];
    const url = `https://api.darksky.net/forecast/${process.env.darkSkySecret}/${location.latitude},${location.longitude}?exclude=${exclude.join(',')}`;
    request.get(url, function(error, response, body){
      if (error) {
        reject(error);
      } else {
        const results = JSON.parse(body);
        results.location = location.name;
        resolve(results);
      }
    });
    
  });
}

function saveForecast(forecast){
  new Promise(function(resolve, reject){
    const puts = setupRequest(forecast).reduce(function(a,b){return a.concat(b)},[]);
    if (puts.length) {
      const params = {
        RequestItems: {
          'DarkSkyForecasts': puts
        }
      };
      dynamodb.batchWriteItem(params, function(error,results){
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    } else {
      resolve();
    }
  });
}

function setupRequest(forecast){
  if ("daily" in forecast) {
    return forecast.daily.data.map(function(dayForecast){
      const item = {
        PutRequest: {
          Item: {
            "location": { S: forecast.location },
            "time": { N: dayForecast.time.toString() },
            "latitude": { N: forecast.latitude.toString() },
            "longitude": { N: forecast.longitude.toString() },
            "icon": { S: dayForecast.icon },
            "summary": { S: dayForecast.summary }
          }
        }
      };
      metrics.filter(m => m in forecast.daily.data)
      .map(m => item.PutRequest.Item[m] = { N: dayForecast[m].toString() })
      return item
    });
  } else {
    return [];
  }
    
}
