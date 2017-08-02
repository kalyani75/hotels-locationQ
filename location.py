import os

import json
import redis

from flask import Blueprint, jsonify, request, url_for, make_response, abort
from flask_cors import cross_origin

location = Blueprint('location', __name__)

if 'VCAP_SERVICES' in os.environ: 
	vcap_services = json.loads(os.environ['VCAP_SERVICES'])

	for key, value in vcap_services.iteritems():   # iter on both keys and values
		if key.find('redis') > 0:
		  redis_info = vcap_services[key][0]
		
	cred = redis_info['credentials']
	uri = cred['uri'].encode('utf8')
  
	redis = redis.StrictRedis.from_url(uri + '/0')
else:
  redis = redis.StrictRedis(host=os.getenv('REDIS_HOST', 'localhost'), port=os.getenv('REDIS_PORT', 6379), db=0)
    
@location.errorhandler(400)
def not_found(error):
  return make_response(jsonify( { 'error': 'Bad request' }), 400)

@location.errorhandler(404)
def not_found(error):
  return make_response(jsonify( { 'error': 'Not found' }), 404)

def getlocationfragments(prefix, pagelength):
  prefix = prefix.lower()
  listpart = 50

  start = redis.zrank('locationfragments', prefix)
  if start < 0: return []

  locationarray = []
  while (len(locationarray) != pagelength):
    range = redis.zrange('locationfragments', start, start + listpart - 1)
    start += listpart

    if not range or len(range) <= 0: 
      break

    for entry in range:
      minlen = min(len(entry), len(prefix))
      
      if entry[0:minlen] != prefix[0:minlen]:
        pagelength = len(locationarray)
        break

      if entry[-1] == '%' and len(locationarray) != pagelength: 
        location = {}
        
        locationfull = entry[0:-1]
        indexwithperc = locationfull.rfind('%')

        locationid = entry[indexwithperc + 1:-1]
        locationname = entry[0:indexwithperc] 
        
        locationproperties = redis.lrange(locationid, 0, -1)
        if len(locationproperties) > 0:
          location['id'] = locationproperties[0]
          location['displayname'] = locationproperties[1]
          location['acname'] = locationproperties[2]
          location['icon'] = locationproperties[3]
          location['latitude'] = locationproperties[4]
          location['longitude'] = locationproperties[5]
        
          locationarray.append(location)

  return locationarray

@location.route('/api/v1.0/locations/autocomplete/<prefix>', methods=['GET'])
@cross_origin()
def autocomplete(prefix):
  if request.args.get('pagelength') is None: pagelength = 20
  else: pagelength = int(request.args.get('pagelength'))

  locationarray = getlocationfragments(prefix, pagelength)

  locationcollection = {}
  locationcollection['locations'] = locationarray

  return json.dumps(locationcollection)  

def querylocationkeys(query):
  locations = []
  keys = redis.keys(query)

  for key in keys:
    locationattributes = redis.lrange(key, 0, -1)
    if len(locationattributes) > 0:
      location = {}
    
      location['id'] = locationattributes[0]
      location['displayname'] = locationattributes[1]
      location['acname'] = locationattributes[2]
      location['icon'] = locationattributes[3]
      location['latitude'] = locationattributes[4]
      location['longitude'] = locationattributes[5]    

      locations.append(location)

  return locations

def makepubliclocation(location):
  newlocation = {}
  
  for field in location:
    if field == 'id':
      newlocation['uri'] = url_for('location.getlocation', locationkey=location['id'], _external=True)
    
    newlocation[field] = location[field]
    
  return newlocation

@location.route('/api/v1.0/locations', methods=['GET'])
@cross_origin()
def getlocations():
  locations = []
  locations = querylocationkeys('L-*')
  
  return jsonify({ 'locations': map(makepubliclocation, locations) })

@location.route('/api/v1.0/locations/<int:locationkey>', methods=['GET'])
@cross_origin()
def getlocation(locationkey):
  locations = []
  locations = querylocationkeys('L-' + str(locationkey))
  
  return jsonify({ 'locations': map(makepubliclocation, locations) }) 

@location.route('/api/v1.0/locations', methods=['POST'])
@cross_origin()
def createlocation():
  if not request.json or not 'displayname' in request.json or not 'id' in request.json:
    abort(400)
 
  location = {
    'id': request.json['id'],
    'displayname': request.json['displayname'],
    'acname': request.json['acname'],    
    'icon': request.json.get('icon', ''),
    'latitude': request.json.get('latitude', 0),
    'longitude': request.json.get('longitude', 0)
  }
  
  locationname = location['acname']
  for l in range(1, len(locationname)):
    locationfragment = locationname[0:l]
    redis.zadd('locationfragments', 0, locationfragment)
  
  locationwithid = locationname + '%L-' + str(location['id']) + '%'
  redis.zadd('locationfragments', 0, locationwithid)

  locationkey = 'L-' + str(location['id'])
  redis.delete(locationkey)

  redis.rpush(locationkey, location['id'])
  redis.rpush(locationkey, location['displayname'])
  redis.rpush(locationkey, location['acname'])
  redis.rpush(locationkey, location['icon'])
  redis.rpush(locationkey, location['latitude'])
  redis.rpush(locationkey, location['longitude'])

  return jsonify({ 'location': location }), 201   

@location.route('/api/v1.0/locations/<int:locationkey>', methods = ['PUT'])
@cross_origin()
def updatelocation(locationkey):
  if not request.json: abort(400)
  locationkey = 'L-' + str(locationkey)

  location = {
    'id': request.json['id'],
    'displayname': request.json['displayname'],
    'acname': request.json['acname'],
    'icon': request.json.get('icon', ''),
    'latitude': request.json.get('latitude', 0),
    'longitude': request.json.get('longitude', 0)
  }

  redis.lset(locationkey, 0, location['id'])
  redis.lset(locationkey, 1, location['displayname'])
  redis.lset(locationkey, 2, location['acname'])  
  redis.lset(locationkey, 3, location['icon'])
  redis.lset(locationkey, 4, location['latitude'])
  redis.lset(locationkey, 5, location['longitude'])
  
  return jsonify({ 'locations': makepubliclocation(location) })

@location.route('/api/v1.0/locations/<int:locationkey>', methods = ['DELETE'])
@cross_origin()
def deletelocation(locationkey):
  locations = querylocationkeys('L-' + str(locationkey))

  if (len(locations)) <= 0: 
    return jsonify({ 'result': False })
  else:
    locationfullname = locations[0]['acname'] + '%L-' + str(locationkey) + '%'
    start = redis.zrank('locationfragments', locationfullname)
    
    previous = start - 1
    locationfragment = locationfullname

    commonfragment = redis.zrange('locationfragments', start + 1, start + 1)
    while (len(locationfragment) > 0):
      locationfragment = redis.zrange('locationfragments', previous, previous)
      
      if (locationfragment[0][-1] == '%' or (len(commonfragment) > 0 and locationfragment[0] == commonfragment[0][0:-1])): 
        break
      else:
        previous = previous - 1
     
    redis.zremrangebyrank('locationfragments', previous + 1, start)  
    redis.delete('L-' + str(locationkey))
    
    return jsonify( { 'result': True } )