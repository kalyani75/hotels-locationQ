import os
import json

import redis
import requests

from flask import Blueprint, jsonify, request, url_for, make_response, abort
from flask_cors import cross_origin

from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
location = Blueprint('location', __name__)

redispool = None
mqpool = None

if 'VCAP_SERVICES' in os.environ: 
  vcap_services = json.loads(os.environ['VCAP_SERVICES'])

  uri = ''
  for key, value in vcap_services.iteritems():   # iter on both keys and values
    if key.find('redis') > 0:
      rdb_info = vcap_services[key][0]
		
      cred = rdb_info['credentials']
      uri = cred['uri'].encode('utf8')

  redispool = redis.ConnectionPool.from_url(uri + '/0')
else:
  redispool = redis.ConnectionPool.from_url(os.getenv('REDISURI', 'redis://localhost:6379/0'))

PORT = int(os.getenv('PORT', '9011'))
ZIPKINURI = os.getenv('ZIPKINURI', 'http://zipkin:9411')
ZIPKINSAMPLERATE = float(os.getenv('ZIPKINSAMPLERATE', 100.0))

@location.errorhandler(400)
def not_found(error):
  return make_response(jsonify( { 'error': 'Bad request' }), 400)

@location.errorhandler(404)
def not_found(error):
  return make_response(jsonify( { 'error': 'Not found' }), 404)

def http_transport(encoded_span):
  # The collector expects a thrift-encoded list of spans.
  requests.post(ZIPKINURI + '/api/v1/spans', data=encoded_span, headers={'Content-Type': 'application/x-thrift'})  

@zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:getlocationfragments')
def getlocationfragments(prefix, pagelength):
  rdb = redis.StrictRedis(connection_pool=redispool)

  prefix = prefix.lower()
  listpart = 50

  start = rdb.zrank('locationfragments', prefix)
  if start < 0: return []

  locationarray = []
  while (len(locationarray) != pagelength):
    range = rdb.zrange('locationfragments', start, start + listpart - 1)
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
        
        locationproperties = rdb.lrange(locationid, 0, -1)
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
  with zipkin_span(service_name='hotels.com:locationquery', zipkin_attrs=ZipkinAttrs(trace_id=request.headers['X-B3-TraceID'],
    span_id=request.headers['X-B3-SpanID'], parent_span_id=request.headers['X-B3-ParentSpanID'], flags='1', is_sampled=
    request.headers['X-B3-Sampled']), span_name='locationquery:autocomplete', transport_handler=http_transport, port=PORT, 
    sample_rate=ZIPKINSAMPLERATE):   
    if request.args.get('pagelength') is None: pagelength = 20
    else: pagelength = int(request.args.get('pagelength'))

    locationarray = getlocationfragments(prefix, pagelength)

    locationcollection = {}
    locationcollection['locations'] = locationarray

    return json.dumps(locationcollection)  

@zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:querylocationkeys')
def querylocationkeys(query):
  rdb = redis.StrictRedis(connection_pool=redispool)

  locations = []
  keys = rdb.keys(query)

  for key in keys:
    locationattributes = rdb.lrange(key, 0, -1)
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

@zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:makepubliclocation')
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
  with zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:getlocations', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):  
    locations = []
    locations = querylocationkeys('L-*')
  
    return jsonify({ 'locations': map(makepubliclocation, locations) })

@location.route('/api/v1.0/locations/<int:locationkey>', methods=['GET'])
@cross_origin()
def getlocation(locationkey):
  with zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:getlocation', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):  
    locations = []
    locations = querylocationkeys('L-' + str(locationkey))
  
    return jsonify({ 'locations': map(makepubliclocation, locations) }) 

@location.route('/api/v1.0/locations', methods=['POST'])
@cross_origin()
def createlocation():
  with zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:createlocation', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):  
    rdb = redis.StrictRedis(connection_pool=redispool)
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
      rdb.zadd('locationfragments', 0, locationfragment)
  
    locationwithid = locationname + '%L-' + str(location['id']) + '%'
    rdb.zadd('locationfragments', 0, locationwithid)

    locationkey = 'L-' + str(location['id'])
    rdb.delete(locationkey)

    rdb.rpush(locationkey, location['id'])
    rdb.rpush(locationkey, location['displayname'])
    rdb.rpush(locationkey, location['acname'])
    rdb.rpush(locationkey, location['icon'])
    rdb.rpush(locationkey, location['latitude'])
    rdb.rpush(locationkey, location['longitude'])

    return jsonify({ 'location': location }), 201   

@location.route('/api/v1.0/locations/<int:locationkey>', methods = ['PUT'])
@cross_origin()
def updatelocation(locationkey):
  with zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:updatelocation', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):  
    rdb = redis.StrictRedis(connection_pool=redispool)
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

    rdb.lset(locationkey, 0, location['id'])
    rdb.lset(locationkey, 1, location['displayname'])
    rdb.lset(locationkey, 2, location['acname'])  
    rdb.lset(locationkey, 3, location['icon'])
    rdb.lset(locationkey, 4, location['latitude'])
    rdb.lset(locationkey, 5, location['longitude'])
  
    return jsonify({ 'locations': makepubliclocation(location) })

@location.route('/api/v1.0/locations/<int:locationkey>', methods = ['DELETE'])
@cross_origin()
def deletelocation(locationkey):
  with zipkin_span(service_name='hotels.com:locationquery', span_name='locationquery:updatelocation', transport_handler=http_transport,
    port=PORT, sample_rate=ZIPKINSAMPLERATE):   
    rdb = redis.StrictRedis(connection_pool=redispool)
    locations = querylocationkeys('L-' + str(locationkey))

    if (len(locations)) <= 0: 
      return jsonify({ 'result': False })
    else:
      locationfullname = locations[0]['acname'] + '%L-' + str(locationkey) + '%'
      start = rdb.zrank('locationfragments', locationfullname)
    
      previous = start - 1
      locationfragment = locationfullname

      commonfragment = rdb.zrange('locationfragments', start + 1, start + 1)
      while (len(locationfragment) > 0):
        locationfragment = rdb.zrange('locationfragments', previous, previous)
      
        if (locationfragment[0][-1] == '%' or (len(commonfragment) > 0 and locationfragment[0] == commonfragment[0][0:-1])): 
          break
        else:
          previous = previous - 1
     
      rdb.zremrangebyrank('locationfragments', previous + 1, start)  
      rdb.delete('L-' + str(locationkey))
    
      return jsonify( { 'result': True } )