#!/usr/bin/python

from pycassa.pool import ConnectionPool
import pycassa
from PythonBittorrent.torrent import Torrent
from PythonBittorrent.bencode import decode, encode
from urllib2 import quote
import json
import sys
from twisted.internet import reactor
from twisted.web.resource import Resource 
from twisted.web.server import Site
import time
import re
import cgi
import logging
import pickle
import transmissionrpc
import base64
import re
from datetime import datetime

class web(object):
  ''' Web Interface '''

  def __init__(self, f):
    ''' init '''
    self.f = f  # pointer to an object with external persistent data
    
  class UI(Resource): 
    ''' User Interface '''
    
    def __init__(self, f):
      self.f = f  # pointer to an object with external persistent data
      self.isLeaf = False
  
    def RenderFileUploadForm(self, request): 
      ''' Render a form to upload bittorrent 
        
      '''
      return '''<html><body><form action="/form"
      enctype="multipart/form-data" method="post">
      <p>
      Please select a bittorrent file, or a set of files:<br>
      <input type="file" name="btdata" size="40">
      </p>
      <p>
      Data Receivers:<br>
      <input type="text" name="dr" size="40">
      </p>
      <div>
      <input type="submit" value="Send">
      </div>
      </form></body></html>
      '''
  
    #     def render_POST(self, request):
    #       ''' process POST request '''
    #       #logging.debug( 'request.args: ', request.args)
    #       btdata = request.args['btdata'][0]
    #       dr = request.args['dr'][0]
    #       #from StringIO import StringIO
    #       #fileh = StringIO(btdata)
    #       #logging.debug( 'fileh: ', type(fileh), dir(fileh), fileh)
    #       import tempfile
    #       tmpf, tmpfn = tempfile.mkstemp()
    #       logging.debug('%s, %s' %(tmpf, tmpfn))
    #       f = open(tmpfn, "w")
    #       f.write(btdata)
    #       f.close
    #       logging.debug( 'btdata: %s, len(dr): %s, dr: %s' %(type(btdata), type(dr), dr))
    #       r = self.f.btcp.publish(f = decode(btdata)['info']['name'], btdata = btdata, dr = dr)
    #       logging.debug('publishing btcp.fc.publish(f, btdata, dr): %s' %(r, ))
    #       #tmpf.write(btdata)
    #       #tmpf.close()
    #       return '<html><body>' + str(['<p>You submitted: len(%s): %s</p>' % (x, request.args[x][0]) for x in request.args]) + 'result is: %s' %(r,) + '</body></html>'
    #    
    #     def render_GET(self, request):
    #       ''' show form to upload files '''
    #       try:
    #         #return getattr(self, self.c[request.uri])(request)
    #         return self.RenderFileUploadForm(request)
    #       except KeyError:
    #         logging.debug( 'request: %s, unhandled error' % (request,))
    #         return 'request: %s, unhandled error' % (request,)
    #       except:
    #         logging.debug( 'unhandled error: ', sys.exc_info(), ', on request: ', request)
    #         return 'unhandled error: ', sys.exc_info(), ', on request: ', request

  class API(Resource): 
    ''' API '''
    
    def __init__(self, f, method=None):
      ''' call method by name '''
      self.f = f # pointer to factory object to store persistent data
      self.isLeaf = False
      try:
        self.c = getattr(self, method)
      except KeyError:
        return 'no such method: %s' % (method,)
    
    def getData(self, request): 
      ''' show all files from the FC '''
      keys = None
      limit = None
      pattern = None
      if 'keys' in request.args:
        keys = request.args['keys'][0].split(',')
      if 'limit' in request.args:
        limit = int(request.args['limit'][0])
      if 'pattern' in request.args:
        pattern = request.args['pattern'][0]
      d = self.f.data.getData(keys=keys,limit=limit,pattern=pattern)
      return '<p>fetched data from %s queues:</p><p>tcp.fc.getalldata():</p>' %(len(d),) + ''.join([ '<p><b>%s</b></p> %s' % (k, ''.join(['<p>%s</p>' %(x,) for x in d[k]]),) for k in d]) 

    def render_GET(self, request):
      ''' run method 'request' and output result '''
      return self.c(request)
