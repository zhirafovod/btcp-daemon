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

class FlowControl(object):
  ''' Flow Control '''

  def __init__(self, f, config='/etc/btcp/fc.conf'):
    ''' init '''
    self.f = f  # pointer to an object with external persistent data
  
  def run(self):
    ''' run Flow Control '''
    self._control()

  #   def _tack(self,interval=60): # less frequent, fix inconsistency
  #     ''' Consistency checking - run that function each 'interval' '''
  #     reactor.callLater(self.f.btcp.interval, self._tack)
  #     self.cleanDownloads()        # process files marked as finished
  #     self.checkInconsistency()    # check any inconsistency
  # 
  #   def _tick(self,interval=30): # more frequent
  #     ''' Perform all FLow Control logic - run that function each 'interval' '''
  #     self.f.btcp.interval = interval
  #     reactor.callLater(self.f.btcp.interval, self._tick)
  #     self.checkTorrents()          # 
  #     self.checkCassandraQueues()   # check torrents in 'dr' cassandra column family, add new download

  def _control(self,interval=60): # less frequent, fix inconsistency
    ''' run control flow instance '''  
    reactor.callLater(interval, self._control)
    self.f.data.Update()
    self.checkTorrents()          # 
    self.checkCassandraQueues()   # check torrents in 'dr' cassandra column family, add new download
    self.cleanDownloads()        # process files marked as finished
    self.checkInconsistency()    # check any inconsistency

  def checkInconsistency(self, stalled=300):
    ''' find torrents older than 'stalled', attempt to fix problem torrents '''
    for n in self.f.data.tc_torrents:
      d = int(time.time() - self.f.data.tc_torrents[n].activityDate)
      if stalled < d:
        self.cleanInconsitentDownload(n)      

  def checkTorrents(self):
    ''' check torrents in torrent client, add new or mark to cassandra finished downloads '''
     # get a list of torrents from Transmission torrent client and put it to hash tc_torrents 
    torrents = self.f.data.tc_torrents # list of transmissionbt.torrent objects 
    downloaded = self.f.data.downloaded # list of files that already have been downloaded
    for n in torrents:
      if n in downloaded: pass # torrent is already downloaded 
      elif torrents[n].status == 'seeding' and not n in downloaded: # has just finished downloading/added a new torrent to client
        self.checkAllDownloaded(n)                                            # check if all data receivers downloaded a file 'n', mark it as complete then
        self.markDownloaded(n) # mark a torrent as downloaded in cassandra
      elif torrents[n].status == 'finished': # has just finished downloading/added a new torrent to client
        self.removeDownloaded(n)                                            # check if all data receivers downloaded a file 'n', mark it as complete then
        

  def removeDownloaded(self, n):
    ''' remove a downloaded torrent from DR Cassandra queue, remove from Torrent Client '''
    self.f.data.cf['dr'].remove(self.f.config.node_name, (n,))

  def markDownloaded(self, n):
    ''' mark a torrent as downloaded in cassandra '''
    self.f.connector.cf['dr'].insert(self.f.config.node_name, {n: 'seeding'}) # changing status to seeding in 'dr'
    self.f.connector.cf['queue'].insert(n, {self.f.config.node_name: 'seeding'}) # changing status to seeding in 'queue'
    self.f.data.downloaded[n] = datetime.utcnow()
    logging.debug('markDownloaded(): file %s has just been finished downloading: %s' %(n, self.f.connector.cf['dr'].get(self.f.config.node_name, )))

  def startGroupDownload(self, drs, n, group):
    ''' if group is not started - publish torrent 'n' for a group of 'drs' '''
    logging.debug('startGroupDownload(): nodes %s, starting torrent file %s on node %s' %(str(drs), n, self.f.config.node_name,))

    btdata = self.f.connector.cf['files'].get(n)['btdata']
    torrent = decode(btdata)
    torrent['announce'] = 'http://%s:9200/ann?ls=topsecret' % (self.f.config.node_name,)
    btdata = encode(torrent)

    for x in self.f.btcp.tc.get_torrents():
      if x.name == n:
        self.f.btcp.tc.remove_torrent(x.id)    # no way to update tracker url, so will remove
        self.f.btcp.tc.add_torrent(base64.b64encode(btdata), download_dir = self.f.btcp.download_dir)    # add a new torrent

    key = 'btdata' + group
    self.f.connector.cf['files'].insert(n, {key: btdata})
    logging.debug('startGroupDownload(): published %s for file %s, announce %s' %(key, n, 'http://%s:9200/ann?ls=topsecret' % (self.f.config.node_name,),))

    for r in drs:    # insert file to each Data Receivers queue
      if r != self.f.config.node_name:
        self.f.connector.cf['dr'].insert(r, {n: 'group'})
        logging.debug('startGroupDownload(): dr.insert: group: %s, node: %s' %(n,r,))

    logging.debug('startGroupDownload(): Sucessfully inserted: f: %s, drs: %s' %(n, drs, ))

  def checkGroupDownloaded(self, drs, n):
    ''' check if download is not started in the group - publish torrent for a group '''
    # convert data receivers dict to string, example { 'sva1': 'seeding', 'sva2': '2', ...} => 'sva1,sva2,...'
    nodes = ','.join([x for x in self.f.connector.cf['queue'].get(n)])   
    nodesGrouped = self.f.btcp.groupByPattern(nodes)
    group = self.f.btcp.groupName(self.f.config.node_name)
    for dr in nodesGrouped[group]:
      if drs[dr] == '2':
        self.startGroupDownload(nodesGrouped[group], n, group)
        logging.debug('checkGroupDownloaded(): group %s started file f: %s' %(str(nodesGrouped[group]),n,))
        return None
    return None
    logging.debug('checkGroupDownloaded(): group %s is already downloading file %s' %(str(nodesGrouped[group]),n,))

  def checkAllDownloaded(self, n):
    ''' check if all data receivers downloaded a file 'n'
        if all finished - mark them as finished 
    '''
    try:    # fetch file queue properties
      drs = self.f.connector.cf['queue'].get(n)
    except pycassa.cassandra.ttypes.NotFoundException:
      logging.error('checkAllDownloaded(): file %s not in cassandra "queue"' %(n,))
      return None

    try:
      if drs[self.f.config.node_name] != 'groupdownloading' and drs[self.f.config.node_name] != 'group':
        self.checkGroupDownloaded(drs, n)    # check if a group download should be started
    except KeyError:
      logging.debug('checkAllDownloaded(): node is not in queue for file: %s' %(n,))

    for dr in drs:    # exits if not everybody finished
      if drs[dr] != 'seeding' and dr != self.f.config.node_name:
        logging.debug('checkAllDownloaded(): file %s not finished yet by node %s, status is %s' %(n,dr,drs[dr],))
        return None
    
    self.markAllDownloaded(drs, n)  # mark all downloaded, remove local data

  def markAllDownloaded(self, drs, n):
    ''' mark a torrent as downloaded for all nodes in cassandra '''
    logging.debug('markAllDownloaded(): file %s is downloaded by all nodes %s' %(n,str(drs),))
    for dr in drs:
      self.f.connector.cf['dr'].insert(dr, {n : 'finished'}) # change status for the torrent to finished for each DR
      logging.debug('markAllDownloaded(): file %s, changing status to finished for node %s in DR Cassandra queue' %(n,dr,))
    self.f.connector.cf['files'].insert(n, {'status' : 'finished'}) 
    logging.debug('markAllDownloaded(): file %s, changed status to finished in files Cassandra queue' %(n, ))
    self.f.connector.cf['queue'].remove(n)
    logging.debug('markAllDownloaded(): file %s, removed from queue Cassandra queue' %(n, ))
    del self.f.data.downloaded[n]
    logging.debug('markAllDownloaded(): file %s remove from hash self.f.data.downloaded' %(n, ))

  def cleanInconsitentDownload(self, n):
    ''' check torrent's status in cassandra, remove 'finished' torrent from local client '''
    logging.debug('cleanInconsitentDownload(): %s' %(n,))
    try:
      status = self.f.connector.cf['files'].get(n, columns=('status',)).items()[0][1]
    except pycassa.NotFoundException:
      print "Debug: %s: %s not found" %(self, n)
      return pycassa.NotFoundException
    if status == 'finished':
      try: 
        self.f.btcp.tc.remove_torrent(self.f.data.tc_torrents[n].id)
        logging.debug('cleanInconsitentDownload(): removed_torrent: %s' %(n,))
      except: 
        logging.debug('cleanInconsitentDownload(): failed to removed_torrent: %s' %(n,))
      try: 
        self.f.connector.cf['dr'].remove(self.f.config.node_name, self.f.connector.cf['dr'].get(self.f.config.node_name, (n,) ).iteritems().next() )
        logging.debug('cleanInconsitentDownload(): removed from dr: %s' %(n,))
      except: 
        logging.debug('cleanInconsitentDownload(): failed to remove from dr: %s' %(n,))
      try: 
        self.f.connector.cf['queue'].remove(n, self.f.connector.cf['queue'].get(n, (self.f.config.node_name,)).iteritems().next() )
        logging.debug('cleanInconsitentDownload(): removed from queue: %s' %(n,))
      except: 
        logging.debug('cleanInconsitentDownload(): failed to remove from queue: %s' %(n,))
    else:
      logging.debug('cleanInconsitentDownload(): status not finished: %s' %(n,))

  def cleanDownloads(self):
    ''' check torrents statuses in cassandra, remove torrents downloaded by all clients from cassandra '''
    logging.debug('cleanDownloads(): checking self.f.data.tc_torrents')
    for n in self.f.data.tc_torrents:
      if n in self.f.data.downloaded:
        try: 
          status = self.f.connector.cf['files'].get(n, columns=('status',)).items()[0][1]
          if status == 'finished':
            self.f.btcp.tc.remove_torrent(self.f.data.tc_torrents[n].id)
            logging.debug('cleanDownloads(): %s is downloaded %s, file status in queue is %s, torrent is removed from client' %(n, self.f.connector.cf['files'].get(n, ), status, ))
            self.f.connector.cf['dr'].remove(self.f.config.node_name, (n,))
            drs = self.f.connector.cf['files'].get(n, columns=('drs',)).itervalues().next()
            for dr in [ x for x in re.sub('\s', '', drs).split(',') ]:
              self.f.connector.cf['dr'].remove(dr, (n,))
              logging.debug('cleanDownloads(): removed %s from dr %s' %(n, dr, ))
            logging.debug('cleanDownloads(): removed %s from dr queue, node %s' %(n, self.f.config.node_name, ))
          else:
            logging.debug('cleanDownloads(): %s is downloaded %s, file status in queue is %s' %(n, self.f.connector.cf['files'].get(n, ), status, ))
        except pycassa.cassandra.ttypes.NotFoundException:
          logging.error('cleanDownloads(): file %s is in self.f.data.downloaded, but cannot clean cassandras queues: %s' %(n, str(sys.exc_info()), ))

  def checkCassandraQueues(self):
    ''' check torrents in 'dr' cassandra column family, add new downloads '''
    btcp = self.f.btcp
    try:
      ts = self.f.connector.cf['dr'].get(self.f.config.node_name) # torrents in DR queue
    except pycassa.cassandra.ttypes.NotFoundException:
      logging.debug('checkCassandraQueues() no hostname %s in dr cassandra queue' %(self.f.config.node_name,))
      return None
    for n in ts: 
      if ts[n] == 'new': btcp.new(n, btdata)
      elif ts[n] == 'group': btcp.group(n, btdata)
      elif ts[n] == 'finished': btcp.finish(n)     
      elif (ts[n] == 'seeding' or ts[n] == 'downloading') and n not in btcp.tc_torrents: btcp.fix(n)

class FormHandler(Resource): 
  ''' HTTP interface to debug BtCP work '''
  def __init__(self, f):
    ''' Map requests to functions '''
    self.c = { 
     '/getallfiles': 'GetAllFiles',
     '/favicon.ico': 'GetAllFiles', 
     '/request': 'DirRequest', 
     '/form': 'RenderForm', 
    } 
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

  def render_POST(self, request):
    ''' process POST request '''
    #logging.debug( 'request.args: ', request.args)
    btdata = request.args['btdata'][0]
    dr = request.args['dr'][0]
    #from StringIO import StringIO
    #fileh = StringIO(btdata)
    #logging.debug( 'fileh: ', type(fileh), dir(fileh), fileh)
    import tempfile
    tmpf, tmpfn = tempfile.mkstemp()
    logging.debug('%s, %s' %(tmpf, tmpfn))
    f = open(tmpfn, "w")
    f.write(btdata)
    f.close
    logging.debug( 'btdata: %s, len(dr): %s, dr: %s' %(type(btdata), type(dr), dr))
    r = self.f.btcp.publish(f = decode(btdata)['info']['name'], btdata = btdata, dr = dr)
    logging.debug('publishing btcp.fc.publish(f, btdata, dr): %s' %(r, ))
    #tmpf.write(btdata)
    #tmpf.close()
    return '<html><body>' + str(['<p>You submitted: len(%s): %s</p>' % (x, request.args[x][0]) for x in request.args]) + 'result is: %s' %(r,) + '</body></html>'

  def render_GET(self, request):
    ''' show form to upload files '''
    try:
      #return getattr(self, self.c[request.uri])(request)
      return self.RenderFileUploadForm(request)
    except KeyError:
      logging.debug( 'request: %s, unhandled error' % (request,))
      return 'request: %s, unhandled error' % (request,)
    except:
      logging.debug( 'unhandled error: ', sys.exc_info(), ', on request: ', request)
      return 'unhandled error: ', sys.exc_info(), ', on request: ', request

class CommandHandler(Resource): 
  def __init__(self, f, run=None):
    ''' Map run to method, store it to self.c '''
    self.f = f # pointer to factory object to store persistent data
    self.isLeaf = False
    logging.debug('CommandHandler.__init__: %s' % (self.__dict__,))
    logging.debug('something else')
    try:
      self.c = getattr(self, run)
    except KeyError:
      ''' no such method '''
      logging.debug('no such method to run: %s' % (run,))
      return 'no such method to run: %s' % (run,)
    #except:
    #  ''' unhandled error '''
    #  logging.debug('unhandled error: %s, run: %s' %(sys.exc_info(), run))
    #  return 'unhandled error: %s, run: %s' %(sys.exc_info(), run)

  def getAllFiles(self, request): 
    ''' show all files from the FC '''
    files = [ r for r in self.f.btcp.getallfiles()]
    logging.debug('GetAllFiles: type(files): %s, len(files): %s' %(type(files), len(files)))
    return '<p>fetched %s rows:</p><p>tcp.fc.getallfiles():</p>%s' %(len(files), [ '<p>%s</p>' %(x,) for x in files],) 

  def getAllData(self, request): 
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
    d = self.f.btcp.getalldata(keys=keys,limit=limit,pattern=pattern)
    logging.debug('GetAllData: type(d): %s\n' %(str(d),))
    return '<p>fetched data from %s queues:</p><p>tcp.fc.getalldata():</p>' %(len(d),) + ''.join([ '<p><b>%s</b></p> %s' % (k, ''.join(['<p>%s</p>' %(x,) for x in d[k]]),) for k in d]) 
      
  def saveBtFile(self, request): 
    ''' fetch torrent file data for file 'file' and save as 'saveas' '''
    logging.debug('request.args: %s' %(len(request.args),))
    # Fetch btdata
    btdata = self.f.btcp.getBtData(fn = request.args['file'][0])
    if btdata == pycassa.cassandra.ttypes.NotFoundException:
      return '404: File Not Found'
    logging.debug('btdata: type: %s, len: %s' %(type(btdata),len(btdata)))
    if not request.args['saveas']:
      import tempfile
      tmpf, tmpfn = tempfile.mkstemp()
      saveas = tmpfn
      logging.debug('mkstemp: %s, %s, saveas: %s' %(tmpf, tmpfn, saveas))
    else:
      saveas = request.args['saveas'][0]
    f = open(saveas, "w")
    f.write(btdata['btdata'])
    f.close
    return 'Success: %s' %(request.args,)

  def dirRequest(self, request): 
    ''' dir request namespace and attributes '''
    return "request.dir: ", dir(request), "request.__dict__: ", ['<p>%s</p>'%(x,) for x in request.__dict__.iteritems() ] 

  def render_GET(self, request):
    ''' show form to upload files '''
    try:
      ''' run a method for request, based on mapping in self.c '''
      return self.c(request)
    except KeyError:
      ''' if we do not have a method for that request in self.c - return error'''
      logging.debug( 'request: %s, no such method for request' % (request,))
      return 'request: %s, no such method' % (request,)
    #except:
    #  ''' unhandled error '''
    #  logging.debug( 'unhandled error: %s on request: %s' %(sys.exc_info(), request))
    #  return 'unhandled error: %s on request: %s' %(sys.exc_info(), request)

if __name__ == '__main__':
  reactor.run()
else:
  from twisted.python import log
  from twisted.python.logfile import DailyLogFile
  log.startLogging(DailyLogFile.fromFullPath('/var/tmp/' + "flowcontrol.log"))
  logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
