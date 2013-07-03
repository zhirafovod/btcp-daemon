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
  ''' Flow Control Server '''

  def __init__(self, f, config='/etc/btcp/fc.conf'):
    ''' !!! Read config here '''
    self.f = f  # pointer to an object with external persistent data
    logging.debug('FlowControl.__init__() finished')

  def _tack(self,interval=300): # less frequent
    ''' Consistency checking - run that function each 'interval' '''
    logging.debug('FlowControl._tack()')
    #reactor.callLater(self.f.btcp.interval, self._tack)
    #self.cleanDownloads() # check torrents statuses in cassandra, remove torrents downloaded by all clients from cassandra

  def _tick(self,interval=30): # more frequent
    ''' Perform all FLow Control logic - run that function each 'interval' '''
    logging.debug('FlowControl._tick()')
    self.f.btcp.interval = interval
    reactor.callLater(self.f.btcp.interval, self._tick)
    self.checkTorrents()          # 
    self.checkCassandraQueues()   # check torrents in 'dr' cassandra column family, add new download

  def checkTorrents(self):
    ''' check torrents in torrent client, add new or mark to cassandra finished downloads '''
    logging.debug('checkTorrents(): self.f.btcp.tc_torrents: %s, self.f.btcp.downloaded: %s' %(self.f.btcp.tc_torrents, self.f.btcp.downloaded, ))
    self.f.btcp.checkTransmission() # get a list of torrents from Transmission torrent client and put it to hash tc_torrents 
    for n in self.f.btcp.tc_torrents:
      if n in self.f.btcp.downloaded: # torrent is already downloaded 
        logging.debug('checkTorrents(): has finished download of %s' %(n,))
      elif self.f.btcp.tc_torrents[n].status == 'seeding' and not n in self.f.btcp.downloaded: # has just finished downloading/added a new torrent to client
        self.markDownloaded(n) # mark a torrent as downloaded in cassandra
        self.checkAllDownloaded(n)                                            # check if all data receivers downloaded a file 'n', mark it as complete then
      elif self.f.btcp.tc_torrents[n].status == 'finished': # has just finished downloading/added a new torrent to client
        self.removeDownloaded(n)                                            # check if all data receivers downloaded a file 'n', mark it as complete then
        logging.debug('checkTorrents(): file %s, not finished yet: %s' %(n, self.f.btcp.cf['dr'].get(self.f.btcp.node_name, )))
      else:
        logging.debug('checkTorrents(): skipping - file: %s, status: %s ' %(n, self.f.btcp.tc_torrents[n].status))

  def removeDownloaded(self, n):
    ''' remove a downloaded torrent from DR Cassandra queue, remove from Torrent Client '''
    self.f.btcp.cf['dr'].remove(self.f.btcp.node_name, (n,))
    logging.debug('removeDownloaded(): file %s removed from my DR Cassandra queue' %(n,))

  def markDownloaded(self, n):
    ''' mark a torrent as downloaded in cassandra '''
    self.f.btcp.cf['dr'].insert(self.f.btcp.node_name, {n: 'seeding'}) # changing status to seeding in 'dr'
    self.f.btcp.cf['queue'].insert(n, {self.f.btcp.node_name: 'seeding'}) # changing status to seeding in 'queue'
    self.f.btcp.downloaded[n] = datetime.utcnow()
    logging.debug('markDownloaded(): file %s has just been finished downloading: %s' %(n, self.f.btcp.cf['dr'].get(self.f.btcp.node_name, )))

  def startGroupDownload(self, drs, n, group):
    ''' if group is not started - publish torrent 'n' for a group of 'drs' '''
    logging.debug('startGroupDownload(): nodes %s, starting torrent file %s on node %s' %(str(drs), n, self.f.btcp.node_name,))

    btdata = self.f.btcp.cf['files'].get(n)['btdata']
    torrent = decode(btdata)
    torrent['announce'] = 'http://%s:9200/ann?ls=topsecret' % (self.f.btcp.node_name,)
    btdata = encode(torrent)

    for x in self.f.btcp.tc.get_torrents():
      if x.name == n:
	self.f.btcp.tc.remove_torrent(x.id)    # no way to update tracker url, so will remove
    self.f.btcp.tc.add_torrent(base64.b64encode(btdata), download_dir = self.f.btcp.download_dir)    # add a new torrent

    key = 'btdata' + group
    self.f.btcp.cf['files'].insert(n, {key: btdata})
    logging.debug('startGroupDownload(): published %s for file %s, announce %s' %(key, n, 'http://%s:9200/ann?ls=topsecret' % (self.f.btcp.node_name,),))

    for r in drs:    # insert file to each Data Receivers queue
      if r != self.f.btcp.node_name:
        self.f.btcp.cf['dr'].insert(r, {n: 'group'})
        logging.debug('startGroupDownload(): dr.insert: group: %s, node: %s' %(n,r,))

    logging.debug('startGroupDownload(): Sucessfully inserted: f: %s, drs: %s' %(n, drs, ))

  def checkGroupDownloaded(self, drs, n):
    ''' check if download is not started in the group - publish torrent for a group '''
    # convert data receivers dict to string, example { 'sva1': 'seeding', 'sva2': '2', ...} => 'sva1,sva2,...'
    nodes = ','.join([x for x in self.f.btcp.cf['queue'].get(n)])   
    nodesGrouped = self.f.btcp.groupByPattern(nodes)
    group = self.f.btcp.groupName(self.f.btcp.node_name)
    for dr in nodesGrouped[group]:
      if drs[dr] == '2':
        self.startGroupDownload(nodesGrouped[group], n, group)
	return None
    logging.debug('checkGroupDownloaded(): group %s is already downloading file %s' %(str(nodesGrouped[group]),n,))

  def checkAllDownloaded(self, n):
    ''' check if all data receivers downloaded a file 'n'
        if all finished - mark them as finished 
    '''
    try:    # fetch file queue properties
      drs = self.f.btcp.cf['queue'].get(n)
    except pycassa.cassandra.ttypes.NotFoundException:
      logging.error('checkAllDownloaded(): file %s not in cassandra "queue"' %(n,))
      return None

    self.checkGroupDownloaded(drs, n)    # check if a group download should be started

    for dr in drs:    # exits if not everybody finished
      if drs[dr] != 'seeding':
        logging.debug('checkAllDownloaded(): file %s not finished yet by node %s, status is %s' %(n,dr,drs[dr],))
        return None
    
    self.markAllDownloaded(drs, n)  # mark all downloaded, remove local data

  def markAllDownloaded(self, drs, n):
    ''' mark a torrent as downloaded for all nodes in cassandra '''
    logging.debug('markAllDownloaded(): file %s is downloaded by all nodes %s' %(n,str(drs),))
    for dr in drs:
      self.f.btcp.cf['dr'].insert(dr, {n : 'finished'}) # change status for the torrent to finished for each DR
      logging.debug('markAllDownloaded(): file %s, changing status to finished for node %s in DR Cassandra queue' %(n,dr,))
    self.f.btcp.cf['files'].insert(n, {'status' : 'finished'}) 
    logging.debug('markAllDownloaded(): file %s, changed status to finished in files Cassandra queue' %(n, ))
    self.f.btcp.cf['queue'].remove(n)
    logging.debug('markAllDownloaded(): file %s, removed from queue Cassandra queue' %(n, ))
    del self.f.btcp.downloaded[n]
    logging.debug('markAllDownloaded(): file %s remove from hash self.f.btcp.downloaded' %(n, ))

  def cleanDownloads(self):
    ''' check torrents statuses in cassandra, remove torrents downloaded by all clients from cassandra '''
    logging.debug('cleanDownloads(): self.f.btcp.tc_torrents: %s, self.f.btcp.downloaded: %s' %(self.f.btcp.tc_torrents, self.f.btcp.downloaded, ))
    for n in self.f.btcp.tc_torrents:
      if n in self.f.btcp.downloaded:
        try: 
          status = self.f.btcp.cf['files'].get(n, columns=('status',)).items()[0]
          if status == 'finished':
            self.f.btcp.tc.remove_torrent(self.f.btcp.downloaded[n].id)
            logging.debug('cleanDownloads(): %s is downloaded %s, file status in queue is %s, torrent is removed from client' %(n, self.f.btcp.cf['files'].get(n, ), status, ))
            self.f.btcp.cf['dr'].remove(self.f.btcp.node_name, (n,))
            drs = self.f.btcp.cf['files'].get(n, columns=('drs',)).itervalues().next()
            for dr in [ x for x in re.sub('\s', '', drs).split(',') ]:
              self.f.btcp.cf['dr'].remove(dr, (n,))
              logging.debug('cleanDownloads(): removed %s from dr %s' %(n, dr, ))
            logging.debug('cleanDownloads(): removed %s from dr queue, node %s' %(n, self.f.btcp.node_name, ))
          else:
            logging.debug('cleanDownloads(): %s is downloaded %s, file status in queue is %s' %(n, self.f.btcp.cf['files'].get(n, ), status, ))
        except pycassa.cassandra.ttypes.NotFoundException:
          logging.error('cleanDownloads(): file %s is in self.f.btcp.downloaded, but cannot clean cassandras queues: %s' %(n, str(sys.exc_info()), ))

  def checkCassandraQueues(self):
    ''' check torrents in 'dr' cassandra column family, add new downloads '''
    logging.debug('checkCassandraQueues()')
    try:
      ts = self.f.btcp.cf['dr'].get(self.f.btcp.node_name) # torrents in DR queue
    except pycassa.cassandra.ttypes.NotFoundException:
      logging.debug('checkCassandraQueues() no hostname %s in dr cassandra queue' %(self.f.btcp.node_name,))
      return None
    for n in ts:
      if ts[n] == 'new':
        logging.debug('checkCassandraQueues() %s is a new status, adding torrent to downloads...' %(n,))
        btdata = self.f.btcp.cf['files'].get(n)['btdata']
        self.f.btcp.add_torrent(n, btdata) 
        self.f.btcp.cf['dr'].insert(self.f.btcp.node_name, {n: 'downloading'}) # change status to downloading
        self.f.btcp.cf['queue'].insert(n, {self.f.btcp.node_name: 'downloading'}) # change status to downloading
        self.f.btcp.checkTransmission() # update torrents list
        logging.debug('checkCassandraQueues() %s torrent added to torrent client, status in cassandra changed to downloading...' %(n,))
      elif ts[n] == 'group':    # new status for group download
        logging.debug('checkCassandraQueues() %s is a group status, adding torrent to downloads...' %(n,))
        group = self.f.btcp.groupName(self.f.btcp.node_name)    # determine node group
        btdata = self.f.btcp.cf['files'].get(n)['btdata' + group]
        try:  
          self.f.btcp.remove_torrent(self.f.btcp.tc_torrents[n].id)
        except:  
          pass
        self.f.btcp.add_torrent(n, btdata) 
        self.f.btcp.cf['dr'].insert(self.f.btcp.node_name, {n: 'groupdownloading'}) # change status to downloading
        self.f.btcp.cf['queue'].insert(n, {self.f.btcp.node_name: 'groupdownloading'}) # change status to downloading
        self.f.btcp.checkTransmission() # update torrents list
        logging.debug('checkCassandraQueues() %s torrent added to torrent client, status in cassandra changed to downloading...' %(n,))
      elif ts[n] == 'finished': # the file is marked as finished
        if n in self.f.btcp.tc_torrents:
          self.f.btcp.save_torrent_stats(n, self.f.btcp.tc_torrents[n].id) # save torrent stats to cassandra
          self.f.btcp.stop_torrent(n, self.f.btcp.tc_torrents[n].id) # remove torrent from Transmission torrent client, move to finished folder
        self.f.btcp.cf['dr'].remove(self.f.btcp.node_name, (n,))
        logging.debug('checkCassandraQueues() %s torrent removed from DR cassandra queue' %(n,))
      elif (ts[n] == 'seeding' or ts[n] == 'downloading') and n not in self.f.btcp.tc_torrents: # seeiding/downloading in cassandra, but not listed on torrent client - recreate the file
        logging.debug('checkCassandraQueues() torrent %s has status %s in cassandra, but not listed on local torrent client. Fixing.' %(n,str(ts[n]),))
        btdata = self.f.btcp.cf['files'].get(n)['btdata']
        try: 
          self.f.btcp.add_torrent(n, btdata) 
          self.f.btcp.cf['dr'].insert(self.f.btcp.node_name, {n: 'downloading'})
          self.f.btcp.tc_torrents = self.f.btcp.tc.get_torrents()
          logging.debug('checkCassandraQueues() torrent %s fixed, added to the local torrent client and changed status in cassandra to %s...' %(n,str(ts[n]),))
        except:
          logging.debug('checkCassandraQueues() attempted to fix torrent %s, failed %s' %(n,str(sys.exc_info()),))
        else:
          logging.error('checkCassandraQueues() torrent %s has status %s in cassandra, but not listed on local torrent client...' %(n,str(ts[n]),))
    logging.debug('checkCassandraQueues() finished')

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
    d = self.f.btcp.getalldata()
    logging.debug('GetAllData: type(d): %s\n' %(type(d),))
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
