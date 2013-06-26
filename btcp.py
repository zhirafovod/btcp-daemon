#!/usr/bin/python

from pycassa.pool import ConnectionPool
import pycassa
from PythonBittorrent.torrent import *
from PythonBittorrent.bencode import decode, encode
from urllib2 import quote
import json
import sys
import time
import re
import cgi
import logging
import pickle
import transmissionrpc
import base64
import re
from datetime import datetime
import shutil
import os
import random

class BtCP(object):
  ''' Root class for the project '''

  def __init__(self, config='/etc/btcp/sourcer.conf', f=None, standalone=None):
    ''' initialize services '''
    self.standalone = standalone # store run mode
    # detect run mode
    if f:           # the object is called from tiwstd factory, factory object is supplied in 'f'
      self.f = f    # Pointer to factory object, to store persistent data
    else:           # regular run or no 'f'
      self.f = self # pointer to self, to store persistent data
    # configuration
    self.parse_config()  # parse config file
    self.set_logging()   # set logging options
    self.config()
    self.connectCassandra()
    self.connectTransmission()
    self.dataReload() # read queues 
    self.blog.debug('BtCP.__init__() finished')

  def set_logging(self):
    ''' set logging based on self.standalone, with self.logLevel verbosity
    if standalone enabled - write to console
    otherwise - log to twisted.python module  
    '''
    level = getattr(logging,self.logLevel)  # get logging level value from logging module by 'logLevel' name (DEBUG|INFO|WARNING)
    if self.standalone:
      ch = logging.StreamHandler()
      ch.setLevel(level)
      self.blog = logging.getLogger('btcpstandalone')
      self.blog.addHandler(ch)
      self.blog.setLevel(level)
      self.blog.debug('btcp standalone loaded, __name__: %s' %(__name__,))
    else:
      from twisted.python import log
      from twisted.python.logfile import DailyLogFile
      log.startLogging(DailyLogFile.fromFullPath(self.f.log_dir + "flowcontrol.log"))
      self.blog = logging.getLogger('btcp')
      self.blog.setLevel(level)
      self.blog.debug('btcp loaded, __name__: %s' %(__name__,))

  def parse_config(self, config_name='/etc/btcp/btcp.conf'):
    ''' read values from config file 'config_name' and put them to 'self' variables '''
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read(config_name)
    self.node_name= config.get('btcp', 'hostname')    # node host name
    #self.domain = config.get('btcp', 'domain')        # node domain <- obsolete 
    self.interval = int(config.get('btcp', 'interval'))    # interval for clients to check tracker updates
    self.logLevel = config.get('btcp', 'logLevel')    # logging verbosity (DEBUG|WARNING|INFO)
    self.cassa_keyspace = config.get('btcp', 'cassa_keyspace')     # cassandra keyspace name
    self.cassa_nodes = config.get('btcp', 'cassa_nodes').replace(' ','').split(',') # comma-separated list of cassandra nodes

  def config(self):
    ''' !!! Read config here '''
    # configuration
    self.fc_local_name = 'localhost' # To which interface to bind local Flow Control 
    # file queues
    self.downqueue = None # files to download)
    self.upqueue = None # files to upload
    # Cassandra configuration 
    self.cfs = ( # Cassandra column families list
      'queue', # Queued Files, each raw key is file name, contain bittorrent data, ds list with statuses, dr list with statuses
      'processed', # Processed files, each raw key is file name, contain bittorrent data, ds list with statuses, dr list with statuses
      'hosts', # Hosts, each raw key is a host name with list of files
      'dr', # Data Receivers Cassandra column family, list of files by data receiver name
      'ds', # Data Senders Cassandra column family, list of files by data sender name
      'files', # Data Senders Cassandra column family, list of files by file name, with data receivers as columns
      'uploadRatio', # uploadRatio statistics, list of files with uploadRatio by receiver name
    )
    # Cassandra data persistance 
    self.pool = None # Cassandra connections pool
    self.cf = {} # Column Families objects
    self.last_seen = None # Time when the 
    # Transmission configuration
    self.tc_host = 'localhost' # transmission-daemon host 
    self.tc = None # Transmission Client
    self.tc_torrents = {} # Torrents on Transmission Client
    self.download_dir = '/var/lib/transmission-daemon/downloads/' # where to download torrents to
    self.finished_dir = '/var/lib/transmission-daemon/finished/' # where to download torrents to
    self.downloaded = {} # store list of downloaded files
    # Initialization
    self.blog.debug('BtCP.config() finished')

  def start(self):
    ''' Start a daemon, bittorrent tracker, bittorrent client '''
    from flowcontrol import FlowControl
    from twisted.internet import reactor
    from twisted.python import log
    from twisted.python.logfile import DailyLogFile
    log.startLogging(DailyLogFile.fromFullPath(self.f.log_dir + "btcp.log"))
    #self.tt = bttracker(self.ts_name) # !!! Code bttracker() !!!
    #self.ts = bttorrent(self.ts_name) # !!! Code btTorrent() !!!
    self.fc = FlowControl(f=self.f) # !!! Code FlowControle() !!!
    reactor.callLater(self.interval, self.fc._tick) # schedule to run next time in self.interval 
    reactor.callLater(self.interval + 10, self.fc._tack) # schedule to run next time in self.interval 
    self.blog.debug('BtCP.start: started!')
    ''' !!! Code me !!! '''

  def stop(self):
    ''' Stop running daemons, bittorrent tracker, bittorrent client '''
    ''' !!! Code me !!! '''

  def connectCassandra(self):
    ''' create connection to cassandra and create column family objects '''
    if not self.pool:
      try:
        self.pool = ConnectionPool(self.cassa_keyspace, self.cassa_nodes)
        for k in self.cfs:
          self.cf[k] = pycassa.ColumnFamily(self.pool, k)
        self.blog.debug('BtCP.connectCassandra() - conencted to cassandra. pool: %s, cf: %s' %(self.pool, self.cf, )) 
      except:
        self.blog.debug('BtCP.connectCassandra() - error connecting to cassandra. pool: %s, cf: %s, error: %s' %(self.pool, self.cf, sys.exc_info()[0], ))
    return None

  def connectTransmission(self):
    ''' start transmission-daemon '''
    try:
      # need a code to check if transmission is started 
      self.tc = transmissionrpc.Client(address='localhost')
      self.blog.debug('BtCP.connectTransmission() - connected to Transmissionbt. tc: %s' %(self.tc, ))
    except:
      self.blog.debug('BtCP.connectTransmission() - error connecting to Transmissionbt. error: %s' %(str(sys.exc_info()),))
      raise Exception('cannot connect to transmission-daemon is running and allow connection')

  def checkTransmission(self):
    ''' get a list of torrents from Transmission torrent client and put it to hash tc_torrents '''
    try: 
      self.tc_torrents = { t.name: t for t in self.tc.get_torrents()}
      self.blog.debug('BtCP.checkTransmission() - torrents and statuses: ' + ', '.join(['%s: status: %s' %(t.name, t.status, ) for t in self.tc_torrents.itervalues()]))
    except ValueError:   # that error happens if transmission client gets a bad data cached
      self.blog.error('BtCP.checkTransmission() - received bad data from transmission daemon, restarting it...')
      self.restartTransmission()

  def restartTransmission(self):
    ''' restart transmission daemon '''
    os.system('/etc/init.d/transmission-daemon restart')
    self.blog.error('BtCP.restartTransmission() - restarted Transmission daemon')

  def copy(self, files=None, dr=None):
    ''' Create bittorrent file for files, start seeding it, notify command server 
          files - list of strings, files
          dr - list of strings - data receivers: btcp nodes - receivers of the files
          return status
    '''
    if not files or not dr:
      return self.copy.__doc__ 
    for f in files:
      tracker_url = 'http://%s:9200/ann?ls=topsecret' % (self.node_name,)
      btdata = make_torrent_file(file = f, tracker = tracker_url, comment = None)
      self.blog.debug('btdata for file %s created' % (f))
      r = self.publish(f, btdata, dr)
      if not r:
        self.blog.debug('file %s published' % (f,))
      else:
        self.blog.debug('file %s not published published: %s' % (f, r, ))

  def files_exist(self, files):
    ''' Check if each file in files exist '''
    for f in files:
      try:
        ''' !!! Code me !!! Check that file exist and readable '''
      except:
        ''' !!! Check me !!! if an error - we need to raise an exception '''
        raise 'File is not accessible: %s', f

  def bt_create(self, files):
    ''' Create a bittorrent for files 
        files - a list of strings
        return - path to bittorrent file '''
    self.file_exist(files)
    ''' !!! Code me !!! a call to bittorrent lib to create torrent file '''
    return btfile

  def dataReload(self):
    ''' Load files from 'hosts' row matching host name '''
    ''' !!! Code me - need actual logic for that '''
    try: # receive files to download and store to self.downqueue
      self.downqueue = self.cf['dr'].get(self.node_name)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.downqueue = None
      self.blog.debug('dataReload() - NotFoundException in dr: %s' %(sys.exc_info()[0], )) 
    try: # receive files to upload and store to self.upqueue 
      self.upqueue = self.cf['ds'].get(self.node_name)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.upqueue = None
      self.blog.debug('dataReload() - NotFoundException in ds: %s' %(sys.exc_info()[0], )) 
    self.blog.debug('dataReload() - dr: %s, ds: %s' %(self.downqueue, self.upqueue, )) 

  def update(self):
    ''' fetch files to be downloaded from cassandra '''
    ''' !!! Code me - need actual logic for that '''
    try:
      #fc = self.cf['queue'].get_range()
      pass
    except:
      self.blog.debug("Exception: self.cf['queue'].insert(f, {'btdata': btdata, 'status': 'new', 'source': 'localhost', 'dr': dr}): %s" %(sys.exc_info()[0]))
      raise

  def checkDir(self, d):
    ''' check that a dir exist and has the right permissions '''
    if not os.path.exists(d):
      os.makedirs(d)
      self.blog.debug('checkDir: created dir %d' %(d, ))
    os.chmod(d, 0777)
    self.blog.debug('checkDir: applied permissions to dir %d' %(d, ))

  def save_torrent_stats(self, f, i):
    ''' retrive torrent stats from transmission client and store them to cassandra '''
    try: 
      uploadRatio = self.f.btcp.tc.get_torrent(i).uploadRatio
      self.blog.debug('save_torrent_stats: file %s with id %s uploadRatio %s' %(f, i, uploadRatio, ))
      self.f.btcp.cf['uploadRatio'].insert(f, {self.f.btcp.node_name : str(uploadRatio)}) # add uploadRatio for file f
      self.blog.debug('save_torrent_stats: node %s uploadRatio was inserted for file %s' %(self.f.btcp.node_name, f, ))
    except:
      self.blog.debug('save_torrent_stats: file %s an error happened: %s' %(f, sys.exc_info()[0],))

  def stop_torrent(self, f, i):
    ''' remove torrent file with id i from torrent client , move file f to finished_dir '''
    try: 
      self.f.btcp.tc.remove_torrent(i)
      self.blog.debug('stop_torrent: file %s with id %s was removed from torrent client' %(f, i, ))
      shutil.move(self.download_dir + '/' + f, self.finished_dir + '/' + f )
      self.blog.debug('stop_torrent: file %s was moved to dir %s' %(f, self.finished_dir, ))
    except:
      self.blog.debug('stop_torrent: file %s an error happened: %s' %(f, sys.exc_info()[0],))

  def start_torrent(self, f, btdata):
    ''' add a torrent to torrent client '''
    try:
      # move file to a download dir
      print '%s: %s' %(f, self.download_dir + '/' + f)
      shutil.move(f, self.download_dir + '/' + f)
      os.chmod(self.download_dir + '/' + f, 0777)
      self.tc.add_torrent(base64.b64encode(btdata), download_dir = self.download_dir)
      self.blog.debug('flowControle.start_torrent: started btdata for f: %s' %(f,))
    except:
      self.blog.error('flowControle.start_torrent: error: %s' %(sys.exc_info()[0],))
      raise

  def add_torrent(self, f, btdata):
    ''' add a torrent to torrent client '''
    try:
      self.tc.add_torrent(base64.b64encode(btdata), download_dir = self.download_dir)
      self.blog.debug('add_torrent: started btdata for f: %s' %(f,))
    except:
      self.blog.debug('add_torrent: error: %s' %(str(sys.exc_info()),))
      raise 

  def groupName(self, name, pattern='.*(\D{2})\d+$'):
    ''' match node 'name' against 'pattern' and return group name
        default is to match nodes by 2 letters identifying node's Data Center ('s(tx)123' => tx, for example)
        return string - group name
    '''
    g = 'unknown'    # 'unknown' group by default
    m = re.match(pattern, name)  
    if m.group(1):    # set group if node name matched to regex 'pattern'
      g = m.group(1)  
    return g

  def groupByPattern(self, dr, pattern='.*(\D{2})\d+$'):
    ''' group Data Receivers to list matching against 'pattern' 
        default is to match nodes by 2 letters identifying node's Data Center ('s(tx)123' => tx, for example)
        return dict of groups with list of matched nodes, for example: { 'unknown': ['testnode'], 'tx': ['stx1','stx2','stx3'], 'va': ['sva1','sva2','sva3'] } 
    '''
    drs = re.sub('\s','',dr).split(',') # remove white-spaces, split to an array by ',', map to dict with a New status
    r = {};    # dict to store result list
    for node in drs: 
      g = self.groupName(node, pattern)    # return group name
      r[g] = r.get(g, []) + [node]    # add node to group list
    return r

  def prioritizeNodes(self, d):
    ''' receives a dict 'd' with nodes grouped by a key, for example: { 'unknown': ['testnode'], 'tx': ['stx1','stx2','stx3'], 'va': ['sva1','sva2','sva3'] } 
        return a dict of nodes and priority for each node (who will download 1st, who will download 2nd), example: { 'testnode': 1, 'stx1': 2, 'stx2': 2, 'stx3': 1, ...} 
    '''
    t = {};    # a dict of nodes with priorities
    for k in d.keys():
      for n in d[k]:
        t[n] = '2'    # set everybody to the 2nd priority by default
      t[random.choice(d[k])] = 'new'    # select one random node for a group to be downloaded
    return t

  def publish(self, f, btdata, dr):
    ''' put files for dr to Flow Control server 
          f - string, file to transfer
          btdata - string, file data to transfer
          dr - string, data receivers 
    '''
    try: # check if the file already exist
      q = self.cf['files'].get(f)
      self.blog.debug('Error publishing f: %s, file already exist in the queue: %s' %(f,q,))
      return 'Error publishing f: %s, file already exist in the queue: %s' %(f,q,)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.blog.debug('checked that f: %s is not in the queue' %(f,))
      pass

    nodesGrouped = self.groupByPattern(dr)    # group nodes by pattern
    nodesPrioritized = self.prioritizeNodes(nodesGrouped)    # prioritize nodes in each group, selecting one node in each group for 1st copy turn, the rest for 2nd turn
    if len(nodesPrioritized) == 0:
      self.blog.debug('No Data Receivers recognized in string dr: %s' %(dr,))
      raise 'No Data Receivers recognized in string dr: %s' %(dr,)

    self.start_torrent(f, btdata)    # start torrent file for file named 'f' and bittorrent data 'btdata'
    self.publishData(f, btdata, dr, nodesGrouped, nodesPrioritized)    # 

  def publishData(self, f, btdata, dr, nodesGrouped, nodesPrioritized):
    ''' publish all information related to file to Cassandra '''

    p = {    # file properties
      'btdata': btdata,      # Bittorrent data 
      'status': 'new',       # status - new
      'tier': '1',           # status - new
      'drs': dr,             # string with list of all Data Receivers
      'ds': self.node_name   # Data Source
      }
    self.cf['files'].insert(f, p)    # publish file properties
    self.blog.debug('files.insert: %s, nodes: %s' %(f,nodesPrioritized,))
    
    self.cf['ds'].insert(self.node_name, {f: 'seeding'})    # put information about data sender
    self.blog.debug('ds.insert: %s, ds: %s' %(f, self.node_name, ))
   
    for r in nodesPrioritized:    # insert file to each Data Receivers queue
      if r == self.node_name:
        self.cf['dr'].insert(r, {f: 'seeding'})
        self.blog.debug('dr.insert: %s, node: %s' %(f,r,))
      elif nodesPrioritized[r] == 'new':    # starting tier 1 here
        self.cf['dr'].insert(r, {f: 'new'})
        self.blog.debug('dr.insert: %s, node: %s' %(f,r,))
    
    self.cf['queue'].insert(f, nodesPrioritized)    # add file to global queue, with dataa receivers statuses
    self.blog.debug('queue.insert: %s, nodes: %s' %(f,str(nodesPrioritized),))
    self.blog.debug('Sucessfully inserted: f: %s, drs: %s' %(f, nodesPrioritized, ))

  def getBtData(self, fn):
    try:
      btdata = self.cf['files'].get(fn)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.blog.debug("Exception: getBtData: NotFoundException: self.cf['files'].get('fn'): %s" %(sys.exc_info()[0]))
      return pycassa.cassandra.ttypes.NotFoundException
    except:
      self.blog.debug("Exception: getBtData: self.cf['files'].get('fn'): %s" %(sys.exc_info()[0]))
      raise 
    return btdata

  def getallfiles(self, f = None):
    try:
      l = self.cf['files'].get_range()
    except:
      self.blog.debug("Exception: a problem getting f: %s, %s" %(f, sys.exc_info()[0]), )
      raise 
    #for r in l:
    #  self.blog.debug( 'r: ', r)
    return l

  def getalldata(self,):
    ''' print all data '''
    d = {}
    for k in self.cfs:
      try:
        d[k] = self.cf[k].get_range()
      except:
        self.blog.debug("Exception: a problem getting all data for queue: %s, error: %s" %(f, sys.exc_info()[0]), )
        raise 
    return d

  def unpublish(self, files):
    ''' put files for dr to Flow Control server 
          files - list of strings, files to transfer '''
    ''' !!! Code me - need actual code to del files from FC server '''

  def check_uploaded(self, files):
    ''' check if all files were uploaded '''

  def saveBtdataFile(self, n=None, s=None):
    ''' fetch torrent file data for file 'n' and save as 's' '''
    # Fetch btdata
    try:
      btdata = self.cf['files'].get(n)
    except pycassa.cassandra.ttypes.NotFoundException:
      return '404: File %s Not Found' %(n,)
    self.blog.debug('btdata: type: %s, len: %s' %(type(btdata),len(btdata)))
    if not s:
      s = n + '.torrent'
    with open(s, "w") as f:
      f.write(btdata['btdata'])
    return 'Success: %s saved as %s' %(n, s, )


if __name__ == '__main__':
  #btcp = BtCP()
  #self.blog = logging.getLogger('btcp')
  pass
