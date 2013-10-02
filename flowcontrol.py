#!/usr/bin/python

import pycassa
import sys
import time
import re
import base64
from datetime import datetime
import transmissionrpc
from PythonBittorrent.torrent import Torrent
from PythonBittorrent.bencode import decode, encode
from twisted.internet import reactor
from btcpdaemon import tools
from singleton import Singleton


class flowControl(Singleton):
  ''' Flow Control '''

  def __init__(self, f):
    """
    @type f: btcpdaemon.daemon.Daemon
    @param f:
    @return:
    """
    self.f = f  # pointer to an object with external persistent data
    self.log = self.f.config.log
  
  def run(self):
    ''' run Flow Control '''
    self._control()

  #   def _tack(self,interval=60): # less frequent, fix inconsistency
  #     ''' Consistency checking - run that function each 'interval' '''
  #     reactor.callLater(self.f.config.interval, self._tack)
  #     self.cleanDownloads()        # process files marked as finished
  #     self.checkInconsistency()    # check any inconsistency
  # 
  #   def _tick(self,interval=30): # more frequent
  #     ''' Perform all FLow Control logic - run that function each 'interval' '''
  #     self.f.config.interval = interval
  #     reactor.callLater(self.f.config.interval, self._tick)
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
    self.log.debug('markDownloaded(): file %s has just been finished downloading: %s' %(n, self.f.connector.cf['dr'].get(self.f.config.node_name, )))

  def startGroupDownload(self, drs, n, group):
    ''' if group is not started - publish torrent 'n' for a group of 'drs' '''
    self.log.debug('startGroupDownload(): nodes %s, starting torrent file %s on node %s' %(str(drs), n, self.f.config.node_name,))

    btdata = self.f.connector.cf['files'].get(n)['btdata']
    torrent = decode(btdata)
    torrent['announce'] = 'http://%s:9200/ann?ls=topsecret' % (self.f.config.node_name,)
    btdata = encode(torrent)

    for x in self.f.connector.tc.get_torrents():
      if x.name == n:
        self.f.connector.tc.remove_torrent(x.id)    # no way to update tracker url, so will remove
        self.f.connector.tc.add_torrent(base64.b64encode(btdata), download_dir = self.f.config.download_dir)    # add a new torrent

    key = 'btdata' + group
    self.f.connector.cf['files'].insert(n, {key: btdata})
    self.log.debug('startGroupDownload(): published %s for file %s, announce %s' %(key, n, 'http://%s:9200/ann?ls=topsecret' % (self.f.config.node_name,),))

    for r in drs:    # insert file to each Data Receivers queue
      if r != self.f.config.node_name:
        self.f.connector.cf['dr'].insert(r, {n: 'group'})
        self.log.debug('startGroupDownload(): dr.insert: group: %s, node: %s' %(n,r,))

    self.log.debug('startGroupDownload(): Sucessfully inserted: f: %s, drs: %s' %(n, drs, ))

  def checkGroupDownloaded(self, drs, n):
    ''' check if download is not started in the group - publish torrent for a group '''
    # convert data receivers dict to string, example { 'sva1': 'seeding', 'sva2': '2', ...} => 'sva1,sva2,...'
    nodes = ','.join([x for x in self.f.connector.cf['queue'].get(n)])   
    nodesGrouped = tools.groupByPattern(nodes)
    group = tools.groupName(self.f.config.node_name)
    for dr in nodesGrouped[group]:
      if drs[dr] == '2':
        self.startGroupDownload(nodesGrouped[group], n, group)
        self.log.debug('checkGroupDownloaded(): group %s started file f: %s' %(str(nodesGrouped[group]),n,))
        return None
    return None
    self.log.debug('checkGroupDownloaded(): group %s is already downloading file %s' %(str(nodesGrouped[group]),n,))

  def checkAllDownloaded(self, n):
    ''' check if all data receivers downloaded a file 'n'
        if all finished - mark them as finished 
    '''
    try:    # fetch file queue properties
      drs = self.f.connector.cf['queue'].get(n)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.log.error('checkAllDownloaded(): file %s not in cassandra "queue"' %(n,))
      return None

    try:
      if drs[self.f.config.node_name] != 'groupdownloading' and drs[self.f.config.node_name] != 'group':
        self.checkGroupDownloaded(drs, n)    # check if a group download should be started
    except KeyError:
      self.log.debug('checkAllDownloaded(): node is not in queue for file: %s' %(n,))

    for dr in drs:    # exits if not everybody finished
      if drs[dr] != 'seeding' and dr != self.f.config.node_name:
        self.log.debug('checkAllDownloaded(): file %s not finished yet by node %s, status is %s' %(n,dr,drs[dr],))
        return None
    
    self.markAllDownloaded(drs, n)  # mark all downloaded, remove local data

  def markAllDownloaded(self, drs, n):
    ''' mark a torrent as downloaded for all nodes in cassandra '''
    self.log.debug('markAllDownloaded(): file %s is downloaded by all nodes %s' %(n,str(drs),))
    for dr in drs:
      self.f.connector.cf['dr'].insert(dr, {n : 'finished'}) # change status for the torrent to finished for each DR
      self.log.debug('markAllDownloaded(): file %s, changing status to finished for node %s in DR Cassandra queue' %(n,dr,))
    self.f.connector.cf['files'].insert(n, {'status' : 'finished'}) 
    self.log.debug('markAllDownloaded(): file %s, changed status to finished in files Cassandra queue' %(n, ))
    self.f.connector.cf['queue'].remove(n)
    self.log.debug('markAllDownloaded(): file %s, removed from queue Cassandra queue' %(n, ))
    del self.f.data.downloaded[n]
    self.log.debug('markAllDownloaded(): file %s remove from hash self.f.data.downloaded' %(n, ))

  def cleanInconsitentDownload(self, n):
    ''' check torrent's status in cassandra, remove 'finished' torrent from local client '''
    self.log.debug('cleanInconsitentDownload(): %s' %(n,))
    try:
      status = self.f.connector.cf['files'].get(n, columns=('status',)).items()[0][1]
    except pycassa.NotFoundException:
      print "Debug: %s: %s not found" %(self, n)
      return pycassa.NotFoundException
    if status == 'finished':
      try: 
        self.f.connector.tc.remove_torrent(self.f.data.tc_torrents[n].id)
        self.log.debug('cleanInconsitentDownload(): removed_torrent: %s' %(n,))
      except: 
        self.log.debug('cleanInconsitentDownload(): failed to removed_torrent: %s' %(n,))
      try: 
        self.f.connector.cf['dr'].remove(self.f.config.node_name, self.f.connector.cf['dr'].get(self.f.config.node_name, (n,) ).iteritems().next() )
        self.log.debug('cleanInconsitentDownload(): removed from dr: %s' %(n,))
      except: 
        self.log.debug('cleanInconsitentDownload(): failed to remove from dr: %s' %(n,))
      try: 
        self.f.connector.cf['queue'].remove(n, self.f.connector.cf['queue'].get(n, (self.f.config.node_name,)).iteritems().next() )
        self.log.debug('cleanInconsitentDownload(): removed from queue: %s' %(n,))
      except: 
        self.log.debug('cleanInconsitentDownload(): failed to remove from queue: %s' %(n,))
    else:
      self.log.debug('cleanInconsitentDownload(): status not finished: %s' %(n,))

  def cleanDownloads(self):
    ''' check torrents statuses in cassandra, remove torrents downloaded by all clients from cassandra '''
    self.log.debug('cleanDownloads(): checking self.f.data.tc_torrents')
    for n in self.f.data.tc_torrents:
      if n in self.f.data.downloaded:
        try: 
          status = self.f.connector.cf['files'].get(n, columns=('status',)).items()[0][1]
          if status == 'finished':
            self.f.connector.tc.remove_torrent(self.f.data.tc_torrents[n].id)
            self.log.debug('cleanDownloads(): %s is downloaded %s, file status in queue is %s, torrent is removed from client' %(n, self.f.connector.cf['files'].get(n, ), status, ))
            self.f.connector.cf['dr'].remove(self.f.config.node_name, (n,))
            drs = self.f.connector.cf['files'].get(n, columns=('drs',)).itervalues().next()
            for dr in [ x for x in re.sub('\s', '', drs).split(',') ]:
              self.f.connector.cf['dr'].remove(dr, (n,))
              self.log.debug('cleanDownloads(): removed %s from dr %s' %(n, dr, ))
            self.log.debug('cleanDownloads(): removed %s from dr queue, node %s' %(n, self.f.config.node_name, ))
          else:
            self.log.debug('cleanDownloads(): %s is downloaded %s, file status in queue is %s' %(n, self.f.connector.cf['files'].get(n, ), status, ))
        except pycassa.cassandra.ttypes.NotFoundException:
          self.log.error('cleanDownloads(): file %s is in self.f.data.downloaded, but cannot clean cassandras queues: %s' %(n, str(sys.exc_info()), ))

  def checkCassandraQueues(self):
    ''' check torrents in 'dr' cassandra column family, add new downloads '''
    try:
      ts = self.f.connector.cf['dr'].get(self.f.config.node_name) # torrents in DR queue
    except pycassa.cassandra.ttypes.NotFoundException:
      self.log.debug('checkCassandraQueues() no hostname %s in dr cassandra queue' %(self.f.config.node_name,))
      return None
    for n in ts:
      btcp = BtCP(n)
      if ts[n] == 'new': btcp.new(n, btdata)
      elif ts[n] == 'group': btcp.group(n, btdata)
      elif ts[n] == 'finished': btcp.finish(n)
      elif (ts[n] == 'seeding' or ts[n] == 'downloading') and n not in btcp.tc_torrents: btcp.fix(n)

  def reload(self):
    ''' Load info from 'hosts' row matching host name '''
    #try: # receive files to download and store to self.downqueue
    # self.downqueue = self.cf['dr'].get(self.node_name)
    #except pycassa.cassandra.ttypes.NotFoundException:
    #  self.downqueue = None
    #try: # receive files to upload and store to self.upqueue
    #  self.upqueue = self.cf['ds'].get(self.node_name)
    #except pycassa.cassandra.ttypes.NotFoundException:
    #  self.upqueue = None

if __name__ == '__main__':
  reactor.run()
