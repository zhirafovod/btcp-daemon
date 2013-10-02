from btcpdaemon.tools import groupByPattern


class client(object):
  """
  BtCP client
  """

  def __init__(self):
    """
    @return:
    """

  def start():
    ''' Start a daemon, bittorrent tracker, bittorrent client '''
    #from flowcontrol import FlowControl
    #from twisted.internet import reactor
    #from twisted.python import   log
    #from twisted.python.logfile import DailyLogFile
    #log.startLogging(DailyLogFile.fromFullPath(self.f.config.log_dir + "btcpdaemon.log"))
    #self.tt = bttracker(self.ts_name) # !!! Code bttracker() !!!
    #self.ts = bttorrent(self.ts_name) # !!! Code btTorrent() !!!
    #self.fc = FlowControl(f=self.f) # !!! Code FlowControle() !!!
    #reactor.callLater(self.config.interval, self.fc._tick) # schedule to run next time in self.interval
    #reactor.callLater(self.interval + 10, self.fc._tack) # schedule to run next time in self.interval
    #self.log.debug('BtCP.start: started!')
    ''' !!! Code me !!! '''

  def stop(self):
    ''' Stop running daemons, bittorrent tracker, bittorrent client '''
    ''' !!! Code me !!! '''

  def start_torrent(self, f, btdata):
    ''' add a torrent to torrent client '''
    try:
      # move file to a download dir
      print '%s: %s' % (f, self.f.config.download_dir + '/' + f)
      shutil.move(f, self.f.config.download_dir + '/' + f)
      os.chmod(self.f.config.download_dir + '/' + f, 0777)
      self.tc.add_torrent(base64.b64encode(btdata), download_dir=self.f.config.download_dir)
      self.log.debug('flowControle.start_torrent: started btdata for f: %s' % (f,))
    except:
      self.log.error('flowControle.start_torrent: error: %s' % (sys.exc_info()[0],))
      raise

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
      self.log.debug('Error publishing f: %s, file already exist in the queue: %s' % (f, q,))
      return 'Error publishing f: %s, file already exist in the queue: %s' % (f, q,)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.log.debug('checked that f: %s is not in the queue' % (f,))
      pass

    nodesGrouped = groupByPattern(dr)    # group nodes by pattern
    nodesPrioritized = self.prioritizeNodes(
      nodesGrouped)    # prioritize nodes in each group, selecting one node in each group for 1st copy turn, the rest for 2nd turn
    if len(nodesPrioritized) == 0:
      self.log.debug('No Data Receivers recognized in string dr: %s' % (dr,))
      raise 'No Data Receivers recognized in string dr: %s' % (dr,)

    self.start_torrent(f, btdata)    # start torrent file for file named 'f' and bittorrent data 'btdata'
    self.publishData(f, btdata, dr, nodesGrouped, nodesPrioritized)    #

  def publishData(self, f, btdata, dr, nodesGrouped, nodesPrioritized):
    ''' publish all information related to file to Cassandra '''

    p = {# file properties
         'btdata': btdata, # Bittorrent data
         'status': 'new', # status - new
         'tier': '1', # status - new
         'drs': dr, # string with list of all Data Receivers
         'ds': self.node_name   # Data Source
    }
    self.cf['files'].insert(f, p)    # publish file properties
    self.log.debug('files.insert: %s, nodes: %s' % (f, nodesPrioritized,))

    self.cf['ds'].insert(self.node_name, {f: 'seeding'})    # put information about data sender
    self.log.debug('ds.insert: %s, ds: %s' % (f, self.node_name, ))

    for r in nodesPrioritized:    # insert file to each Data Receivers queue
      if r == self.node_name:
        self.cf['dr'].insert(r, {f: 'seeding'})
        self.log.debug('dr.insert: %s, node: %s' % (f, r,))
      elif nodesPrioritized[r] == 'new':    # starting tier 1 here
        self.cf['dr'].insert(r, {f: 'new'})
        self.log.debug('dr.insert: %s, node: %s' % (f, r,))

    self.cf['queue'].insert(f, nodesPrioritized)    # add file to global queue, with dataa receivers statuses
    self.log.debug('queue.insert: %s, nodes: %s' % (f, str(nodesPrioritized),))
    self.log.debug('Sucessfully inserted: f: %s, drs: %s' % (f, nodesPrioritized, ))

  def getBtData(self, fn):
    try:
      btdata = self.cf['files'].get(fn)
    except pycassa.cassandra.ttypes.NotFoundException:
      self.log.debug("Exception: getBtData: NotFoundException: self.cf['files'].get('fn'): %s" % (sys.exc_info()[0]))
      return pycassa.cassandra.ttypes.NotFoundException
    except:
      self.log.debug("Exception: getBtData: self.cf['files'].get('fn'): %s" % (sys.exc_info()[0]))
      raise
    return btdata

  def unpublish(self, files):
    ''' put files for dr to Flow Control server
          files - list of strings, files to transfer '''
    ''' !!! Code me - need actual code to del files from FC server '''

  def saveBtdataFile(self, n=None, s=None):
    ''' fetch torrent file data for file 'n' and save as 's' '''
    # Fetch btdata
    try:
      btdata = self.cf['files'].get(n)
    except pycassa.cassandra.ttypes.NotFoundException:
      return '404: File %s Not Found' % (n,)
    self.log.debug('btdata: type: %s, len: %s' % (type(btdata), len(btdata)))
    if not s:
      s = n + '.torrent'
    with open(s, "w") as f:
      f.write(btdata['btdata'])
    return 'Success: %s saved as %s' % (n, s, )

