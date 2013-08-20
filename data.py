import pycassa

class Data(object):
  ''' Data object '''
  
  def __init__(self, f=None):
    ''' declare shared objects '''
    self.f = f
    # configuration
    self.fc_local_name = 'localhost' # To which interface to bind local Flow Control 
    # file queues
    self.downqueue = None # files to download)
    self.upqueue = None # files to upload
    self.last_seen = None # Time when the 
    self.downloaded = {} # store list of downloaded files
    self.tc_torrents = {} # Torrents on Transmission Client
    # Cassandra's Column Families
    self.cfs = ( 
      'queue', # Queued Files, each raw key is file name, contain bittorrent data, ds list with statuses, dr list with statuses
      'processed', # Processed files, each raw key is file name, contain bittorrent data, ds list with statuses, dr list with statuses
      'hosts', # Hosts, each raw key is a host name with list of files
      'dr', # Data Receivers Cassandra column family, list of files by data receiver name
      'ds', # Data Senders Cassandra column family, list of files by data sender name
      'files', # Data Senders Cassandra column family, list of files by file name, with data receivers as columns
      'uploadRatio', # uploadRatio statistics, list of files with uploadRatio by receiver name
    )

  def reload(self):
    ''' Load files from 'hosts' row matching host name '''
    #try: # receive files to download and store to self.downqueue
    # self.downqueue = self.cf['dr'].get(self.node_name)
    #except pycassa.cassandra.ttypes.NotFoundException:
    #  self.downqueue = None
    #try: # receive files to upload and store to self.upqueue 
    #  self.upqueue = self.cf['ds'].get(self.node_name)
    #except pycassa.cassandra.ttypes.NotFoundException:
    #  self.upqueue = None

  def getData(self, keys=None, limit=None, pattern=None):
    ''' fetch data from Cassandra '''
    d = {}
    if keys == None:
      keys = self.cfs
    for k in keys:
      try:
        d[k] = self.f.connector.cf[k].get_range(row_count=limit)
      except:
        raise 
    r = {}
    if pattern:
      for k in d:
        r[k] = self.grepRange(d[k],pattern)
    else:
      r = d
    return r
    
  def grepRange(self, gr, pattern=None):
    ''' receives Cassandra get_range generator as 'gr'
        returns records where key or a column mantches to 'pattern' 
    '''
    if not pattern:
      return ()
    f = []  # result is an array
    for r in gr:
      if pattern == r[0]:
        f.append(r)
      elif pattern in [c for c in r[1]]:
        f.append(r)
    return f

  def Update(self):
    ''' fetch data from remote sources '''
    self.UpdateTransmission()
    #self.UpdateCassandra()
    
  def UpdateTransmission(self):
    ''' get a list of torrents from Transmission torrent client and put it to hash tc_torrents '''
    try: 
      self.tc_torrents = { t.name: t for t in self.f.connector.tc.get_torrents()}
    except ValueError:   # that error happens if transmission client gets a bad data cached
      self.connector.restartTransmission()

