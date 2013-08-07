import pycassa
import transmissionrpc
import sys 


class Connector(object):
  ''' Object to manage and cache connections '''

  def __init__(self, f):
    ''' set required data '''
    # set aliases to access external objects
    self.f = f # reference to factory object
    self.log = self.f.config.log
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
    # Cassandra connections cache 
    self.cf = {} # Column Families objects
    self.pool = None # Cassandra connections pool
    # Transmission configuration
    self.tc_host = 'localhost' # transmission-daemon host 
    self.tc = None # Transmission Client
    self.download_dir = '/var/lib/transmission-daemon/downloads/' # where to download torrents to
    self.finished_dir = '/var/lib/transmission-daemon/finished/' # where to download torrents to
    
    # connect services
    self.connectCassandra()
    self.connectTransmission()
  
  def start(self):
    ''' connect all connectors '''
    
  def connectCassandra(self):
    ''' create connection to cassandra and create column family objects '''
    if not self.pool:
      try:
        self.pool = pycassa.ConnectionPool(self.cassa_keyspace, self.cassa_nodes)
        for k in self.cfs:
          self.cf[k] = pycassa.ColumnFamily(self.pool, k)
        self.log.debug('BtCP.connectCassandra() - conencted to cassandra. pool: %s, cf: %s' %(self.pool, self.cf, )) 
      except:
        self.log.debug('BtCP.connectCassandra() - error connecting to cassandra. pool: %s, cf: %s, error: %s' %(self.pool, self.cf, sys.exc_info()[0], ))
    return None

  def connectTransmission(self):
    ''' start transmission-daemon '''
    try:
      # need a code to check if transmission is started 
      self.tc = transmissionrpc.Client(address='localhost')
      self.log.debug('BtCP.connectTransmission() - connected to Transmissionbt. tc: %s' %(self.tc, ))
    except:
      self.log.debug('BtCP.connectTransmission() - error connecting to Transmissionbt. error: %s' %(str(sys.exc_info()),))
      raise Exception('cannot connect to transmission-daemon is running and allow connection')

  def checkTransmission(self):
    ''' get a list of torrents from Transmission torrent client and put it to hash tc_torrents '''
    try: 
      self.tc_torrents = { t.name: t for t in self.tc.get_torrents()}
      self.log.debug('BtCP.checkTransmission() - torrents and statuses: ' + ', '.join(['%s: status: %s' %(t.name, t.status, ) for t in self.tc_torrents.itervalues()]))
    except ValueError:   # that error happens if transmission client gets a bad data cached
      self.log.error('BtCP.checkTransmission() - received bad data from transmission daemon, restarting it...')
      self.restartTransmission()

  def restartTransmission(self):
    ''' restart transmission daemon '''
    os.system('/etc/init.d/transmission-daemon restart')
    self.log.error('BtCP.restartTransmission() - restarted Transmission daemon')
