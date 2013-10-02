import ConfigParser
import logging 
import sys

class Config(object):
  ''' Configuration object '''

  # Cassandra's Column Families
  cfs = (
    'queue', # Queued Files, each raw key is file name, contain bittorrent data, ds list with statuses, dr list with statuses
    'processed', # Processed files, each raw key is file name, contain bittorrent data, ds list with statuses, dr list with statuses
    'hosts', # Hosts, each raw key is a host name with list of files
    'dr', # Data Receivers Cassandra column family, list of files by data receiver name
    'ds', # Data Senders Cassandra column family, list of files by data sender name
    'files', # Data Senders Cassandra column family, list of files by file name, with data receivers as columns
    'uploadRatio', # uploadRatio statistics, list of files with uploadRatio by receiver name
  )


  def __init__(self, conf_file='/etc/btcp/btcp.conf',standalone=None):
    """
    @type self: Config
    @param conf_file:
    @param standalone:
    @return:
    """
    self.standalone = standalone
    self.parse_config(conf_file)  # parse config file
    self.set_logging()

  def parse_config(self, conf_name):
    ''' read values from config file 'conf_name' and store them to parameter '''
    config = ConfigParser.ConfigParser()
    config.read(conf_name)
    self.node_name= config.get('btcp', 'hostname')    # node host name
    self.interval = int(config.get('btcp', 'interval'))    # interval for clients to check tracker updates
    self.log_dir = config.get('btcp', 'log_dir')
    self.logLevel = config.get('btcp', 'logLevel')    # logging verbosity (DEBUG|WARNING|INFO)
    self.cassa_keyspace = config.get('btcp', 'cassa_keyspace')     # cassandra keyspace name
    self.cassa_nodes = config.get('btcp', 'cassa_nodes').replace(' ','').split(',') # comma-separated list of cassandra nodes
    self.handler_port = config.getint('btcp', 'handler_port')     # port tp start http handler

  def set_logging(self):
    ''' set logging based on self.standalone, with self.logLevel verbosity
    if standalone enabled - write to console
    otherwise - log to twisted.python module
    '''
    level = getattr(logging,self.logLevel)  # get logging level value from logging module by 'logLevel' name (DEBUG|INFO|WARNING)
    if self.standalone:
      ch = logging.StreamHandler()
      ch.setLevel(level)
      self.log = logging.getLogger('btcpstandalone')
      self.log.addHandler(ch)
      self.log.setLevel(level)
      self.log.debug('btcp standalone loaded, __name__: %s' %(__name__,))
    else:
      self.log = logging.getLogger('btcp')
      self.log.setLevel(level)
    e = logging.StreamHandler(stream=sys.stderr)
    e.setLevel(logging.DEBUG)
    self.log.addHandler(e)
