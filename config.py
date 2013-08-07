import ConfigParser
import logging 

class Config(object):
  ''' Configuration object '''
  
  def __init__(self, conf_file, f=None, standalone=None):
    ''' init config '''
    # detect run mode
    if f:           # the object is called from tiwstd factory, factory object is supplied in 'f'
      self.f = f    # Pointer to factory object, to store persistent data
    else:           # regular run or no 'f'
      self.f = self # pointer to self, to store persistent data
    self.standalone = standalone  # standalone mode
    self.parse_config(conf_file)  # parse config file
    self.set_logging()

  def parse_config(self, conf_name):
    ''' read values from config file 'conf_name' and store them to parameter '''
    config = ConfigParser.ConfigParser()
    config.read(conf_name)
    self.node_name= config.get('btcp', 'hostname')    # node host name
    self.log_dir = config.get('btcp', 'log_dir') 
    self.interval = int(config.get('btcp', 'interval'))    # interval for clients to check tracker updates
    self.logLevel = config.get('btcp', 'logLevel')    # logging verbosity (DEBUG|WARNING|INFO)
    self.cassa_keyspace = config.get('btcp', 'cassa_keyspace')     # cassandra keyspace name
    self.cassa_nodes = config.get('btcp', 'cassa_nodes').replace(' ','').split(',') # comma-separated list of cassandra nodes
    self.handler_port = config.getint('btcp', 'handler_port')     # cassandra keyspace name

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
      from twisted.python import log
      from twisted.python.logfile import DailyLogFile
      log.startLogging(DailyLogFile.fromFullPath(self.log_dir + "/flowcontrol.log"))
      self.log = logging.getLogger('btcp')
      self.log.setLevel(level)