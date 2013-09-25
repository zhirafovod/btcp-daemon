import pycassa
import transmissionrpc
import sys 
import os

from singleton import Singleton
from daemon import Daemon


class Connector(Singleton):
  ''' Object to manage and cache connections '''

  def __init__(self):
    ''' set required data '''
    # set aliases to access external objects
    self.f = Daemon() # reference to factory object
    self.log = self.f.config.log
    # Cassandra connections cache 
    self.cf = {} # Column Families objects
    self.pool = None # Cassandra connections pool
    # Transmission configuration
    self.tc_host = 'localhost' # transmission-daemon host 
    self.tc = None # Transmission Client
    self.download_dir = '/var/lib/transmission-daemon/downloads/' # where to download torrents to
    self.finished_dir = '/var/lib/transmission-daemon/finished/' # where to download torrents to
    
  def start(self):
    ''' connect all connectors '''
    self.restartTransmission()
    self.connectCassandra()
    self.connectTransmission()
    # connect services
    
  def connectCassandra(self):
    ''' create connection to cassandra and create column family objects '''
    if not self.pool:
      self.pool = pycassa.ConnectionPool(self.f.config.cassa_keyspace, self.f.config.cassa_nodes)
      print 'id: %s' % id(self.f)
      for k in self.f.data.cfs:
        self.cf[k] = pycassa.ColumnFamily(self.pool, k)

  def connectTransmission(self):
    ''' start transmission-daemon '''
    try:
      # need a code to check if transmission is started 
      self.tc = transmissionrpc.Client(address='localhost')
    except:
      raise Exception('cannot connect to transmission-daemon is running and allow connection')

  def restartTransmission(self):
    ''' restart transmission daemon '''
    os.system('/etc/init.d/transmission-daemon restart')
