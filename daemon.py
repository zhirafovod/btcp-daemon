from singleton import Singleton
from config import Config
from connector import Connector
from data import Data

class Daemon(Singleton):
  ''' BtCP daemon '''

  def __init__(self, *args, **kwargs):
    ''' init all core objects '''
    pass

  def start(self, *args, **kwargs):
    ''' init all core objects '''
    self.config = Config()
    self.data = Data()
    self.connector = Connector()
    self.connector.start = Connector()
    self.data.reload()
