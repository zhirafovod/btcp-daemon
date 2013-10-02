from singleton import Singleton
from config import Config
from connector import Connector
from data import Data
from flowcontrol import flowControl

class Daemon(Singleton):
  ''' BtCP daemon '''

  def __init__(self, *args, **kwargs):
    """
    @type self: Daemon
    
    @param args:
    @param kwargs:
    @return:
    """
    self.config = Config()
    self.data = Data(self)
    self.connector = Connector(self)
    self.connector.start()
    self.data.reload()
    self.flowcontrol = flowControl(self)