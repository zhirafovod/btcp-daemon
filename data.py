import pycassa

class Data(object):
  ''' Data object '''
  
  def __init__(self, f=None):
    ''' declare shared objects '''
    # configuration
    self.fc_local_name = 'localhost' # To which interface to bind local Flow Control 
    # file queues
    self.downqueue = None # files to download)
    self.upqueue = None # files to upload
    self.last_seen = None # Time when the 
    self.downloaded = {} # store list of downloaded files
    self.tc_torrents = {} # Torrents on Transmission Client

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