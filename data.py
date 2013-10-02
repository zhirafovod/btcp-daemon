import pycassa
import btcpdaemon.tools as tools
from btcpdaemon.singleton import SingletonByName

class Data(SingletonByName):
  ''' Data object '''

  def __init__(self, f, name):
    """
    @param f:
    @type f: btcp.daemon.Daemon
    @param name: name of a data class
    @type name: str
    @return:
    """

    self.f = f
    if '_torrent' not in self.__dict__:
      self.d = {}
    # aliases
    self.name = name
    self.node_name = self.f.config.node_name
    self.cfs = self.f.config.cfs

  def fetch_status(self):
    """
    get file status from cassandra queue

    @return:
    """
    self.info_status = self.cfs['info'].get(self.name)['status'] # file info
    self.queue_status = self.cfs['queue'].get(self.name)[self.node_name] # global queue
    self.dr_status = self.cfs['dr'].get(self.node_name)[self.name] # host queue

  def update_status(self, status):
    """
    set file status to 'status' 
    
    @param status: status in queues
    @type status: str
    @return:
    """
    self.status = status
    self.cfs['queue'].insert(self.name, {self.node_name: self.status}) # global queue
    self.cfs['dr'].insert(self.node_name, {self.name: self.status}) # host queue
    if self.node_name == self.ds:
      self.cfs['info'].insert(self.name, {'status': self.status}) # file info

  def fetch_info(self):
    """

    @return:
    """
    self._from_info(self._fetch_info())

  def update_info(self):
    """

    @return:
    """
    self._update_info(self._to_info())

  def _fetch_info(self):
    """
    get file info

    @return:
    """
    return self.cfs['info'].get(self.name)

  def _update_info(self, info):
    """
    update object info to cassandra

    @param info: info dict
    @type info: dict
    @return:
    """
    self.cfs['info'].insert(self.name, info)

  def _make_info(self, btdata, drs, ds, status, tier):
    """
    make file info

    @param btdata: Bittorrent data (info_hash)
    @param drs: Data Receivers, as comma separated string
    @param ds: Data Sender
    @param status: status
    @param tier: tier
    """
    self.btdata = btdata
    self.drs = drs
    self.ds = ds
    self.status = status
    self.tier = tier

  def _from_info(self, info):
    """
    store info in object

    @type info: dict
    @param info: info dictionary
    @return:
    """
    self.btdata = info['btdata']
    self.drs = info['drs']
    self.ds = info['ds']
    self.status = info['status']
    self.tier = info['tier']

  def _to_info(self):
    """
    return dict with object info

    @return: dict with the info data
    """
    return {
      'btdata': self.btdata,
      'drs': self.drs,
      'ds': self.ds,
      'status': self.status,
      'tier': self.tier
    }

  def Update(self):
    ''' fetch data from remote sources '''
    self.UpdateTransmission()
    #self.UpdateCassandra()

  def UpdateTransmission(self):
    ''' get a list of torrents from Transmission torrent client and put it to hash tc_torrents '''
    try:
      self.tc_torrents = { t.name: t for t in self.f.connector.tc.get_torrents()}
    except ValueError:   # that error happens if transmission client gets a bad data cached
      self.f.connector.restartTransmission()

  def signal(self, status):
    """
    signal object a status

    @param status:
    @type status: str
    @return:
    """
    if status == 'finish':
      self.delete_data(status)
    if not self.status:
      self.add_data(status)
    else:
      self.update_data(status)

  def delete_data(self, status):
    """
    delete data

    @param status:
    @return:
    """
    if 'torrent_id' in self.__dict__:
      try:
        self.f.connector.tc.get_torrent(self.torrent_id)
      except KeyError:
        pass
    self.update_status(status)
    self._instances.pop(self.name)

  def add_data(self, status):
    """
    add data (received new signal or

    @param status:
    @return:
    """
    self.fetch_info()
    self.fetch_status()
    self.f.connector.add_torrent(self.name, self.btdata)
    self.update_status('downloading')

  def update_data(self, status, info=None, queue=None, drs=None):
    """
    update data

    @param status:
    @type status: str
    @return:
    """
    if self.status == status:
      return
    if self.status == 'seeding':
      return self.switch_tier(status)
    self.update_status(status)