#!/usr/bin/python
from btcpdaemon.client import client
from btcpdaemon.singleton import Singleton
from btcpdaemon.tools import groupName, groupByPattern

class btcp(client):
  """ Bittorrent Copy file """

  btcps = {}

  def __new__(cls, name, *args, **kwargs):
    """ implements btcp factory to instantiate only one instance for each file

    @param cls:
    @param name: btcp file name
    @type name: str
    @param args:
    @param kwargs:
    @return:
    """
    if name not in cls.btcps:
      cls.btcps[name] = super(btcp, cls).__new__(
                               cls, name, *args, **kwargs)
    return cls.btcps[name]

  def __init__(self, name):
    """
    @type name: str
    @param name: file name
    @return:
    """
    if file not in self.btcps:
      self.btcps[btcp] = btcp(name)
    return self.btcp[name]

  def __init__(self, name, f, config='/etc/btcpdaemon/sourcer.conf', standalone=None):
    """
    @type self: BtCP
    @type f: btcpdaemon.daemon.Daemon
    """
    self.standalone = standalone # store run mode
    # detect run mode
    if f:           # the object is called from tiwstd factory, factory object is supplied in 'f'
      self.f = f    # Pointer to factory object, to store persistent data
      self.log = f.config.log    # alias to logger
    else:                           # regular run or no 'f'
      #import logging
      #self.f = self # pointer to self, to store persistent data
      #self.log = logging.getLogger('btcpdaemon')
      pass





  def copy(self, files=None, dr=None):
    ''' Create bittorrent file for files, start seeding it, notify command server
          files - list of strings, files
          dr - list of strings - data receivers: btcpdaemon nodes - receivers of the files
          return status
    '''
    if not files or not dr:
      return self.copy.__doc__
    for f in files:
      tracker_url = 'http://%s:9200/ann?ls=topsecret' % (self.node_name,)
      btdata = make_torrent_file(file=f, tracker=tracker_url, comment=None)
      self.log.debug('btdata for file %s created' % (f))
      r = self.publish(f, btdata, dr)
      if not r:
        self.log.debug('file %s published' % (f,))
      else:
        self.log.debug('file %s not published published: %s' % (f, r, ))

  def files_exist(self, files):
    ''' Check if each file in files exist '''
    for f in files:
      try:
        ''' !!! Code me !!! Check that file exist and readable '''
      except:
        ''' !!! Check me !!! if an error - we need to raise an exception '''
        raise 'File is not accessible: %s', f

  def bt_create(self, files):
    ''' Create a bittorrent for files
        files - a list of strings
        return - path to bittorrent file '''
    self.file_exist(files)
    ''' !!! Code me !!! a call to bittorrent lib to create torrent file '''
    return btfile

  def update(self):
    ''' fetch files to be downloaded from cassandra '''
    ''' !!! Code me - need actual logic for that '''
    try:
      #fc = self.cf['queue'].get_range()
      pass
    except:
      self.log.debug(
        "Exception: self.cf['queue'].insert(f, {'btdata': btdata, 'status': 'new', 'source': 'localhost', 'dr': dr}): %s" % (
          sys.exc_info()[0]))
      raise

  def checkDir(self, d):
    ''' check that a dir exist and has the right permissions '''
    if not os.path.exists(d):
      os.makedirs(d)
      self.log.debug('checkDir: created dir %d' % (d, ))
    os.chmod(d, 0777)
    self.log.debug('checkDir: applied permissions to dir %d' % (d, ))

  def save_torrent_stats(self, f, i):
    ''' retrive torrent stats from transmission client and store them to cassandra '''
    try:
      uploadRatio = self.f.connector.tc.get_torrent(i).uploadRatio
      self.log.debug('save_torrent_stats: file %s with id %s uploadRatio %s' % (f, i, uploadRatio, ))
      self.f.btcp.cf['uploadRatio'].insert(f, {self.f.btcp.node_name: str(uploadRatio)}) # add uploadRatio for file f
      self.log.debug('save_torrent_stats: node %s uploadRatio was inserted for file %s' % (self.f.btcp.node_name, f, ))
    except:
      self.log.debug('save_torrent_stats: file %s an error happened: %s' % (f, sys.exc_info()[0],))

  def remove_torrent(self, i):
    ''' remove torrent file with id i from torrent client
    @type self: BtCP
    '''
    try:
      self.f.connector.tc.remove_torrent(i)
      self.log.debug('remove_torrent: torrent id %s was removed from torrent client' % (i, ))
    except:
      self.log.debug('remove_torrent: an error happened: %s' % (sys.exc_info()[0],))

  def stop_torrent(self, f, i):
    ''' remove torrent file with id i from torrent client , move file f to finished_dir '''
    try:
      self.remove_torrent(i)
      shutil.move(self.f.config.download_dir + '/' + f, self.finished_dir + '/' + f)
      self.log.debug('stop_torrent: file %s was moved to dir %s' % (f, self.finished_dir, ))
    except:
      self.log.debug('stop_torrent: file %s an error happened: %s' % (f, sys.exc_info()[0],))



  def add_torrent(self, f, btdata):
    ''' add a torrent to torrent client '''
    try:
      self.tc.add_torrent(base64.b64encode(btdata), download_dir=self.f.config.download_dir)
      self.log.debug('add_torrent: started btdata for f: %s' % (f,))
    except:
      self.log.debug('add_torrent: error: %s' % (str(sys.exc_info()),))
      raise





  #### refactored ####

  def new(self, n):
    ''' start new btcpdaemon '''
    self.name = n
    self.getBtdata()
    self.add_torrent()
    self.status('downloading')


  def group(self, n, btdata):
    ''' start group donwload '''
    group = groupName(btcp.node_name)    # determine node group
    btdata = btcp.cf['info'].get(n)['btdata' + group]
    try:
      btcp.remove_torrent(btcp.tc_torrents[n].id)
    except:
      pass
    self.add_torrent()

  def finish(self, n):
    btdata = btcp.cf['info'].get(n)['btdata']
    try:
      btcp.add_torrent(n, btdata)
      btcp.cf['dr'].insert(btcp.node_name, {n: 'downloading'})
      btcp.tc_torrents = btcp.tc.get_torrents()
    except:
      logging.error('checkCassandraQueues() attempted to fix torrent %s, failed %s' % (n, str(sys.exc_info()),))
    else:
      logging.error(
        'checkCassandraQueues() torrent %s has status %s in cassandra, but not listed on local torrent client...' % (
          n, str(ts[n]),))

  def stop(self, n):
    if n in self.tc_torrents:
      btcp.save_torrent_stats(n, btcp.tc_torrents[n].id) # save torrent stats to cassandra
      btcp.stop_torrent(n, btcp.tc_torrents[
        n].id) # remove torrent from Transmission torrent client, move to finished folder
    btcp.cf['dr'].remove(btcp.node_name, (n,))
ode_name, (n,))


if __name__ == '__main__':
  #btcpdaemon = BtCP()
  #self.log = logging.getLogger('btcpdaemon')
  pass
