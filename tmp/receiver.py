from btcpdaemon.connector import Connector

class Receiver(object):
  """  """

  receivers = {}

  def __new__(cls, name, *args, **kwargs):
    """ implements factory to instantiate only one instance for each file

    @param cls:
    @param name: file name
    @type name: str
    @param args:
    @param kwargs:
    @return:
    """
    if name not in cls.receivers:
      cls.receivers[name] = super(receiver, cls).__new__(
                               cls, name, *args, **kwargs)
    return cls.receivers[name]

  def __init__(self, name):
    """
    @type self: btcpdaemon.receiver.Receiver
    @return:
    """
    pass

  def files(self):
    """
    @return: dict of receiver objects
    """
    return self.receivers

  def receiver(self, name):
    """
    @type self: btcpdaemon.receiver.Receiver
    @type name: str
    @param name: receiver name
    @return:
    """
    return self.receivers


