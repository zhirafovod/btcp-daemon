

class Queue(object):
  """  """

  _queues = {}

  def __new__(cls, name, *args, **kwargs):
    """ implements factory to instantiate only one instance for each file

    @param cls:
    @param name: file name
    @type name: str
    @param args:
    @param kwargs:
    @return:
    """
    if name not in cls._queues:
      cls._queues[name] = super(queue, cls).__new__(
                               cls, name, *args, **kwargs)
    return cls._queues[name]

  def __init__(self):
    """
    @type self: btcpdaemon.queue.Queue
    @return:
    """

  def files(self):
    """
    @return: dict of queue objects
    """
    return self._queues

  def receiver(self, name):
    """
    @type self: btcpdaemon.queue.Queue
    @type name: str
    @param name: receiver name
    @return:
    """
    self


