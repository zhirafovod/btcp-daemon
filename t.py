from btcpdaemon.singleton import SingletonByName


class a(SingletonByName):

  def __init__(self, name, t):
    if '_t' not in self.__dict__:
      self._t = {}
    self._t[t] = t
