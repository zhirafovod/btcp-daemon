class Singleton(object):
  ''' application core '''
  _instance = None

  def __new__(cls, *args, **kwargs):
    ''' implement Singleton pattern '''
    if not cls._instance:
      cls._instance = super(Singleton, cls).__new__(
                               cls, *args, **kwargs)
    return cls._instance

class SingletonByName(object):
  ''' application core '''
  _instances = {}

  def __new__(cls, name, *args, **kwargs):
    ''' implement Singleton pattern '''
    if name not in cls._instances:
      cls._instances[name] = super(SingletonByName, cls).__new__(
                               cls, name, *args, **kwargs)
    return cls._instances[name]
