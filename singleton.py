class Singleton(object):
  ''' application core '''
  _instance = None

  def __new__(cls, *args, **kwargs):
    ''' implement Singleton pattern '''
    if not cls._instance:
      cls._instance = super(Singleton, cls).__new__(
                               cls, *args, **kwargs)
    return cls._instance
