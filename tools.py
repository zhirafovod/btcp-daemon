import re

__author__ = 'zhirafovod'

def groupName(name, pattern='.*(\D{2})\d+$'):
  ''' match node 'name' against 'pattern' and return group name
      default is to match nodes by 2 letters identifying node's Data Center ('s(tx)123' => tx, for example)
      return string - group name
  '''
  g = 'unknown'    # 'unknown' group by default
  m = re.match(pattern, name)
  if m.group(1):    # set group if node name matched to regex 'pattern'
    g = m.group(1)
  return g


def groupByPattern(dr, pattern='.*(\D{2})\d+$'):
  ''' group Data Receivers to list matching against 'pattern'
      default is to match nodes by 2 letters identifying node's Data Center ('s(tx)123' => tx, for example)
      return dict of groups with list of matched nodes, for example: { 'unknown': ['testnode'], 'tx': ['stx1','stx2','stx3'], 'va': ['sva1','sva2','sva3'] }
  '''
  drs = re.sub('\s','',dr).split(',') # remove white-spaces, split to an array by ',', map to dict with a New status
  r = {}   # dict to store result list
  for node in drs:
    g = groupName(node, pattern)    # return group name
    r[g] = r.get(g, []) + [node]    # add node to group list
  return r

def grepRange(gr, pattern=None):
  """ receives Cassandra get_range generator as 'gr'
      returns records where key or a column mantches to 'pattern'

  @type gr: pycassa.
  """
  if not pattern:
    return ()
  f = []  # result is an array
  for r in gr:
    if pattern == r[0]:
      f.append(r)
    elif pattern in [c for c in r[1]]:
      f.append(r)
  return f

