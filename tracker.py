# pytorrent-tracker.py
# A bittorrent tracker

from logging import basicConfig, info, INFO
from pickle import dump, load
from socket import inet_aton
from struct import pack
import sys
import logging

from twisted.application import internet, service
from twisted.web.resource import Resource
from twisted.web.server import Site

from PythonBittorrent.bencode import encode

class TrackerHandler(Resource):
  ''' Bittorrent Tracker HTTP handler '''

  def __init__(self, f):
    """
    @type self: TrackerHandler
    @type f: btcp.daemon.Daemon
    @param f:
    @return:
    """
    # store pointer to factory object for persistent data
    self.f = f
    tlog.debug('Tracker().__init__, received t: %s, f.__dict__: %s' %(f,f.__dict__,))

  def render_GET(self, r):
    ''' Take a request, do some some database work, return a peer list response. '''
    tlog.debug('%s: received package: %s' %(self.__class__.__name__, r.args,))
    if not r.args:
      return 403
    # Get the necessary info out of the request
    try:
      info_hash = r.args['info_hash'][0]
      port = r.args['port'][0]
      peer_id = r.args['peer_id'][0]
    except KeyError:
      return 'info_hash, port and peer_id keys are required. You sent: %s' %(r.args,)
    compact = bool(r.args.get("compact", False))
    ip = r.getClientIP()
    # add a peer to peer_list stored in factory dict for persistance: self.f.torrent
    tlog.debug('self.__dict__: %s' %(self.__dict__))
    self.add_peer(self.f.data.tc_torrents, info_hash, peer_id, ip, port)
    tlog.debug('self.f.torrents after add_peer: self.f.torrents: %s, info_hash: %s, compact: %s, peer_id: %s, ip: %s, port: %s' %(self.f.torrents, info_hash, compact, peer_id, ip, port))
    # Generate a response
    response = {}
    response["interval"] = self.f.config.interval
    response["complete"] = 0
    response["incomplete"] = 0
    response["peers"] = self.peer_list(self.f.data.tc_torrents[info_hash], compact)
    # Log the request, and what we send back
    tlog.debug("PACKAGE: %s", r.args)
    tlog.debug("RESPONSE: %s", response)
    return '%s' %(encode(response),)

  def add_peer(self, torrents, info_hash, peer_id, ip, port):
    """ Add the peer to the torrent database. """
    tlog.debug('add_peer: torrents: %s, info_hash: %s, peer_id: %s, ip: %s, port: %s' %(torrents, info_hash, peer_id, ip, port))
    # If we've heard of this, just add the peer
    if info_hash in torrents:
      # Only add the peer if they're not already in the database
      tlog.debug('add_peer: info_hash in torrents: %s: %s: %s' %(peer_id, ip, port))
      if (peer_id, ip, port) not in torrents[info_hash]:
        torrents[info_hash].append((peer_id, ip, port))
        tlog.debug('add_peer: info_hash in torrents: torrents[info_hash].append((peer_id, ip, port)): %s, %s, %s' %(peer_id, ip, port))
    # Otherwise, add the info_hash and the peer
    else:
      torrents[info_hash] = [(peer_id, ip, port)]
      tlog.debug('add_peer: info_hash NOT in torrents: torrents[info_hash] = [(peer_id, ip, port)]: %s, %s, %s' %(peer_id, ip, port))

  def peer_list(self, peer_list, compact):
    """ Depending on compact, dispatches to compact or expanded peer
    list functions. """
    if compact:
      return self.make_compact_peer_list(peer_list)
    else:
      return self.make_peer_list(peer_list)

  def make_compact_peer_list(self, peer_list):
    """ Return a compact peer string, given a list of peer details. """
    peer_string = ""
    for peer in peer_list:
      ip = inet_aton(peer[1])
      port = pack(">H", int(peer[2]))
      peer_string += (ip + port)
    return peer_string

  def make_peer_list(self, peer_list):
    """ Return an expanded peer list suitable for the client, given
    the peer list. """
    peers = []
    for peer in peer_list:
      p = {}
      p["peer id"] = peer[0]
      p["ip"] = peer[1]
      p["port"] = int(peer[2])
      peers.append(p)
    return peers

class Command(Resource):
  ''' HTTP handler to monitor and manage cassandra queues '''

  def __init__(self, run=''):
    """
    load config, map run to method, store it to self.c

    @type self: Command
    @param run:
    @return:
    """
    self.isLeaf = True
    try:
      self.c = getattr(self, run)
      tlog.debug('__init__: method to run: %s' % (run,))
    except AttributeError:
      ''' no such method, set default '''
      self.c = getattr(self, 'renderEcho')
      tlog.debug('__init__: no such method to run: %s, set to default: %s' % (run, 'renderEcho',))

  def renderEcho(self, request):
    ''' return request as responce '''
    tlog.debug( 'renderEcho: request: %s' % (request,))
    return '%s' % (request,)

  def renderAnnounce(self, request):
    """ Take a request, do some some database work, return a peer list response. """
    tlog.debug( 'renderAnnounce: request: %s' % (request,))
    # Generate a response
    response = {}
    #response["interval"] = self.interval
    #response["complete"] = 0
    #response["incomplete"] = 0
    #response["peers"] = peer_list( \
    #self.torrents[info_hash], compact)
    return response

  def render_GET(self, request):
    ''' show form to upload files '''
    tlog.info('render_GET')
    #log.msg('log.msg')
    try:
      ''' run a method for request, based on mapping in self.c '''
      tlog.debug( 'render_Get: request: %s' % (request,))
      return self.c(request)
    except KeyError:
      ''' if we do not have a method for that request in self.c - return error'''
      tlog.debug( 'request: %s, no such method for request' % (request,))
      return 'request: %s, no such method' % (request,)

if __name__ == '__main__':
  reactor.run()
else:
  tlog = logging.getLogger('tracker')
  tlog.setLevel(logging.INFO)
