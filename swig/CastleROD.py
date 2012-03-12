#!/usr/bin/python2.6
"""
An experimental Catle Remote Object implementation, for all your remote Castling needs.
"""
import Pyro4  #on an Acunu dev build, try easy_install-2.6 Pyro4
import socket
import castle
import logging

castle.pycastle_log.setLevel(logging.INFO)
castle = castle.Castle()

daemon=Pyro4.Daemon(host=socket.gethostbyname(socket.gethostname()),
                    port=8080)        # make a Pyro daemon
uri=daemon.register(castle, "Castle") # register the Castle object as a Pyro object

print "Ready. Object uri =", uri      # print the uri so we can use it in the client later
daemon.requestLoop()                  # start the event loop of the server to wait for calls

