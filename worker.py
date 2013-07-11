#!/usr/bin/python
"""
TO DO
"""

import zmq
import socket
import os
import logging
import time
from threading import Thread, Event
from Queue import Queue

import random


DEFAULT_REG_PORT = 13000
DEFAULT_MANAGER = "127.0.0.1"

class Worker(object):
	"""
	TO DO
	"""
	def __init__(self, dataPort = None, manager = DEFAULT_MANAGER, regPort = DEFAULT_REG_PORT):
		"""
		TO DO
		"""
		self._manager = manager
		self._regPort = regPort
		self._dataPort = dataPort
		self._thisHost =  socket.gethostbyname(socket.gethostname())
		self._inData = Queue()
		self._outData = set()
		self._stopEvent = Event()

		## initialize sockets.
		context = zmq.Context()
		self._regSocket = context.socket(zmq.REQ)
		self._regSocket.connect("tcp://%s:%d" % (manager, self._regPort))
		self._dataPushSocket = context.socket(zmq.PUSH)

		self._dataPullSocket = context.socket(zmq.PULL)
		if(self._dataPort is None):
			self._dataPort = self._dataPullSocket.bind_to_random_port("tcp://%s" % self._thisHost)
		else:
			self._dataPullSocket.bind("tcp://*:%d" % (self._dataPort))

		## prepare logger
		if(not os.path.exists("log")):
			os.makedirs("log")
		node = self._thisHost+str(self._dataPort)
		self._logger = logging.Logger(node)
		self._logger.addHandler(logging.FileHandler(os.path.abspath("log/worker%s.log" % node)))
		self._logger.setLevel(logging.INFO)


	def _log(self, msg):
		"""
		Log a message.
		"""
		self._logger.info("[%s] %s" % (time.ctime(), msg))

	def stop(self):
		self._stopEvent.set()
		self._regSocket.send("UNREG %s %d" % (self._thisHost, self._dataPort))
		response = self._regSocket.recv()
		self._log("received %s" % response)

	def start(self):
		"""
		TO DO
		"""
		self._register()
		self._workingThread = Thread(target = self._work)
		self._workingThread.daemon = True
		self._dataAcceptor = Thread(target = self._recvData)
		self._dataAcceptor.daemon = True

		self._workingThread.start()
		self._dataAcceptor.start()

	def _register(self):
		"""
		Request connection to master. 
		"""
		self._regSocket.send("REG %s %d" % (self._thisHost, self._dataPort))
		response = self._regSocket.recv()
		managerDataPort = response.split()[1]
		self._log("success to connect to manager:%s" % managerDataPort)
		self._dataPushSocket.connect("tcp://%s:%s" % (self._manager, managerDataPort))

	def _recvData(self):
		"""
		Keeps listening to dataPort.
		Process each arrival data.
		"""
		while(not self._stopEvent.isSet()):
			data = self._dataPullSocket.recv_pyobj()
			self._log("received %s" % data)
			for piece in data:
				self._inData.put(piece)

	def _work(self):
		"""
		A fake working thread.
		"""
		while(not self._stopEvent.isSet()):
			if(self._inData.empty()):
				time.sleep(2)
			data = self._inData.get()
			self._outData.update(self._process(data)) 
			if(len(self._outData) >= 8):
				self._dataPushSocket.send_pyobj(self._outData)
				self._log("sending %s" % self._outData)
				self._outData.clear()

	def _process(self, data):
		""" 
		A fake processing method.
		"""
		num = random.randint(0, 10)
		res = set()
		for i in range(num):
			res.add(random.randint(0, 200))
		return res

			