#!/usr/bin/python
"""
TO DO
"""

import zmq
import os
import logging
import time
from threading import Thread, RLock

DEFAULT_REG_PORT = 13000
DEFAULT_DATA_PORT = 13001

class Manager(object):
	"""
	A manager accepts connections from worker nodes, updates nodes' info,
	collect data from connected nodes and dispatch jobs among them in a load-balanced way.

	Data Members:
	_nodes: dict{ host: data_port, push_socket, last_update_time, job_done, job_to_do }
		host: str, the name or ip of the worker node.
		data_port: int, the port on which the node uses to receive job/data from master.
		push_socket: zmq.Context.socket, a socket used to send data to node.
		job_done: set, the jobs already done by the node.
		job_to_do: Queue, the jobs to be sent to the node.
	"""
	def __init__(self, registerPort = DEFAULT_REG_PORT, dataPort = DEFAULT_DATA_PORT, initialData = None):
		"""
		Initialize the manager object.
		"""
		self._regPort = registerPort
		self._dataPort = dataPort
		self._nodes = {}
		self._nodeByID = []
		self._buffer = set() if initialData is None else initialData
		self._lock = RLock()

		## prepare logger
		if(not os.path.exists("log")):
			os.makedirs("log")
		self._logger = logging.Logger("manager")
		self._logger.addHandler(logging.FileHandler(os.path.abspath("log/manager.log")))
		self._logger.setLevel(logging.INFO)

		## initialize sockets.
		self._context = zmq.Context()
		self._regSocket = self._context.socket(zmq.REP)
		self._regSocket.bind("tcp://*:%d" % self._regPort)
		self._dataPullSocket = self._context.socket(zmq.PULL)
		self._dataPullSocket.bind("tcp://*:%d" % self._dataPort)

	def _log(self, msg):
		"""
		Log a message.
		"""
		self._logger.info("[%s] %s" % (time.ctime(), msg))

	def start(self):
		"""
		Starts the two threads: one to keep listening connection requests and one to receive data and dispatch jobs.
		"""
		connAcceptor = Thread(target = self._acceptConnections)
		connAcceptor.daemon = True
		connAcceptor.start()
		dataReceiver = Thread(target = self._recvData)
		dataReceiver.daemon = True
		dataReceiver.start()
		dataProcessor = Thread(target = self._processData)
		dataProcessor.daemon = True
		dataProcessor.start()


	def _acceptConnections(self):
		"""
		Keeps listening to _regPort. 
		On each arrival connection request, accepts and replies with the port number on which manager expects data.
		"""
		while(True):
			connectionReq = self._regSocket.recv()
			req, host, port = connectionReq.split()
			self._log("Received %s request from %s which expects data on %s" % (req, host, port))
			node = host + ":" + port
			if(req.upper() == "REG"):
				self._nodeByID.append(node)
				self._nodes[node] = {}
				dataPushSocket = self._context.socket(zmq.PUSH)
				dataPushSocket.connect("tcp://%s:%s" % (host, port))
				self._nodes[node]["socket"] = dataPushSocket
				self._nodes[node]["job_done"] = set()
				self._nodes[node]["job_to_do"] = set()
				self._log("sending port number [%d] to %s" % (self._dataPort, node))
				self._regSocket.send("REG_RESPONSE %d" % self._dataPort)
			else: ## UNREG
				self._nodeByID.remove(node)
				if(self._nodes.has_key(node)):
					self._nodes.pop(node)
				self._regSocket.send("UNREG_RESPONSE success")

	def _recvData(self):
		"""
		Keeps listening to dataPort.
		Process each arrival data report and dispatch jobs if necessary, 
		e.g. when getting enough data for dispatching a job to some node.
		"""
		while(True):
			data = self._dataPullSocket.recv_pyobj()
			self._log("received %s" % data)
			self._lock.acquire()
			self._buffer.update(data)
			self._lock.release()
			
	def _processData(self):
		"""
		Process data (a set of data) received from worker nodes.
		"""
		## TO DO: 
		## 1. nodedataSet.put(data)
		## 2. only if the nodedataSet is filled with enough data, send out to the responding worker node.
		while(True):
			if(len(self._buffer) == 0 or len(self._nodes) == 0):
				time.sleep(2)
				continue

			self._lock.acquire()
			while(len(self._buffer) > 0):
				piece = self._buffer.pop()
				targetNode = self._matchTarget(piece)
				targetSet = self._nodes[targetNode]["job_to_do"]
				targetSet.add(piece)
				if(len(targetSet) >= 5):
					targetSocket = self._nodes[targetNode]["socket"]
					targetSocket.send_pyobj(targetSet)
					self._log("sending to %s: %s" % (targetNode, targetSet))
					self._nodes[targetNode]["job_done"].update(targetSet)
					targetSet.clear()
			self._lock.release()

	def _matchTarget(self, data):
		"""
		A fake method to decide to which node the data should go.
		"""
		## 1. check if anyone is already responsible for this data.
		## 2. if nobody found in step 1, match the data to a target node by hashing.
		# nodeID = data % len(self._nodeByID)
		for node, nodeinfo in self._nodes.iteritems():
			if(data in nodeinfo["job_done"]):
				# print "----------------"
				# print "%s has done %d before, so sending to it..." %(node, data)
				# print "otherwise, it should be sent to %s" % self._nodeByID[nodeID]
				return node

		nodeID = data % len(self._nodeByID)
		return self._nodeByID[nodeID]
			
