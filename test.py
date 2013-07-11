from worker import Worker
from manager import Manager

def getCommand():
	return raw_input('''
	what shall we do next? choose:
		-a 			add a new worker
		-d [workerID] 		stop a worker
		-q 			quit\n''')

def handleCommand(cmd, workers):
	cmd = cmd.strip().lower()
	if(cmd == '-a'):
		w = Worker()
		workers.append(w)
		w.start()
	elif(cmd == '-q'):
		exit(0)
	else:
		workerID = int(cmd.split()[1])
		w = workers.pop(workerID)
		w.stop()


if __name__ == "__main__":
	initialData = set()
	for i in range(20):
		initialData.add(i)
	manager = Manager(initialData = initialData)
	manager.start()	

	## initially there are 2 workers in the cluster
	workers = []
	for i in range(2):
		workers.append(Worker())
	for w in workers:
		w.start()	

	while(True):
		cmd = getCommand()
		handleCommand(cmd, workers)
