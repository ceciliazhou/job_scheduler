About
------------
This toy program models job scheduling in a distributed system. 

Usage
------------
Run the following command, check out the available commands and try them.

> python test.py

Check out the log files under ./log to see the data communicatoin between manager and workers.

How it works?
------------
- ***Cluster***

  A cluster is made up of one and only one manager of an arbitrary number of worker. 
  The data tranported among the workers are pure numbers.
  
- ***Manager***

  The manager of the cluster is responsible to:
  
    1. listen to connection requests from workers and response with the port on which it expects data from workers.
    2. collect data from workers, delivery resonably partioned data to different workers to achieve load-balance.
    3. be albe to handle new-joined worker and going-away worker.
    
- ***Worker***
  
  Each worker in the cluster is responsible to:
  
    1. register to manager and notify it on which port it expects data from manager.
    2. process data recieved from manager. 
       (This toy application fake processData procedure by take in data and produce another set of random data.)
    3. send data resulted by the processData procedure back to manager.


