# distributed_computing_python_redis
design a distributed computing architecture based on python and redis

redis has replication with master and slave,in our architechture,we have one master and four slaves,each slave has a python agent,
the agent reads configurations and switcher information which we set or configiured in master node.
then the agent do the works follows the configurations and switcher impormation.
