# distributed_computing_python_redis
design a distributed computing architecture based on python and redis

redis has replication with master and slave,in our architechture,we have one master and four slaves,each slave has a python agent,
the agent reads configurations and switcher information which we set or configiured in master node.
then the agent do the works follows the configurations and switcher impormation.

In our example,each agent will call the computing-function if his switcher is setting to on and his left running-times is is setting bigger than 0.
In this architecture we conquer the big computing task to several small computing tasks, and assign them to slave nodes,in order to get higher computing performance
