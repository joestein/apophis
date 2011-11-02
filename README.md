Apophis
=======

A sample implementation for implementing aggregate composite metrics using counters
Getting Started
---------------

0) Assumptions

* You have SBT installed - https://github.com/harrah/xsbt/wiki/Setup
* Cassandra 0.8 or later is running locally - http://wiki.apache.org/cassandra/GettingStarted

1) Get Apophis

  git clone git@github.com:joestein/apophis.git
	cd apophis

2) Update the schema for Apophis's Specification Tests

schema/bootstrap.txt contains the schema for Apophis's Specification Tests

	~/apache-cassandra-0.8.6/bin/cassandra-cli -host localhost -port 9160 -f schema/bootstrap.txt

3) Run Apophis's test
	
	sbt test

How To Use
----------

The tests are also examples of how to use Apophis's.  Take a look at them.

Thanx =) Joe Stein

http://linkedin.com/in/charmalloc
