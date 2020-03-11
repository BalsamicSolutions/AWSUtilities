# AWS tools and utilities 

This project includes some commonly used AWS tools and utilities. Including the following:

##AutoScaleLifeCycleMonitor
This is a class that monitors the local instance, if it is part of an autoscaling group it can terminate itself 
when it detects that there is no mroe mwork to do. This simplifies the implementation of autoscaling groups by only having to define a
scale up/out rule instead of having to guess at scale down/in rules. You programatically set the monitor to be active by calling AddRef,
when your task is done you call Release  

##SqsQueueDispatcher
This class is a generic SQS monitor. It dequeues items from a named queue and calls a provided hanadler. It simplifies the process of 
polling for items, extending thier life, deleting items. It supports multiple worker threads. It also includes utilities for enqueuing
messages. It is integrated into the AutoScaleLifeCycleMonitor such that you can define a scale out group, and the host system will be
automatically terminated/scalled in when the queue is quiet for a defined period of time.  

##MySqlAuthentication
This are a collection of classes that provide for IAM role based authentication to MySql and Aurora RDS instances. This allows you to assgn
database access permissions based on IAM roles or IAM users instead of individual username/password combinations. The resulting connection string
no longer needs to contain a password. It is a bit tricky to configure (see notes in the class) but once running forces a very secure envionrment

##EntityFramework
This is a collection of classes supporting EntityFramework 2.X (3.1 coming soon) that specifically target Aurora RDS. This includes a
cloud Execution Strategy for Aurora, a MySql/Aurora index attribute, a full text attribute (for enabling FULLTEXT), an UpperCase attribute for
forcing strings to upper case. There are corresponding query extensions (for FULLTEXT) and model builder extensions (for intalling attributes)
as well as a DbContext base class that handles all the implementation. 




