#!/usr/bin/env bash

JMX="-Djava.rmi.server.hostname=192.168.0.21"
JMX="$JMX -Djava.net.preferIPv4Stack=true"
JMX="$JMX -Dcom.sun.management.jmxremote.port=8123"
JMX="$JMX -Dcom.sun.management.jmxremote.rmi.port=8123"
JMX="$JMX -Dcom.sun.management.jmxremote=true"
JMX="$JMX -Dcom.sun.management.jmxremote.local.only=false"
JMX="$JMX -Dcom.sun.management.jmxremote.authenticate=false"
JMX="$JMX -Dcom.sun.management.jmxremote.ssl=false"

#To enable JMX
#nohup $JMX java -cp ./libs/*:./conf io.prever.apps.monitor.MysqlMariaDBMonitor > /dev/null 2>&1&

nohup java -cp ./libs/*:./conf io.prever.apps.monitor.MysqlMariaDBMonitor > /dev/null 2>&1&
