#!/usr/bin/env bash

nohup java -cp ./libs/*:./conf com.pangdata.apps.monitor.MysqlMariaDBMonitor > /dev/null 2>&1&
