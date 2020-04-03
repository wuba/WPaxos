master:nohup java -jar jar.jar /opt/wpaxos/ 127.0.0.1:30000 127.0.0.1:30000,127.0.0.1:30001,127.0.0.1:30002 8 true 10 50 5 20 0
slave:nohup java -jar jar.jar /opt/wpaxos/ 127.0.0.1:30001 127.0.0.1:30000,127.0.0.1:30001,127.0.0.1:30002 8 0
slave:nohup java -jar jar.jar /opt/wpaxos/ 127.0.0.1:30002 127.0.0.1:30000,127.0.0.1:30001,127.0.0.1:30002 8 0
