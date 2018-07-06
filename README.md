# mapreduce-framework

This is a Python MapReduce Framwork that works on Hadoop (HDFS) and validates a large "big data" XML dataset. It is written for a specific product schema, but the underlying framework can be adpated to any XML schema. This framework works best with the paramiko-scp MapReduce automation script that I wrote:<br>
https://github.com/chris-relaxing/paramiko-scp<br><br>


Here is an overview of how the framework works:<br>
![alt text](http://bluegalaxy.info/images/mapreduce-framework.png)<br><br>

And some notes about the mapper and reducer:<br>
![alt text](http://bluegalaxy.info/images/mapper-slide.png)<br>
![alt text](http://bluegalaxy.info/images/reducer-slide.png)


