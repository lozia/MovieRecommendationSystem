# MovieRecommendation originally pulled from https://github.com/PhoenixMay/MovieRecommendation (private)
<Author> Jiaqi Wang, Xuliang Mei, Zongyao Li</Author>

# Before operating the recommendation system, please install the following packages: 
  <code>brew install mongodb@3.4.24</code>
  # if the above command does not work, please manually install the specific version!
  # After installing MongoDB, please install MongoDB Compass (the GUI tool for MongoDB) 1.6.2 to work with MongoDB 3.4.24
  <p><code>brew install zookeeper </code></p>
  <p><code>brew install redis</code></p>
  <p><code>brew install kafka</code></p>
  

# Please put the files in the folder /conf into desired folders as instructed below:
<p><code>cp ./conf/mongodb.conf /usr/local/mongodb/data/mongodb.conf</code></p>
<p><code>mkdir /usr/local/mongodb/data/db</code></p>
<p><code>mkdir /usr/local/mongodb/data/logs</code></p>
<p><code>touch /usr/local/mongodb/data/logs/mongodb.log</code></p>
<p><code>cp ./conf/redis.conf /usr/local/etc/redis.conf</code></p>
<p><code>cp ./conf/server.properties /usr/local/etc/kafka/server.properties</code></p>
<p><code>cp ./conf/zoo.cfg /usr/local/etc/zookeeper</code></p>

# Other dependencies are specified and included in the project pom.xml files.

# Start mongodb service
<p><code>sudo mongod -config /usr/local/mongodb/data/mongodb.conf</code></p>

# Run the DataLoader module to load data into database: simply in IDEA right click "DataLoader.scala" and select "Run".
# After loading data into database, you can run the "ContentRecommender" module, simply right click it and select "Run".
# Then run the StatisticsRecommender module and OfflineRecommender module.
# Finally, to run StreamingRecommender module, start ZooKeeper service and Kafka by typing commands in terminal:
<p><code>/usr/local/opt/redis/bin/redis-server /usr/local/etc/redis.conf</code></p>
<p><code>zkServer start</code></p>
<p><code>/usr/local/opt/kafka/bin/kafka-server-start -daemon /usr/local/etc/kafka/server.properties</code></p>
<p><code>/usr/local/opt/kafka/bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic rs</code></p>
# Then right click StreamingRecommender and select "Run".
