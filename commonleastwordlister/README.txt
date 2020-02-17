I am new to mongodb but tried to learn for this test
on mongo db create
1) use wordsdb : create database
2) db.createCollection("words") : create collection
3) db.words.createIndex({word:1}) : create index on word field


Executes the program with default configurations of mongo db as local host and port 27017
java -Xmx8192m -jar CommonLeastWordLister-1.0-SNAPSHOT.jar -source /Users/nrr/Downloads/enwiki-latest-abstract.xml"

Executes the program with host and port pointing to different host and port
java -Xmx8192m -Dspring.data.mongodb.host=127.10.0.1 -Dspring.data.mongodb.port=27019 -jar CommonLeastWordLister-1.0-SNAPSHOT.jar -source "/Users/nrr/Downloads/enwiki-latest-abstract.xml"
