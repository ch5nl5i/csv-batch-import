# csv-batch-import
Read CSV files and insert the lines into database tables via threads

1、执行批量插入，需在mysql url参数中设置rewriteBatchedStatements=true

2、运行时虚拟机参数设置为-Xms4096m -Xmx4096m -XX:MaxNewSize=1024m  -XX:MaxPermSize=2048m
