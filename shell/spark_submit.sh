spark-submit --jars $JAR_PATH
--class hackathon.dbs.problem.bigdata.driver
--master yarn
--deploy-mode cluster
--executor-memory 2G
--executor-cores 4
--num-executors 100