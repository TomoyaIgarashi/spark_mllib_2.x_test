#!/bin/bash

spark-submit \
--class "com.example.apachespark.GetRecommendation" \
--master "local[*]" \
--jars \
${X_SPARK_LIB_JAR_DIR}/mysql/mysql-connector-java/jars/mysql-connector-java-5.1.38.jar,\
${X_SPARK_LIB_JAR_DIR}/com.typesafe.slick/slick_2.11/bundles/slick_2.11-3.1.1.jar,\
${X_SPARK_LIB_JAR_DIR}/org.reactivestreams/reactive-streams/jars/reactive-streams-1.0.0.jar,\
${X_SPARK_LIB_JAR_DIR}/com.typesafe/config/bundles/config-1.3.1.jar \
target/scala-2.11/spark-mllib-2-x-examples_2.11-0.0.1-SNAPSHOT.jar ${X_SPARK_BASE_CONF_NAME}
