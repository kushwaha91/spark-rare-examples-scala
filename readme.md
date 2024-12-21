# Documentation
## TOPICS

1. ### **User Defined Type Implementation**
* Use of Spark `3.2.1` for the implementation. This feature is also available in `1.6.1`.
* Feature was hidden until `3.2.1`. Still the implmentation is possible if we create the UDT class under package `com.apache.spark.custom.udts`.
* It can have many implementation based on requirements, but the most significant could be reading Circular/Recursive schemas.
* Spark could not read any such circular schemas. But with UDT implementation it is achievable.
* Some resources provided for the same purpose. 
  * `recursive_element_schema.avsc` : Schema reference for circular schema
  * `recursive_data.json` : Data generated using the recursive schema. I personally created it using random avro generator plugin.
* Used `json` data but the similar implementation can also be used for `avro` as well.
* DataFrame and Dataset implementation is covered. It's basically similar once the UDT is defined.
* Class `ElementRecord` is the class definition extends `UserDefinedType[ElementRoot]`
* RDD implementation is not included as it does not require UDT implementation. We can simply generate `Row[ElementClass]`


2. ### **Spark Execution Metrics**
* Access default sets of executions metrics
  * Number of files written
  * Number of rows written
  * Number of partitions written
  * Volume of data written in Bytes
  * Duration of executions
* Few actions might not have all the metrics produced. Depends on the type of action.
* Helper functions are wrapped around the actions to get the metrics collected in time.
* Case class `Metrics` is defined to store it.
* Sql eager evaluation and actions like write/show are handled differently.

Docs hosted under : https://kushwaha91.github.io/spark-rare-examples-scala/

###THANK YOU####
