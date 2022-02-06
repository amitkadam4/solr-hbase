#!/bin/bash

spark-submit --name "job-name" --class com.poc.solr.hbase.Solrspark --master yarn --deploy-mode cluster --conf spark.yarn.submit.waitAppCompletion=false jar_name.jar
	"{	\"tables\":[
		{
			\"tableProp\":{
				\"hofsPath\": \"/tmp/table_dir/\",
				\"outputTable\": \"output_table_name\",
				\"fieldList\": \"field_1,field_2,field_3,field_4,field_5\",
				\"fieldslistSalr\": \"field_1,field_2,field_3,field_4,field_5\",
				\"fieldSeq\": \"field_1,field_2,field_3,field_4,field_5\",
				\"sortField\":\"field_id\", 
				\"collection\:\"collection_name\",
				\"numTasks\": 18
				}
		}
	]
	}"
