# Makefile for Spark Page Rank project.
#/Users/madhurrajput/Downloads/bigWiki
# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/opt/spark
app.name=PageRank
jar.name=PageRankSpark-0.0.1-SNAPSHOT.jar
maven.jar.name=PageRankSpark-0.0.1-SNAPSHOT.jar
job.name=TestScala
job1.name=TestScalaBase
job2.name=TestScalaAlt
local.master=local
local.input=input
local.input.aws=/home/karthik/Desktop/MR-HW3
local.output=output
local.output1=output1
local.output2=output2
local.output.aws=outputAWS
num.iter=10
k=100
# AWS EMR Execution
aws.release.label=emr-5.11.1
aws.bucket.name=kvprspark
aws.input=input
aws.output=output
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
input.aws=input
aws.subnet.id=subnet-ab4b5ad0
aws.region=us-east-2
local.output.aws=5_AWSoutput
local.logs.aws=5_AWSlogs
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} ${jar.name}

# Removes local output directory.
clean-local-output-test:
	rm -rf ${local.output}*

clean-local-output-test1:
	rm -rf ${local.output1}*

clean-local-output-test2:
	rm -rf ${local.output2}*

# Runs standalone.
local: jar clean-local-output-test
	${spark.root}/bin/spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output}

local1: jar clean-local-output-test1
	${spark.root}/bin/spark-submit --class ${job1.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output1}

local2: jar clean-local-output-test2
	${spark.root}/bin/spark-submit --class ${job2.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output2}

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir. make-bucket
upload-input-aws: 
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
upload-input-aws-big: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}
	
# Main EMR launch.
cloud: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "PageRankSparkCluster_OutofCollectionIgnored" \
		--release-label ${aws.release.label} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--master","yarn","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

cloud1: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "PageRankSparkCluster_WithoutDangling" \
		--release-label ${aws.release.label} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--master","yarn","--class","${job1.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

cloud2: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "PageRankSparkCluster_Everything_5M" \
		--release-label ${aws.release.label} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--master","yarn","--class","${job2.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Package for release.
distro:
	rm SparkPageRank.tar.gz
	rm SparkPageRank.zip
	rm -rf build
	mkdir -p build/deliv/SparkPageRank/main/scala/pagerank
	cp -r src/main/scala/pagerank/* build/deliv/SparkPageRank/main/scala/pagerank
	cp pom.xml build/deliv/SparkPageRank
	cp Makefile build/deliv/SparkPageRank
	cp README.txt build/deliv/SparkPageRank
	tar -czf SparkPageRank.tar.gz -C build/deliv SparkPageRank
	cd build/deliv && zip -rq ../../SparkPageRank.zip SparkPageRank

clean-local-output-aws:
	rm -rf ${local.output.aws}*

clean-local-logs-aws:
	rm -rf ${local.logs.aws}*
			
download-output-aws: clean-local-output-aws
	mkdir ${local.output.aws}
	aws s3 sync s3://${aws.bucket.name}/output/ ${local.output.aws}

download-logs-aws: clean-local-logs-aws
	mkdir ${local.logs.aws}
	aws s3 sync s3://${aws.bucket.name}/${aws.log.dir} ${local.logs.aws}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"
	
delete-logs-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.log.dir}*"
