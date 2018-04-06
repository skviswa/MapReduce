# Makefile for MapReduce Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
hadoop.root=/usr/local/hadoop
jar.name=target/meantemp-0.0.1-SNAPSHOT.jar
jar.path=${jar.name}
job.name=meantemp.MeanTemp
job.name1=meantempcombiner.MeanTemp2
job.name2=meantempinmapcomb.MeanTempInMapComb
local.input=input
local.output=output
local.output1=output1
local.output2=output2
local.log=log
# Pseudo-Cluster Execution
hdfs.user.name=kv
hdfs.input=input
hdfs.output=output
# AWS EMR Execution
aws.emr.release=emr-5.11.1
aws.region=us-east-2
aws.bucket.name=avgtemphw2
aws.subnet.id=subnet-ab4b5ad0
aws.jar.name=meantemp-0.0.1-SNAPSHOT.jar
aws.input=input
aws.output=output
aws.output1=output1
aws.output2=output2
aws.log.dir=log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*
	mkdir ${local.output} 

clean-local-output1:
	rm -rf ${local.output1}* 
	mkdir ${local.output1}

clean-local-output2:
	rm -rf ${local.output2}* 
	mkdir ${local.output2}

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone:	alone3 alone1 alone2

alone3: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.input} ${local.output}

alone1: jar clean-local-output1
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name1} ${local.input} ${local.output1}

alone2: jar clean-local-output2
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name2} ${local.input} ${local.output2}
# Start HDFS
start-hdfs:
	${hadoop.root}/sbin/start-dfs.sh

# Stop HDFS
stop-hdfs: 
	${hadoop.root}/sbin/stop-dfs.sh
	
# Start YARN
start-yarn: stop-yarn
	${hadoop.root}/sbin/start-yarn.sh

# Stop YARN
stop-yarn:
	${hadoop.root}/sbin/stop-yarn.sh

# Reformats & initializes HDFS.
format-hdfs: stop-hdfs
	rm -rf /tmp/hadoop*
	${hadoop.root}/bin/hdfs namenode -format

# Initializes user & input directories of HDFS.	
init-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -rm -r -f /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}
	${hadoop.root}/bin/hdfs dfs -mkdir /user/${hdfs.user.name}/${hdfs.input}

# Load data to HDFS
upload-input-hdfs: start-hdfs
	${hadoop.root}/bin/hdfs dfs -put ${local.input}/* /user/${hdfs.user.name}/${hdfs.input}

# Removes hdfs output directory.
clean-hdfs-output:
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output}*

# Download output from HDFS to local.
download-output:
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output}
	make download-output

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir. make-bucket
upload-input-aws: 
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

delete-output-aws1:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output1}*"

delete-output-aws2:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output2}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "NoCombiner Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${aws.jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

cloud1: jar delete-output-aws1
	aws emr create-cluster \
		--name "Combiner Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name1}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output1}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${aws.jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate


cloud2: jar delete-output-aws2
	aws emr create-cluster \
		--name "InMapperCombiner Cluster" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name2}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output2}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${aws.jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate

# Download output from S3.
download-output-aws: clean-local-output
	mkdir ${local.output} ${local.output1} ${local.output2} ${local.log} 
	aws s3 sync s3://${aws.bucket.name}/${aws.output} ${local.output}
	aws s3 sync s3://${aws.bucket.name}/${aws.output1} ${local.output1}
	aws s3 sync s3://${aws.bucket.name}/${aws.output2} ${local.output2}
	aws s3 sync s3://${aws.bucket.name}/${aws.log} ${local.log}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -rf build
	mkdir build
	mkdir build/deliv
	mkdir build/deliv/WordCount
	cp pom.xml build/deliv/WordCount
	cp -r src build/deliv/WordCount
	cp Makefile build/deliv/WordCount
	cp README.txt build/deliv/WordCount
	tar -czf WordCount.tar.gz -C build/deliv WordCount
	cd build/deliv && zip -rq ../../WordCount.zip WordCount
	
