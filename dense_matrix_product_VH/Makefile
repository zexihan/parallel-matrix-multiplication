# Makefile for Dense Matrix Product V-H in MapReduce

# Customize these paths for your environment.
# -----------------------------------------------------------
cusName=dense-matrix-product-vh-1000-1000-1000-5
cusInput=dense-input-1000
cusOutput-1=output-1-${cusName}
cusOutput-2=output-2-${cusName}
cusOutput-3=output-3-${cusName}

hadoop.root=/usr/local/hadoop
jar.name=${cusName}.jar
maven.jar.name=matrix-product-vh-1.0.jar
jar.path=target/${jar.name}
job.name=matrix.product.vh.MatrixProductVH
local.input=${cusInput}
local.output1=output-1
local.output2=output-2
local.output3=output-3

# Pseudo-Cluster Execution
hdfs.user.name=zexihan
hdfs.input=input
hdfs.output1=output-1
hdfs.output2=output-2
hdfs.output3=output-3

# AWS EMR Execution
aws.clustername=${cusName}
aws.emr.release=emr-5.17.0
aws.region=us-east-1
aws.bucket.name=bucket-matrix-product-vh
aws.subnet.id=subnet-54a1f61e
aws.input=${cusInput}
aws.output1=${cusOutput-1}
aws.output2=${cusOutput-2}
aws.output3=${cusOutput-3}
aws.log.dir=dense-log
aws.num.nodes=5
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package
	cp target/${maven.jar.name} target/${jar.name}

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output1}*
	rm -rf ${local.output2}*
	rm -rf ${local.output3}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
local: jar clean-local-output
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${local.input} ${local.output1} ${local.output2} ${local.output3}

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
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output1}*
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output2}*
	${hadoop.root}/bin/hdfs dfs -rm -r -f ${hdfs.output3}*

# Download output from HDFS to local.
download-output-hdfs: clean-local-output
	mkdir ${local.output}
	${hadoop.root}/bin/hdfs dfs -get ${hdfs.output}/* ${local.output}

# Runs pseudo-clustered (ALL). ONLY RUN THIS ONCE, THEN USE: make pseudoq
# Make sure Hadoop  is set up (in /etc/hadoop files) for pseudo-clustered operation (not standalone).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Pseudo-Distributed_Operation
pseudo: jar stop-yarn format-hdfs init-hdfs upload-input-hdfs start-yarn clean-local-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output1} ${hdfs.output2} ${hdfs.output3}
	make download-output-hdfs

# Runs pseudo-clustered (quickie).
pseudoq: jar clean-local-output clean-hdfs-output 
	${hadoop.root}/bin/hadoop jar ${jar.path} ${job.name} ${hdfs.input} ${hdfs.output1} ${hdfs.output2} ${hdfs.output3}
	make download-output-hdfs

# Create S3 bucket.
make-bucket:
	aws s3 mb s3://${aws.bucket.name}

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output1}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output2}*"
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output3}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
aws: jar upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name ${aws.clustername} \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop \
	    --steps '[{"Args":["${job.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output1}","s3://${aws.bucket.name}/${aws.output2}","s3://${aws.bucket.name}/${aws.output3}"],"Type":"CUSTOM_JAR","Jar":"s3://${aws.bucket.name}/${jar.name}","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--use-default-roles \
		--enable-debugging \
		--auto-terminate \
		--ec2-attributes SubnetId=${aws.subnet.id}

# Download output from S3.
download-output-aws:
	aws s3 sync s3://${aws.bucket.name}/${aws.output3} ${cusOutput-3}

# Change to standalone mode.
switch-standalone:
	cp config/standalone/*.xml ${hadoop.root}/etc/hadoop

# Change to pseudo-cluster mode.
switch-pseudo:
	cp config/pseudo/*.xml ${hadoop.root}/etc/hadoop

# Package for release.
distro:
	rm -f matrix-product-vh.tar.gz
	rm -f matrix-product-vh.zip
	rm -rf build
	mkdir -p build/deliv/matrix-product-vh
	cp -r src build/deliv/matrix-product-vh
	cp -r config build/deliv/matrix-product-vh
	cp -r input build/deliv/matrix-product-vh
	cp pom.xml build/deliv/matrix-product-vh
	cp Makefile build/deliv/matrix-product-vh
	cp README.txt build/deliv/matrix-product-vh
	tar -czf matrix-product-vh.tar.gz -C build/deliv matrix-product-vh
	cd build/deliv && zip -rq ../../matrix-product-vh.zip matrix-product-vh
