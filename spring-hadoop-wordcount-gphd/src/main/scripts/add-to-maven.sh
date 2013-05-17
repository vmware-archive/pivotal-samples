export PIVOTAL_INSTAL_DIR=/usr/lib/gphd

mvn install:install-file -Dfile=libs/spring-data-hadoop-1.0.1.RC1.jar -DgroupId=org.springframework.data -DartifactId=spring-data-hadoop -Dversion=1.0.1.RC1 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-2.0.2_alpha_gphd_2_0_1_0/hadoop-auth-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd  -DartifactId=gphd-auth -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-2.0.2_alpha_gphd_2_0_1_0/hadoop-common-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-common -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-hdfs-2.0.2_alpha_gphd_2_0_1_0/hadoop-hdfs-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-hdfs -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-mapreduce-2.0.2_alpha_gphd_2_0_1_0/hadoop-mapreduce-client-common-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-client-common -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-mapreduce-2.0.2_alpha_gphd_2_0_1_0/hadoop-mapreduce-client-core-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-client-core -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-mapreduce-2.0.2_alpha_gphd_2_0_1_0/hadoop-mapreduce-client-jobclient-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd   -DartifactId=gphd-client-jobclient -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-mapreduce-2.0.2_alpha_gphd_2_0_1_0/hadoop-mapreduce-examples-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-examples -Dversion=2.0.1.0 -Dpackaging=jar


mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-yarn-2.0.2_alpha_gphd_2_0_1_0/hadoop-yarn-api-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-yarn-api -Dversion=2.0.1.0 -Dpackaging=jar


mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-yarn-2.0.2_alpha_gphd_2_0_1_0/hadoop-yarn-client-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-yarn-client -Dversion=2.0.1.0 -Dpackaging=jar

mvn install:install-file -Dfile=$PIVOTAL_INSTAL_DIR/hadoop-yarn-2.0.2_alpha_gphd_2_0_1_0/hadoop-yarn-common-2.0.2-alpha-gphd-2.0.1.0.jar -DgroupId=gphd     -DartifactId=gphd-yarn-common -Dversion=2.0.1.0 -Dpackaging=jar

