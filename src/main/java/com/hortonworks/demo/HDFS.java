package com.hortonworks.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created by acesir on 8/12/15.
 */
public class HDFS {

    Configuration conf = new Configuration();
    private String hdfsUri;

    public HDFS(String hdfsUri){

        System.setProperty("HADOOP_USER_NAME", "hdfs");
        this.hdfsUri = hdfsUri;
    }

    public Configuration getHDFSConf() {

        conf.set("fs.default.name", hdfsUri);
        conf.set("hadoop.job.ugi", "hdfs");

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        return conf;
    }

    public UserGroupInformation getUgi() {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
        return ugi;
    }
}
