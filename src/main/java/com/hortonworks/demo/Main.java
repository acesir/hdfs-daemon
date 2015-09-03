package com.hortonworks.demo;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;

public class Main implements Daemon {
    private Thread hdfsFileWatcher;
    private String SOLR_HOST;
    private String HDFS_URI;
    private String HDFS_PATH;
    private String HBASE_ZK;
    private boolean stopped = false;

    public void main(String[] args) {
    }


    public void init(DaemonContext dc) throws DaemonInitException, Exception {
        System.out.println("Initializing ...");
        String args[] = dc.getArguments();

        SOLR_HOST = args[0];
        HDFS_URI = args[1];
        HDFS_PATH = args[2];
        HBASE_ZK = args[3];

        System.out.println("[SOLR_HOST] " + SOLR_HOST);
        System.out.println("[HDFS_URI] " + HDFS_URI);
        System.out.println("[HDFS_PATH] " + HDFS_PATH);
        System.out.println("[HBASE_ZK] " + HBASE_ZK);

        HdfsFileWatcher hfw = new HdfsFileWatcher(SOLR_HOST, HDFS_URI, HDFS_PATH, HBASE_ZK);

        hdfsFileWatcher = new Thread(hfw);
        hdfsFileWatcher.setDaemon(true);
        hdfsFileWatcher.start();

        while (!stopped) {
            Thread.sleep(2000);
        }
    }

    public void start() throws Exception {
        System.out.println("Starting HDFS Daemon ...");
        //main(null);
    }

    public void stop() throws Exception {
        System.out.println("Stopping HDFS Daemon ...");
        stopped = true;
        if (hdfsFileWatcher.isAlive()) {
            hdfsFileWatcher.join(2000);
        }
    }

    public void destroy() {
        System.out.println("Done.");
        hdfsFileWatcher.destroy();
    }

}

