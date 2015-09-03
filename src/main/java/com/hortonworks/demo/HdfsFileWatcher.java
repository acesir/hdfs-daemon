package com.hortonworks.demo;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.security.UserGroupInformation;

import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by acesir on 8/11/15.
 */
public class HdfsFileWatcher implements Runnable {

    private String hdfsDirectory;
    private String solrHost;
    private String hdfsUri;
    private String hbaseZk;

    public HdfsFileWatcher(String solrHost, String hdfsUri, String hdfsDirectory, String hbaseZk) {

        this.hdfsDirectory = hdfsDirectory;
        this.solrHost = solrHost;
        this.hdfsUri = hdfsUri;
        this.hbaseZk = hbaseZk;
    }

    public void run() {
        try {
            HdfsAdmin admin = new HdfsAdmin(new URI(hdfsUri + hdfsDirectory), new HDFS(hdfsUri).getHDFSConf());
            final DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream();
            final Utils utils = new Utils(hdfsUri);
            final Solr solr = new Solr(solrHost, hdfsUri);
            final HBase hbase = new HBase(hbaseZk);
            final TessParse tesseract = new TessParse(hdfsUri, solrHost);

            new HDFS(hdfsUri).getUgi().doAs(new PrivilegedExceptionAction<Void>() {
                public Void run() throws Exception {
                    while (true) {
                        EventBatch events = eventStream.take();
                        for (Event event : events.getEvents()) {
                            //System.out.println("event type = " + event.getEventType());
                            switch (event.getEventType()) {
                                case CREATE:
                                    Event.CreateEvent createEvent = (Event.CreateEvent) event;
                                    String newFile = createEvent.getPath().replace("._COPYING_", "");

                                    if (createEvent.getPath().contains(hdfsDirectory)) {
                                        //utils.ReadFile(createEvent.getPath().replace("._COPYING_", ""));
                                        Date date = Calendar.getInstance().getTime();
                                        String strUUID = new SimpleDateFormat("yyyyMMddhhmmssSSS").format(date);
                                        String fileExt = FilenameUtils.getExtension(newFile);

                                        solr.IndexSolrCell(newFile, strUUID);
                                        System.out.println("[" + Calendar.getInstance().getTime() + "] " +
                                                "[ID:" + strUUID + "] Indexing Doc in Solr: " + newFile);
                                        hbase.PutFile(utils.ReadHdfsFile(hdfsUri + newFile), strUUID);
                                        System.out.println("[" + Calendar.getInstance().getTime() + "] " +
                                                "[ID:" + strUUID + "] Saving raw Doc to HBase: " + newFile);

                                        if (fileExt.equals("png")) {
                                            System.out.println("[" + Calendar.getInstance().getTime() + "] " +
                                                    "[ID:" + strUUID + "] Detected Image File. Processing content with Tesseract-OCR...");
                                            solr.UpdateSolrDocument(strUUID, tesseract.Image(newFile));
                                        }
                                        System.out.println("[" + Calendar.getInstance().getTime() + "] " +
                                                "[ID:" + strUUID + "] Complete...");
                                    }
                                    break;
                                default:
                                    break;
                            }
                        }
                    }
                }
            });
        }
        catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
}
