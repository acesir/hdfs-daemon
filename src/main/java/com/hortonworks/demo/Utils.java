package com.hortonworks.demo;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by acesir on 8/12/15.
 */
public class Utils {
    public String hdfsUri;

    public Utils(String hdfsUri) {
        this.hdfsUri = hdfsUri;

    }

    public void SolrIndex(List<String> data) {
        String urlString = "http://adis-dal05.cloud.hortonworks.com:8983/solr/hdp-core";
        SolrClient solr = new HttpSolrClient(urlString);

        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", data.get(0));
        document.addField("hdp_component", data.get(1));
        document.addField("event_timestamp", data.get(2));
        document.addField("log_type", data.get(3));
        document.addField("text", data.get(4));

        try {
            UpdateResponse response = solr.add(document);

            solr.commit();
        }
        catch (Exception ex) {
            System.out.println(ex.toString());
        }

    }

    public void RegexParse(String thread, String line) {
        Pattern p = Pattern.compile("(\\S+\\s+\\S+\\s)(\\S+\\s+)(\\S.*)");

        Matcher m = p.matcher(line);
        List<String> record = new ArrayList<String>();

        if (m.find()) {
            record.add(m.group(1).replace("-", "").replace(" ", "").replace(":", ""));
            record.add(thread);
            record.add(m.group(1).trim().replace(" ", "T") + "Z");
            record.add(m.group(2));
            record.add(m.group(3));

            for (String s: record) {
                System.out.println(s);
            }
        }

        if (record.size() > 0) {
            SolrIndex(record);
        }
    }

    public void ReadFile(String path)
    {
        try{
            HDFS hdfs = new HDFS(hdfsUri);
            FileSystem fs = FileSystem.get(hdfs.getHDFSConf());
            Path hdfsPath = new Path(path);

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
            String line;
            line = br.readLine();
            while (line != null){
                System.out.println(line);
                line=br.readLine();
            }
        }catch(Exception ex){
            System.out.println(ex.toString());
        }
    }

    public byte[] ReadHdfsFile(String path) {
        byte[] fileBytes = null;
        try{
            HDFS hdfs = new HDFS(hdfsUri);
            FileSystem fs = FileSystem.get(hdfs.getHDFSConf());
            Path hdfsPath = new Path(path);

            FSDataInputStream fsi = new FSDataInputStream(fs.open(hdfsPath));

            fileBytes = IOUtils.toByteArray(fsi);

        }catch(Exception ex){
            System.out.println(ex.toString());
        }
        finally {
            return fileBytes;
        }
    }
}
