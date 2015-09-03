package com.hortonworks.demo;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.ContentStreamBase;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by acesir on 8/12/15.
 */
public class Solr {

    private String solrHost;
    private SolrClient solrClient;
    private String hdfsUri;

    public Solr (String solrHost, String hdfsUri) {

        this.solrHost = solrHost;
        this.hdfsUri = hdfsUri;
    }

    public void IndexSolrCell(String filename, String UUID) {

        solrClient = new HttpSolrClient(solrHost);
        try {

            ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/extract");

            ContentStreamBase.ByteArrayStream fileStream = new ContentStreamBase.ByteArrayStream(new Utils(hdfsUri).ReadHdfsFile(filename), "test");
            req.setParam("literal.id", UUID);
            req.setParam("literal.resourcename", filename);
            req.addContentStream(fileStream);
            UpdateResponse process = req.process(solrClient);
            process.getStatus();
            solrClient.commit();
        }
        catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }

    public void UpdateSolrDocument(String UUID, String content) {
        solrClient = new HttpSolrClient(solrHost);
        try {
            SolrInputDocument sdoc = new SolrInputDocument();
            sdoc.addField("id", UUID);
            Map<String, Object> fieldModifier = new HashMap(1);
            fieldModifier.put("set", content);
            sdoc.addField("content", fieldModifier);

            solrClient.add(sdoc);
            solrClient.commit();
            solrClient.close();
        }
        catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
}
