package com.hortonworks.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by acesir on 8/14/15.
 */
public class HBase {
    private String hbaseZk;

    public HBase(String hbaseZk) {
        this.hbaseZk = hbaseZk;
    }

    public void PutFile(byte[] file, String UUID)
    {
        Configuration config = HBaseConfiguration.create();
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        config.set("hbase.zookeeper.quorum", hbaseZk);
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            HTable table = new HTable(config, "FiservImages");
            Put p = new Put(Bytes.toBytes(UUID));
            p.add(Bytes.toBytes("ImageData"), Bytes.toBytes("Image"), file);
            table.put(p);
        }
        catch (Exception ex)
        {
            System.out.println(ex.toString());
        }

    }
}
