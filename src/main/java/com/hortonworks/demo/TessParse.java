package com.hortonworks.demo;

import net.sourceforge.tess4j.Tesseract;
import net.sourceforge.tess4j.TesseractException;
import org.apache.hadoop.security.UserGroupInformation;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.concurrent.ExecutionException;

/**
 * Created by acesir on 8/23/15.
 */
public class TessParse {
    String hdfsUri;
    public TessParse(String hdfsUri, String solrHost) {
        this.hdfsUri = hdfsUri;
    }

    public String Image(String imageFile) {
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("root");
        String pngImageDoc = "";

        Tesseract instance = Tesseract.getInstance();
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(new Utils(hdfsUri).ReadHdfsFile(imageFile));
            pngImageDoc = instance.doOCR(ImageIO.read(bais));

        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        return pngImageDoc;
    }
}
