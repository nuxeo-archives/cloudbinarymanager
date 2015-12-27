/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *    Wojciech Sulejman
 */
package org.nuxeo.ecm.core.storage.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.ecm.core.storage.sql.scality.DigestGenerator;
import org.nuxeo.ecm.core.storage.sql.scality.ScalityConfigurationBean;
import org.nuxeo.ecm.core.storage.sql.scality.s3.Contents;

/**
 * Tests integration with the scality system
 * By default tests are ignored for this integration unit.
 * Before the tests are run one needs to set the configuration properties in the src/test/resources/test.properties file
 * @author <a href="mailto:ws@nuxeo.com">Wojciech Sulejman</a>
 */

@Ignore
public class TestScalityBinaryManager {
    private static final Log log = LogFactory.getLog(TestScalityBinaryManager.class);

    private Properties properties;

    private String awsID;

    private String awsSecret;

    private String bucketName;

    private String cacheSize;

    private String hostBase;

    @Before
    public void setup() throws Exception {
        File propertyFile = FileUtils.getResourceFileFromContext("test.properties");
        initializePropertiesFromFile(propertyFile);
    }

    /**
     * An integration test to verify the communication with scality server Test
     * method for
     * {@link org.nuxeo.ecm.core.storage.sql.ScalityBinaryManager#downloadFile(java.lang.String)}
     * .
     */

    @Test
    public void testDownloadFile() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();

        // upload the file
        String testFileName = "testfile3.txt";
        String testFilePath = "test-files/" + testFileName;
        File testFile = FileUtils.getResourceFileFromContext(testFilePath);
        InputStream in = null;
        Binary remoteBinary = null;
        try {
            in = new FileInputStream(testFile);
            remoteBinary = sbm.getBinary(in);
        } finally {
            in.close();
        }

        // download the file
        String expectedDigest = remoteBinary.digest;
        File downloadedFile = null;
        String digest = "";
        File tmp = sbm.getFileCache().getTempFile();
        OutputStream out = new FileOutputStream(tmp);
        try {
            InputStream in2 = new FileInputStream(testFile);
            digest = sbm.storeAndDigest(in2, out);
            assertEquals(expectedDigest, digest);
        } finally {
            in.close();
            out.close();
        }
        log.debug("Downloading file:" + digest);
        downloadedFile = sbm.downloadFile(digest);
        assertTrue(downloadedFile.exists());
        assertEquals(DigestGenerator.getMD5Checksum(downloadedFile),
                expectedDigest);
    }

    /**
     * An integration test to verify the communication with scality server Test
     * method for
     * {@link org.nuxeo.ecm.core.storage.sql.ScalityBinaryManager#downloadFile(java.lang.String)}
     * .
     */

    @Test
    public void testBucketExists() throws Exception {
        String testBucketName = "nuxeo.demo.scality.com";
        ScalityConfigurationBean sconf = new ScalityConfigurationBean();
        sconf.setBucketName(testBucketName);
        ScalityBinaryManager sbm = getInitializedBinaryManager(sconf);
        boolean bucketExists = false;
        bucketExists = sbm.bucketExists(testBucketName);
        assertTrue(bucketExists);

        // test a non-existing bucket
        String fakeBucketName = "fake-bucket.demo.scality.com";
        bucketExists = true;
        bucketExists = sbm.bucketExists(fakeBucketName);
        assertFalse(bucketExists);
    }


    @Test
    public void testCreateBucket() throws Exception {
        ScalityConfigurationBean sconf = new ScalityConfigurationBean();
        ScalityBinaryManager sbm = getInitializedBinaryManager(sconf);

        if (sbm.bucketExists(sconf.getBucketName())) {
            log.info("Bucket " + sconf.getBucketName()
                    + " already exists. Test skipped.");
            return;
        }
        boolean bucketCreated = sbm.createBucket(sconf.getBucketName());
        assertTrue(bucketCreated);
    }


    @Test
    public void testGetBinary() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();

        String knownRemoteDigest = "5368976310281a3a81ae650108498726";
        String knownRemoteFileContents = "abcdef";
        Binary remoteBinary = sbm.getBinary(knownRemoteDigest);
        String fileContents = FileUtils.read(remoteBinary.getStream());
        log.debug(fileContents);
        assertTrue(fileContents.contains(knownRemoteFileContents));
    }


    @Test
    public void testObjectExists() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();

        String knownRemoteDigest = "5368976310281a3a81ae650108498726";
        boolean exists = sbm.objectExists(knownRemoteDigest);
        assertTrue(exists);
        String fakeRemoteDigest = "00000000000000000000000123456789";
        exists = sbm.objectExists(fakeRemoteDigest);
        assertFalse(exists);
    }


    @Test
    public void testGetBinaryUpload() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();

        String testFileName = "testfileUpload.txt";
        String testFilePath = "test-files/" + testFileName;
        File testFile = FileUtils.getResourceFileFromContext(testFilePath);
        InputStream in = new FileInputStream(testFile);
        Binary remoteBinary = sbm.getBinary(in);
        String remoteFileContents = FileUtils.read(remoteBinary.getStream());
        log.debug(remoteFileContents);
        String knownRemoteFileContents = "This test file";
        assertTrue(remoteFileContents.contains(knownRemoteFileContents));
    }


    @Test
    public void testDeleteBinary() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();

        String binaryToDelete = "5368976310281a3a81ae650108498726";
        // upload a file to the store
        String testFileName = "testfile3.txt";
        String testFilePath = "test-files/" + testFileName;
        File testFile = FileUtils.getResourceFileFromContext(testFilePath);
        InputStream in = new FileInputStream(testFile);
        Binary remoteBinary = sbm.getBinary(in);
        assertTrue(remoteBinary.digest.length() > 0);

        // make sure that the binary exists in the remote store
        assertTrue(sbm.objectExists(binaryToDelete));

        // remove and make sure that the binary doesn't exist in the remote
        // store anymore
        sbm.removeBinary(binaryToDelete);
        assertFalse(sbm.objectExists(binaryToDelete));
    }


    @Test
    public void testContentLength() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();

        String remoteBinary = "6d143307cbb434232bbc68b03c606da0";
        // remove and make sure that the binary doesn't exist in the remote
        // store anymore
        long contentLength = sbm.getContentLength(remoteBinary);
        log.debug("Remote file size is:" + contentLength);
        assertTrue(contentLength > 0);
    }


    @Ignore
    @Test
    public void testListObjects() throws Exception {
        ScalityBinaryManager sbm = getInitializedBinaryManager();
        String bucketName = "testing.demo.scality.com";
        List<Contents> objects = sbm.listObjects(bucketName);
        // list some properties for debugging
        for (Contents contents : objects) {
            log.debug(contents.eTag);
            log.debug(contents.owner.id);
            log.debug(contents.owner.displayName);
        }
        assertTrue(objects.size() > 0);
    }

    private ScalityBinaryManager getInitializedBinaryManager(
            ScalityConfigurationBean sconf) throws Exception {
        RepositoryDescriptor rd = new RepositoryDescriptor();
        rd.name = "MY REPO";
        if (sconf.getAwsID() == null) {
            sconf.setAwsID(awsID);
        }
        if (sconf.getAwsSecret() == null) {
            sconf.setAwsSecret(awsSecret);

        }
        if (sconf.getBucketName() == null) {
            sconf.setBucketName(bucketName);
        }
        if (sconf.getCacheSizeStr() == null) {
            sconf.setCacheSizeStr(cacheSize);
        }
        if (sconf.getHostBase() == null) {
            sconf.setHostBase(hostBase);
        }
        ScalityBinaryManager sbm = new ScalityBinaryManager(sconf);
        sbm.initialize(rd);
        return sbm;
    }

    private ScalityBinaryManager getInitializedBinaryManager() throws Exception {
        ScalityConfigurationBean sconf = new ScalityConfigurationBean();
        return getInitializedBinaryManager(sconf);
    }

    private void initializePropertiesFromFile(File propertyFile)
            throws Exception {
        properties = new Properties();
        properties.load(new FileInputStream(propertyFile));
        awsID = properties.getProperty("nuxeo.scality.awsid");
        awsSecret = properties.getProperty("nuxeo.scality.awssecret");
        bucketName = properties.getProperty("nuxeo.scality.bucket");
        cacheSize = properties.getProperty("nuxeo.scality.cache.size");
        hostBase = properties.getProperty("nuxeo.scality.host.name");
    }
}
