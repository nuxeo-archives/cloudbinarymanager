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
 */
package org.nuxeo.ecm.core.storage.sql;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SignatureException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.FileRequestEntity;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.HeadMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.file.FileCache;
import org.nuxeo.common.file.LRUFileCache;
import org.nuxeo.ecm.core.storage.sql.scality.DigestGenerator;
import org.nuxeo.ecm.core.storage.sql.scality.HTTPMethod;
import org.nuxeo.ecm.core.storage.sql.scality.ScalityConfigurationBean;
import org.nuxeo.ecm.core.storage.sql.scality.StringGenerator;
import org.nuxeo.ecm.core.storage.sql.scality.s3.Contents;
import org.nuxeo.ecm.core.storage.sql.scality.s3.ListBucketResult;
import org.nuxeo.runtime.api.Framework;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.amazonaws.AmazonClientException;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

/**
 * An implementation of Nuxeo's Binary Manager storing binaries as Amazon S3
 * BLOBs in the Scality system
 *
 * @author <a href="mailto:ws@nuxeo.com">Wojciech Sulejman</a>
 */
public class ScalityBinaryManager extends CloudBinaryManager {
    private static final Log log = LogFactory.getLog(ScalityBinaryManager.class);

    protected String bucketName;

    protected String awsID;

    protected String awsSecret;

    protected String hostBase;

    protected String cacheSizeStr;

    public static final String AWS_ID_KEY = "nuxeo.scality.awsid";
    public static final String AWS_SECRET_KEY = "nuxeo.scality.awssecret";
    public static final String BUCKET_REGION_KEY = "nuxeo.scality.region";
    public static final String BUCKET_NAME_KEY = "nuxeo.scality.bucket";
    public static final String CACHE_SIZE = "nuxeo.scality.cache.size";
    public static final String SCALITY_HOST_NAME = "nuxeo.scality.host.name";

    public static final String PROTOCOL_PREFIX = "http://";
    public static final String DEFAULT_CONTENT_TYPE = "";

    /**
     * Sets configuration properties from the Framework-exposed properties
     */
    public ScalityBinaryManager() {
        this.bucketName = Framework.getProperty(BUCKET_NAME_KEY);
        this.awsID = Framework.getProperty(AWS_ID_KEY);
        this.awsSecret = Framework.getProperty(AWS_SECRET_KEY);
        this.cacheSizeStr = Framework.getProperty(CACHE_SIZE);
        this.hostBase = Framework.getProperty(SCALITY_HOST_NAME);
    }

    /**
     * Sets configuration properties a configuration file
     */

    public ScalityBinaryManager(ScalityConfigurationBean conf) {
        this.awsID = conf.getAwsID();
        this.awsSecret = conf.getAwsSecret();
        this.bucketName = conf.getBucketName();
        this.cacheSizeStr = conf.getCacheSizeStr();
        this.hostBase = conf.getHostBase();
    }

    @Override
    public void initialize(RepositoryDescriptor repositoryDescriptor)
            throws IOException {
        repositoryName = repositoryDescriptor.name;
        descriptor = new BinaryManagerDescriptor();
        descriptor.digest = getDigest();
        log.info("Repository '" + repositoryDescriptor.name + "' using "
                + this.getClass().getSimpleName());

        // Create file cache
        File dir = File.createTempFile("nxbincache.", "", null);
        boolean dirDeleted = dir.delete();
        if (dirDeleted) {
            log.debug("Deleted directory: " + dir);
        } else {
            log.debug("Problem deleting: " + dir);
        }
        boolean dirCreated = dir.mkdir();
        if (dirCreated) {
            log.debug("Created directory: " + dir);
        } else {
            log.debug("Problem creating: " + dir);
        }
        dir.deleteOnExit();
        long cacheSize = StringGenerator.parseSizeInBytes(cacheSizeStr);
        fileCache = new LRUFileCache(dir, cacheSize);
        log.info("Using binary cache directory: " + dir.getPath() + " size: "
                + cacheSizeStr);

        // create a bucket if it doesn't exist
        if (!bucketExists(this.bucketName)) {
            createBucket(this.bucketName);
        }

        // TODO not implemented yet
        // createGarbageCollector();
    }

    /**
     * Gets the message digest to use to hash binaries.
     */
    protected String getDigest() {
        return DEFAULT_DIGEST;
    }

    @Override
    public Binary getBinary(InputStream in) throws IOException {
        // Write the input stream to a temporary file, while computing a digest
        File tmp = fileCache.getTempFile();
        OutputStream out = new FileOutputStream(tmp);
        String digest;
        try {
            digest = storeAndDigest(in, out);
        } finally {
            in.close();
            out.close();
        }
        // Register the file in the file cache
        File file = fileCache.putFile(digest, tmp);

        // check if the object exists in the remote server
        boolean objectExists = objectExists(digest);

        // upload the object if not present on the remote server
        if (!objectExists) {
            String remoteStorageID = uploadFile(file);
            log.debug("File " + file.getName() + " was stored as "
                    + remoteStorageID);
        }
        return new Binary(file, digest, repositoryName);
    }

    @Override
    public Binary getBinary(String digest) {
        // Check in the cache
        File file = fileCache.getFile(digest);
        if (file == null) {
            // Fetch from Scality and store it in the cache
            try {
                file = downloadFile(digest);
                file = fileCache.putFile(digest, file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return new Binary(file, digest, repositoryName);
    }

    /**
     * Downloads a file from the scality system The file to be downloaded is
     * identified by its id (a digest id is used)
     *
     * @param remoteFileID
     * @return
     */
    public File downloadFile(String remoteFileID) {
        String remoteFileName = remoteFileID;

        String url = PROTOCOL_PREFIX + this.bucketName + "." + this.hostBase;
        log.debug(url);
        GetMethod getMethod = new GetMethod(url);
        String contentMD5 = "";
        String contentType = "";
        String stringToSign = StringGenerator.getStringToSign(HTTPMethod.GET,
                contentMD5, contentType, bucketName, remoteFileName, new Date());
        File tmp = null;
        try {
            getMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));
            getMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());

            getMethod.setPath("/" + remoteFileID);// needs to be properly
            // encoded

            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(getMethod);
            log.debug(getMethod.getResponseBodyAsString());
            if (returnCode != -1) {

                BufferedInputStream bis = new BufferedInputStream(
                        getMethod.getResponseBodyAsStream());
                tmp = fileCache.getTempFile();
                FileOutputStream fos = new FileOutputStream(tmp);
                byte[] bytes = new byte[4];
                int count = bis.read(bytes);
                while (count != -1 && count <= 4) {
                    fos.write(bytes, 0, count);
                    count = bis.read(bytes);
                }
                if (count != -1) {
                    fos.write(bytes, 0, count);
                }
                fos.close();
                bis.close();
            }
            getMethod.releaseConnection();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return tmp;
    }

    /**
     * Upload a file to the Scality system.
     *
     * @param file
     * @return The file identifier (file digest) under which a file will be
     *         stored in the scality system
     */
    protected String uploadFile(File file) {
        String url = PROTOCOL_PREFIX + this.bucketName + "." + this.hostBase;
        log.debug(url);
        PutMethod putMethod = new PutMethod(url);
        try {
            String contentMD5 = DigestGenerator.getMD5Checksum(file);
            String stringToSign = StringGenerator.getStringToSign(
                    HTTPMethod.PUT, "", DEFAULT_CONTENT_TYPE, bucketName,
                    contentMD5, new Date());
            String authorizationString = StringGenerator.getAuthorizationString(
                    stringToSign, awsID, awsSecret);
            putMethod.addRequestHeader("Authorization", authorizationString);
            putMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());
            putMethod.setPath("/" + contentMD5);
            putMethod.setRequestEntity(new FileRequestEntity(file,
                    DEFAULT_CONTENT_TYPE));
            log.debug(">>>" + authorizationString + "<<<");
            log.debug(">>" + stringToSign + "<<");
            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(putMethod);
            if (returnCode != HttpStatus.SC_OK) {
                String failedUploadMsg = "File upload failed for "
                        + file.getName() + " Error:" + returnCode;
                log.debug(failedUploadMsg);
                throw new RuntimeException(failedUploadMsg);
            }
            log.debug(returnCode + putMethod.getResponseBodyAsString());
            putMethod.releaseConnection();
            log.debug(returnCode + putMethod.getResponseBodyAsString());
            return contentMD5;
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies if a specific named bucket exists.
     *
     * @param objectID
     */
    public boolean objectExists(String objectID) {
        boolean objectExists = false;
        String url = PROTOCOL_PREFIX + this.bucketName + "." + this.hostBase;
        log.debug(url);
        HeadMethod headMethod = new HeadMethod(url);
        String contentMD5 = "";
        String stringToSign = StringGenerator.getStringToSign(HTTPMethod.HEAD,
                contentMD5, DEFAULT_CONTENT_TYPE, this.bucketName, objectID,
                new Date());
        try {
            headMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));
            headMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());
            headMethod.setPath("/" + objectID);
            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(headMethod);
            log.debug(headMethod.getResponseBodyAsString());
            // only for logging
            if (returnCode == HttpStatus.SC_OK) {
                objectExists = true;
            } else if (returnCode == HttpStatus.SC_NOT_FOUND) {
                objectExists = false;
                log.debug("Object " + objectID + " does not exist");
            } else {
                String connectionMsg = "Scality connection problem. Object could not be verified";
                log.debug(connectionMsg);
                throw new RuntimeException(connectionMsg);
            }
            headMethod.releaseConnection();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return objectExists;
    }

    /**
     * Lists ids of objects in the provided bucket
     *
     * @param objectID
     */
    public List<Contents> listObjects(String bucketName) {
        List<Contents> objects = null;
        String url = PROTOCOL_PREFIX + bucketName + "." + this.hostBase;
        log.debug(url);
        GetMethod getMethod = new GetMethod(url);
        String contentMD5 = "";
        String fileName = "";
        String stringToSign = StringGenerator.getStringToSign(HTTPMethod.GET,
                contentMD5, DEFAULT_CONTENT_TYPE, this.bucketName, fileName,
                new Date());
        try {
            getMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));
            getMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());
            getMethod.setPath("/");
            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(getMethod);
            String xmlResponse = getMethod.getResponseBodyAsString();
            log.debug("RETURN CODE:\t" + returnCode + "\tRESPONSE:\t"
                    + xmlResponse);

            // extract content objects
            XStream xstream = new XStream(new StaxDriver());
            xstream.processAnnotations(ListBucketResult.class);
            xstream.processAnnotations(Contents.class);
            ListBucketResult bucketResult = (ListBucketResult) xstream.fromXML(xmlResponse);
            objects = bucketResult.contents;
            getMethod.releaseConnection();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return objects;
    }

    /**
     * Lists MD5 digests.
     */
    private Set<String> listTags(String bucketName) {
        Set<String> digests = new HashSet<String>();
        for (Contents contents : this.listObjects(bucketName)) {
            digests.add(contents.eTag);
        }
        return digests;
    }

    /**
     * Retrieves the content-length of the remote object
     *
     * @param objectID
     */
    public long getContentLength(String objectID) {
        String url = PROTOCOL_PREFIX + this.bucketName + "." + this.hostBase;
        log.debug(url);
        HeadMethod headMethod = new HeadMethod(url);
        String contentMD5 = "";
        String stringToSign = StringGenerator.getStringToSign(HTTPMethod.HEAD,
                contentMD5, DEFAULT_CONTENT_TYPE, this.bucketName, objectID,
                new Date());
        long contentLength = 0;
        try {
            headMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));
            headMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());
            headMethod.setPath("/" + objectID);
            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(headMethod);
            // specific header
            if (returnCode == HttpStatus.SC_OK) {
                Header contentLengthHeader = headMethod.getResponseHeader("Content-Length");
                contentLength = Long.parseLong(contentLengthHeader.getValue());

            } else if (returnCode == HttpStatus.SC_NOT_FOUND) {
                log.debug("Object " + objectID + " does not exist");
            } else {
                String connectionMsg = "Scality connection problem. Object could not be verified";
                log.debug(connectionMsg);
                throw new RuntimeException(connectionMsg);
            }
            headMethod.releaseConnection();
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return contentLength;
    }

    /**
     * Deletes an object using its digest string.
     *
     * @param objectID
     */
    protected void removeBinary(String objectID) {
        String url = PROTOCOL_PREFIX + this.bucketName + "." + this.hostBase;
        log.debug(url);
        DeleteMethod deleteMethod = new DeleteMethod(url);
        String contentMD5 = "";

        // date to be provided to the cloud server
        Date currentDate = new Date();

        String cloudDateString = StringGenerator.getCloudFormattedDateString(currentDate);

        String stringToSign = StringGenerator.getStringToSign(
                HTTPMethod.DELETE, contentMD5, DEFAULT_CONTENT_TYPE,
                this.bucketName, objectID, currentDate);
        try {
            deleteMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));
            deleteMethod.addRequestHeader("x-amz-date", cloudDateString);
            deleteMethod.setPath("/" + objectID);

            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(deleteMethod);
            log.debug(deleteMethod.getResponseBodyAsString());
            // only for logging
            if (returnCode == HttpStatus.SC_NO_CONTENT) {
                log.info("Object " + objectID + " deleted");
            } else if (returnCode == HttpStatus.SC_NOT_FOUND) {
                log.debug("Object " + objectID + " does not exist");
            } else {
                String connectionMsg = "Scality connection problem. Object could not be verified";
                log.debug(connectionMsg);
                throw new RuntimeException(connectionMsg);
            }
            deleteMethod.releaseConnection();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Verifies if a specific named bucket exists.
     *
     * @param bucketName
     */
    public boolean bucketExists(String bucketName) {
        boolean bucketExists = false;
        String remoteFileName = "";// no file needed to check the bucket
        String url = PROTOCOL_PREFIX + bucketName + "." + this.hostBase;
        log.debug(url);
        GetMethod getMethod = new GetMethod(url);
        String contentMD5 = "";
        String stringToSign = StringGenerator.getStringToSign(HTTPMethod.GET,
                contentMD5, DEFAULT_CONTENT_TYPE, bucketName, remoteFileName,
                new Date());
        try {
            getMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));
            getMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());

            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(getMethod);

            if (returnCode == HttpStatus.SC_OK) {
                bucketExists = true;
            } else if (returnCode == HttpStatus.SC_NOT_FOUND) {
                log.debug(getMethod.getResponseBodyAsString());
                log.debug("Bucket " + bucketName + " does not exist");
            } else {
                log.debug(getMethod.getResponseBodyAsString());
                log.debug("Connection problem");
            }
            getMethod.releaseConnection();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bucketExists;
    }

    /**
     * Creates a new bucket in the scality system.
     *
     * @param bucketName
     * @return true if a new bucket was created
     * @throws ScalityBucketExistsException
     */
    public boolean createBucket(String bucketName) {
        boolean bucketExists = false;
        String remoteFileName = "";// no file needed to create the bucket
        String url = PROTOCOL_PREFIX + bucketName + "." + this.hostBase;
        log.debug(url);
        PutMethod putMethod = new PutMethod(url);
        String contentMD5 = "";
        String stringToSign = StringGenerator.getStringToSign(HTTPMethod.PUT,
                contentMD5, DEFAULT_CONTENT_TYPE, bucketName, remoteFileName,
                new Date());

        // check if the bucket exists before trying to create it
        if (bucketExists(bucketName)) {
            return false;
        }

        try {
            putMethod.addRequestHeader("Authorization",
                    StringGenerator.getAuthorizationString(stringToSign, awsID,
                            awsSecret));

            log.debug("Using this stringToSign:" + stringToSign
                    + " to create a bucket");
            putMethod.addRequestHeader("x-amz-date",
                    StringGenerator.getCurrentDateString());
            HttpClient client = new HttpClient();
            int returnCode = client.executeMethod(putMethod);
            log.debug(putMethod.getResponseBodyAsString());
            if (returnCode != -1) {
                if (returnCode == HttpStatus.SC_OK) {
                    bucketExists = true;
                }
            }
            putMethod.releaseConnection();
        } catch (SignatureException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return bucketExists;
    }

    public FileCache getFileCache() {
        return fileCache;
    }

    public static class ScalityLazyBinary extends LazyBinary {

        private static final long serialVersionUID = 1L;

        protected final ScalityBinaryManager sbm;

        protected final String bucketName;

        public ScalityLazyBinary(String digest, FileCache fileCache,
                ScalityBinaryManager sbm, String bucketName) {
            super(digest, fileCache);
            this.bucketName = bucketName;
            this.sbm = sbm;
        }

        @Override
        protected boolean fetchFile(File targetFile) {
            Binary b = sbm.getBinary(this.digest);
            return b.getDigest() == this.digest;
        }

        @Override
        protected Long fetchLength() {
            return sbm.getContentLength(digest);
        }
    }

    /**
     * Garbage collector for S3 binaries that stores the marked (in use)
     * binaries in memory.
     */
    public static class ScalityBinaryGarbageCollector implements
            BinaryGarbageCollector {

        protected final ScalityBinaryManager binaryManager;

        protected volatile long startTime;

        protected BinaryManagerStatus status;

        protected Set<String> marked;

        public ScalityBinaryGarbageCollector(ScalityBinaryManager binaryManager) {
            this.binaryManager = binaryManager;

            // TODO unfinished
            log.error("The garbage collector is not implemented yet");
            throw new NotImplementedException();
        }

        @Override
        public String getId() {
            return "scality:" + binaryManager.bucketName;
        }

        @Override
        public BinaryManagerStatus getStatus() {
            return status;
        }

        @Override
        public boolean isInProgress() {
            // volatile as this is designed to be called from another thread
            return startTime != 0;
        }

        @Override
        public void start() {
            if (startTime != 0) {
                throw new RuntimeException("Alread started");
            }
            startTime = System.currentTimeMillis();
            status = new BinaryManagerStatus();
            marked = new HashSet<String>();
        }

        @Override
        public void mark(String digest) {
            marked.add(digest);
        }

        @Override
        public void stop(boolean delete) {
            if (startTime == 0) {
                throw new RuntimeException("Not started");
            }
            try {
                // list Scality objects in the bucket
                // record those not marked
                Set<String> unmarked = new HashSet<String>();
                List<Contents> objectListing = null;
                do {
                    if (objectListing == null) {
                        objectListing = binaryManager.listObjects(binaryManager.bucketName);
                    }
                    for (Contents contents : objectListing) {
                        String digest = contents.eTag;
                        long length = contents.size;
                        if (marked.contains(digest)) {
                            status.numBinaries++;
                            status.sizeBinaries += length;
                        } else {
                            status.numBinariesGC++;
                            status.sizeBinariesGC += length;
                            // record file to delete
                            unmarked.add(digest);
                            marked.remove(digest); // optimize memory
                        }
                    }
                } while (objectListing.isEmpty());
                marked = null; // help GC

                // delete unmarked objects
                if (delete) {
                    for (String digest : unmarked) {
                        binaryManager.removeBinary(digest);
                    }
                }
            } catch (AmazonClientException e) {
                throw new RuntimeException(e);
            }
            status.gcDuration = System.currentTimeMillis() - startTime;
            startTime = 0;
        }
    }

    @Override
    protected void createGarbageCollector() {
        garbageCollector = new ScalityBinaryGarbageCollector(this);
    }

}
