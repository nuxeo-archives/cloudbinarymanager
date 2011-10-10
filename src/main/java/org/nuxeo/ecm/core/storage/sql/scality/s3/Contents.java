package org.nuxeo.ecm.core.storage.sql.scality.s3;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Represents a simple Amazon S3 contents object
 */
@XStreamAlias("Contents")
public class Contents {
    @XStreamAlias("Key")
    public String key;
    @XStreamAlias("LastModified")
    public String lastModified;
    @XStreamAlias("ETag")
    public String eTag;
    @XStreamAlias("Size")
    public long size;
    @XStreamAlias("Owner")
    public Owner owner;
    @XStreamAlias("StorageClass")
    public String storageClass;
}
