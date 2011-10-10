/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
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
package org.nuxeo.ecm.core.storage.sql.scality;


/**
 * A configuration bean used to establish connectivity with the Scality system
 */
public class ScalityConfigurationBean {

    private String bucketName;
    private String awsID;
    private String awsSecret;
    private String cacheSizeStr;
    private String hostBase;

    public String getBucketName() {
        return bucketName;
    }
    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }
    public String getAwsID() {
        return awsID;
    }
    public void setAwsID(String awsID) {
        this.awsID = awsID;
    }
    public String getAwsSecret() {
        return awsSecret;
    }
    public void setAwsSecret(String awsSecret) {
        this.awsSecret = awsSecret;
    }
    public String getCacheSizeStr() {
        return cacheSizeStr;
    }
    public void setCacheSizeStr(String cacheSizeStr) {
        this.cacheSizeStr = cacheSizeStr;
    }
    public String getHostBase() {
        return hostBase;
    }
    public void setHostBase(String hostBase) {
        this.hostBase = hostBase;
    }
}