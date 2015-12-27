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
package org.nuxeo.ecm.core.storage.sql.scality.s3;

import java.util.ArrayList;
import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * Represents an Amazon S3 bucket listing
 */

@XStreamAlias("ListBucketResult")
public class ListBucketResult {
    @XStreamAlias("Name")
    public String name;

    @XStreamAlias("Prefix")
    public String prefix;

    @XStreamAlias("Marker")
    public String marker;

    @XStreamAlias("MaxKeys")
    public int maxKeys;

    @XStreamAlias("IsTruncated")
    public boolean isTruncated;

    @XStreamAlias("Contents")
    @XStreamImplicit
    public List<Contents> contents = new ArrayList<Contents>();

    public void add(Contents entry) {
        contents.add(entry);
    }

    public List getContent() {
        return contents;
    }



}
