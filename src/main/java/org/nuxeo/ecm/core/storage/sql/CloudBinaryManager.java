/*
 * (C) Copyright 2011 Nuxeo SA (http://nuxeo.com/) and others.
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
 *     Mathieu Guillaume
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.file.FileCache;

/**
 */
public abstract class CloudBinaryManager extends DefaultBinaryManager {

    private static final Log log = LogFactory.getLog(CloudBinaryManager.class);



    private static final Pattern MD5_RE = Pattern.compile("[0-9a-f]{32}");

    public abstract void initialize(RepositoryDescriptor repositoryDescriptor)
            throws IOException;

    protected abstract void createGarbageCollector();

    public abstract Binary getBinary(InputStream in) throws IOException;

    public abstract Binary getBinary(String digest);

    protected abstract void removeBinary(String digest);

    public static boolean isMD5(String digest) {
        return MD5_RE.matcher(digest).matches();
    }

    protected FileCache fileCache;


}
