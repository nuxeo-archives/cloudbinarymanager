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
package org.nuxeo.ecm.core.storage.sql.scality.s3;

import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Owner object used in Amazon S3 contents
 */
@XStreamAlias("Owner")
public class Owner {

    @XStreamAlias("ID")
    public String id;

    @XStreamAlias("DisplayName")
    public String displayName;
}
