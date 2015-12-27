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
package org.nuxeo.ecm.core.storage.sql.scality;

import java.io.*;
import java.security.MessageDigest;

/**
 * Calculates file digests
 */
public class DigestGenerator {

    private static final String CHECKSUM_ALGORITHM="MD5";

    public static byte[] generateChecksum(InputStream inputStream, String digestAlgorithm) throws Exception {
        byte[] buffer = new byte[1024];
        MessageDigest md5Instance = MessageDigest.getInstance(digestAlgorithm);
        int readBytes;
        do {
            readBytes = inputStream.read(buffer);
            if (readBytes > 0) {
                md5Instance.update(buffer, 0, readBytes);
            }
        } while (readBytes != -1);
        inputStream.close();
        return md5Instance.digest();
    }

    public static String getMD5Checksum(File file) {
        StringBuffer sb= new StringBuffer();
        try {
            InputStream fis = new FileInputStream(file);
            byte[] b = generateChecksum(fis,CHECKSUM_ALGORITHM);
            for (int i = 0; i < b.length; i++) {
                sb.append(Integer.toString((b[i] & 0xff) + 0x100, 16).substring(1));
            }
        } catch (FileNotFoundException e) {
             throw new RuntimeException(e);
        } catch (Exception e) {
             throw new RuntimeException(e);
        }
        return sb.toString();
    }

}
