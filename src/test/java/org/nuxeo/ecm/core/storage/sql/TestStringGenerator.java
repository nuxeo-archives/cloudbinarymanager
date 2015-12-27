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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.nuxeo.common.utils.FileUtils;
import org.nuxeo.ecm.core.storage.sql.scality.DigestGenerator;
import org.nuxeo.ecm.core.storage.sql.scality.HTTPMethod;
import org.nuxeo.ecm.core.storage.sql.scality.StringGenerator;
import org.nuxeo.ecm.core.storage.sql.scality.s3.Contents;
import org.nuxeo.ecm.core.storage.sql.scality.s3.ListBucketResult;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;

/**
 * Tests signature string generation to match against known working scality
 * examples
 */
public class TestStringGenerator {

    private static final Log log = LogFactory.getLog(ScalityBinaryManager.class);

    // Used to help manually set the dates based on logs from other s3 tools
    // (like s3cmd)
    private static final String datePattern = "dd MMM yyyy HH:mm:ss";

    // Authentication tokens from the amazon webpage
    String amazonWebpageSampleKeyID = "0PN5J17HBGZHT7JJ3X82";
    String amazonWebpageSampleKey = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o";
    String bucketName = "nuxeo.demo.scality.com";

    @Test
    public void testGETSignature() throws Exception {
        String expectedString = "AWS " + amazonWebpageSampleKeyID + ":"
                + "xXjDGYUmKxnwqr5KXNPGldn5LbA=";
        String stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg";
        String resultingString = StringGenerator.getAuthorizationString(
                stringToSign, amazonWebpageSampleKeyID, amazonWebpageSampleKey);
        assertEquals(expectedString, resultingString);
    }

    @Test
    public void testListContent() throws Exception {
        String expectedString = "AWS " + amazonWebpageSampleKeyID
                + ":jsRt/rhG+Vtp88HrYL706QhE4w4=";
        String stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:42:41 +0000\n/johnsmith/";
        String resultingString = StringGenerator.getAuthorizationString(
                stringToSign, amazonWebpageSampleKeyID, amazonWebpageSampleKey);
        assertEquals(expectedString, resultingString);
    }

    @Test
    public void testACL() throws Exception {
        String expectedString = "AWS " + amazonWebpageSampleKeyID + ":"
                + "thdUi9VAkzhkniLj96JIrOPGi0g=";
        String stringToSign = "GET\n\n\nTue, 27 Mar 2007 19:44:46 +0000\n/johnsmith/?acl";
        String resultingString = StringGenerator.getAuthorizationString(
                stringToSign, amazonWebpageSampleKeyID, amazonWebpageSampleKey);
        assertEquals(expectedString, resultingString);
    }

    @Test
    public void testListAllBuckets() throws Exception {
        String expectedString = "AWS " + amazonWebpageSampleKeyID + ":"
                + "Db+gepJSUbZKwpx1FR0DLtEYoZA=";
        String stringToSign = "GET\n\n\nWed, 28 Mar 2007 01:29:59 +0000\n/";
        String resultingString = StringGenerator.getAuthorizationString(
                stringToSign, amazonWebpageSampleKeyID, amazonWebpageSampleKey);
        assertEquals(expectedString, resultingString);
    }

    @Test
    public void testUnicodeKeys() throws Exception {
        String expectedString = "AWS " + amazonWebpageSampleKeyID + ":"
                + "dxhSBHoI6eVSPcXJqEghlUzZMnY=";
        String stringToSign = "GET\n\n\nWed, 28 Mar 2007 01:49:49 +0000\n/dictionary/fran%C3%A7ais/pr%c3%a9f%c3%a8re";
        String resultingString = StringGenerator.getAuthorizationString(
                stringToSign, amazonWebpageSampleKeyID, amazonWebpageSampleKey);
        assertEquals(expectedString, resultingString);
    }

    @Test
    public void testFileUploadStrings() throws Exception {
        /*
         * SNIFF: PUT /testfile2.txt HTTP/1.1..Host:
         * nuxeo.demo.scality.com.demo.scality.com..Accept-Encoding:
         * identity..Authorization: AWS
         * nuxeo2011:KlwWjaHFtm98mPTGy/sWG9tfdcE=..content-length:
         * 4..content-type: text/plain..x-amz-date: Tue, 30 Aug 2011 14:28: 39
         * +0000....
         */

        /*
         * DEBUG: DEBUG: SignHeaders: 'PUT\n\ntext/plain\n\nx-amz-date:Tue, 30
         * Aug 2011 14:28:39 +0000\n/nuxeo.demo.scality.com/testfile2.txt'
         * DEBUG: CreateRequest: resource[uri]=/testfile2.txt DEBUG: Unicodising
         * 'testfile2.txt' using UTF-8 DEBUG: SignHeaders:
         * 'PUT\n\ntext/plain\n\nx-amz-date:Tue, 30 Aug 2011 14:28:39
         * +0000\n/nuxeo.demo.scality.com/testfile2.txt' testfile2.txt ->
         * s3://nuxeo.demo.scality.com/testfile2.txt [1 of 1] DEBUG:
         * get_hostname(nuxeo.demo.scality.com):
         * nuxeo.demo.scality.com.demo.scality.com DEBUG: format_uri():
         * /testfile2.txt 4 of 4 100% in 0s 16.30 B/s DEBUG: Response:
         * {'status': 200, 'headers': {'content-length': '0', 'server':
         * 'RestServer/1.0', 'connection': 'keep-alive', 'etag':
         * '"0bee89b07a248e27c83fc3d5951213c1"', 'cache-control': 'no-cache',
         * 'date': 'Tue, 30 Aug 2011 14:28:39 GMT', 'content-type':
         * 'application/octet-stream'}, 'reason': 'OK', 'data': '', 'size': 4L}
         * 4 of 4 100% in 1s 3.93 B/s done DEBUG: MD5 sums:
         * computed=0bee89b07a248e27c83fc3d5951213c1,
         * received="0bee89b07a248e27c83fc3d5951213c1"
         */

        String testFilePath = "test-files/testfile2.txt";

        // compare the MD5 values
        File testFile = FileUtils.getResourceFileFromContext(testFilePath);
        String MD5Value = DigestGenerator.getMD5Checksum(testFile);
        String expectedMD5Value = "0bee89b07a248e27c83fc3d5951213c1";
        assertEquals(MD5Value, expectedMD5Value);

        // set the MD5 value to empty string (as it won't be used)
        MD5Value = "";

        // compare strings to sign (before signing)
        String stringToSign = "PUT\n" + MD5Value + "\n" + "text/plain\n" + "\n"
                + "x-amz-date:Tue, 30 Aug 2011 14:28:39 +0000\n"
                + "/nuxeo.demo.scality.com/testfile2.txt";
        String debugSignHeaders = "PUT\n\ntext/plain\n\nx-amz-date:Tue, 30 Aug 2011 14:28:39 +0000\n/nuxeo.demo.scality.com/testfile2.txt";
        assertEquals(stringToSign, debugSignHeaders);

    }

    /**
     * Test string to sign and the authorization string in conjunction with the
     * HTTP method: PUT
     *
     * @throws Exception
     */
    @Test
    public void testPUTStringtoSign() throws Exception {
        String testFileName = "testfile3.txt";
        String testFilePath = "test-files/" + testFileName;
        File testFile = FileUtils.getResourceFileFromContext(testFilePath);
        String contentMD5 = DigestGenerator.getMD5Checksum(testFile);
        contentMD5 = "";// skip contentMD5 calc
        String dateString = "08 Sep 2011 16:15:00";
        Date date = StringGenerator.parseGMTDate(dateString, datePattern);
        String formattedDate = StringGenerator.getCloudFormattedDateString(date);
        String expectedStringToSign = "PUT\n\ntext/plain\n\nx-amz-date:"
                + formattedDate + "\n/nuxeo.demo.scality.com/testfile3.txt";
        String calculatedStringToSign = StringGenerator.getStringToSign(
                HTTPMethod.PUT, contentMD5, "text/plain", bucketName,
                testFile.getName(), date);
        assertEquals(expectedStringToSign, calculatedStringToSign);
    }

    /*
     * SignHeaders: 'PUT\n\ntext/plain\n\nx-amz-date:Thu, 08 Sep 2011 16:15:00
     * +0000\n/nuxeo.demo.scality.com/testfile1.txt' DEBUG: CreateRequest:
     * resource[uri]=/testfile1.txt DEBUG: Unicodising 'testfile1.txt' using
     * UTF-8 DEBUG: SignHeaders: 'PUT\n\ntext/plain\n\nx-amz-date:Thu, 08 Sep
     * 2011 16:15:00 +0000\n/nuxeo.demo.scality.com/testfile1.txt' testfile1.txt
     * -> s3://nuxeo.demo.scality.com/testfile1.txt [1 of 1] DEBUG:
     * get_hostname(nuxeo.demo.scality.com):
     * nuxeo.demo.scality.com.demo.scality.com DEBUG: format_uri():
     * /testfile1.txt
     *
     *
     * PUT /testfile1.txt HTTP/1.1..Host:
     * nuxeo.demo.scality.com.demo.scality.com..Accept-Encoding:
     * identity..Authorization: AWS nuxeo2
     * 011:M52u+2nN7wnuRGCUVTTN/x7hEOo=..content-length: 4..content-type:
     * text/plain..x-amz-date: Thu, 08 Sep 2011 16:15:00 +0000....
     */
    @Test
    public void testPUTAuthorizationString() throws Exception {

        String testFileName = "testfile1.txt";
        String testFilePath = "test-files/" + testFileName;
        File testFile = FileUtils.getResourceFileFromContext(testFilePath);
        String contentMD5 = DigestGenerator.getMD5Checksum(testFile);
        contentMD5 = "";// skip contentMD5 calc
        // prepare date strings
        String dateString = "08 Sep 2011 16:15:00";
        Date date = StringGenerator.parseGMTDate(dateString, datePattern);
        String formattedDate = StringGenerator.getCloudFormattedDateString(date);

        String expectedStringToSign = "PUT\n\ntext/plain\n\nx-amz-date:"
                + formattedDate + "\n/nuxeo.demo.scality.com/testfile1.txt";
        String calculatedStringToSign = StringGenerator.getStringToSign(
                HTTPMethod.PUT, contentMD5, "text/plain", bucketName,
                testFile.getName(), date);
        assertEquals(expectedStringToSign, calculatedStringToSign);
    }

    /*
     * ## s3 debug DEBUG: SignHeaders: 'PUT\n\n\n\nx-amz-date:Wed, 07 Sep 2011
     * 13:58:45 +0000\n/test1.demo.scality.com/' DEBUG: CreateRequest:
     * resource[uri]=/ DEBUG: get_hostname(test1.demo.scality.com):
     * test1.demo.scality.com.demo.scality.com DEBUG: format_uri(): / DEBUG:
     * Response: {'status': 200, 'headers': {'content-length': '0', 'server':
     * 'RestServer/1.0', 'connection': 'keep-alive', 'cache-control':
     * 'no-cache', 'date': 'Wed, 07 Sep 2011 13:58:46 GMT', 'content-type':
     * 'application/octet-stream'}, 'reason': 'OK', 'data': ''} Bucket
     * 's3://test1.demo.scality.com/' created
     *
     * ## intercept PUT / HTTP/1.1..Host:
     * test1.demo.scality.com.demo.scality.com..Accept-Encoding:
     * identity..content-length: 0..Authorization: AWS
     * nuxeo2011:Ud6yvwfw9+MPslesMhW0mNOuk3U=..x-amz-date: Wed, 07 Sep 2011
     * 13:58:45 +0000....
     */
    @Test
    public void testCreateBucket() throws Exception {

        String bucketToCreate = "test1.demo.scality.com";
        String testDateString = "07 Sep 2011 13:58:45";
        Date testDate = StringGenerator.parseGMTDate(testDateString,
                datePattern);
        String expectedStringToSign = "PUT\n\n\n\nx-amz-date:"
                + StringGenerator.getCloudFormattedDateString(testDate)
                + "\n/test1.demo.scality.com/";
        String calculatedStringToSign = StringGenerator.getStringToSign(
                HTTPMethod.PUT, "", "", bucketToCreate, "", testDate);
        // make sure the string to sign matches before signing it (it is in
        // cleartext)
        log.debug(calculatedStringToSign);
        assertEquals(expectedStringToSign, calculatedStringToSign);

    }

    @Test
    public void testPUTFromTutorialExamples() throws Exception {
        String stringToSign = "PUT\nc8fdb181845a4ca6b8fec737b3581d76\ntext/html\nThu, 17 Nov 2005 18:49:58 GMT\nx-amz-magic:abracadabra\nx-amz-meta-author:foo@bar.com\n/quotes/nelson";
        String expectedAuthorizationString = "AWS 44CF9590006BF252F707:jZNOcbfWmD/A/f3hSvVzXZjM2HU=";
        String keyID = "44CF9590006BF252F707";
        String key = "OtxrzxIsfpFjA7SwPzILwy8Bw21TLhquhboDYROV";

        String calculatedAuthorizationString = StringGenerator.getAuthorizationString(
                stringToSign, keyID, key);
        log.debug("Calculated Authorization String:"
                + calculatedAuthorizationString);
        assertEquals(expectedAuthorizationString, calculatedAuthorizationString);
    }

    @Test
    public void testDELETE() throws Exception {
        String testDate = "23 Sep 2011 16:10:36";
        Date date = StringGenerator.parseGMTDate(testDate, datePattern);
        String fileID = "5368976310281a3a81ae650108498726";
        String bucketName = "test5.demo.scality.com";
        String stringToSign = "DELETE\n\n\n\nx-amz-date:"
                + StringGenerator.getCloudFormattedDateString(date) + "\n/"
                + fileID;
        stringToSign = "DELETE\n\n\n\nx-amz-date:Fri, 23 Sep 2011 16:10:36 +0000\n/test5.demo.scality.com/5368976310281a3a81ae650108498726";
        String calculatedStringToSign = StringGenerator.getStringToSign(
                HTTPMethod.DELETE, "", "", bucketName, fileID, date);
        assertEquals(stringToSign, calculatedStringToSign);
    }

    @Test
    public void testListString() throws Exception {
        String testDate = "03 Oct 2011 17:22:43";
        Date date = StringGenerator.parseGMTDate(testDate, datePattern);

        String fileID = "";
        String stringToSign = "GET\n\n\n\nx-amz-date:Mon, 03 Oct 2011 17:22:43 +0000\n/test5.demo.scality.com/";
        String testBucketName="test5.demo.scality.com";

        String calculatedStringToSign = StringGenerator.getStringToSign(
                HTTPMethod.GET, "", "", testBucketName, fileID, date);
        assertEquals(stringToSign, calculatedStringToSign);
        String expectedAuthorizationString = "AWS nuxeo2011:h90bkCyl89gI3i/9GYFMsY3WwPQ=";
    }

    /**
     * Returns a fixed date
     *
     * @return
     */
    public Date getFixedDate() {
        String dateString = "01 Sep 2011 16:58:34";
        DateFormat df = new SimpleDateFormat(datePattern);
        df.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date fixedDate;
        try {
            fixedDate = df.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return fixedDate;
    }

    @Test
    public void parseS3Response(){
        String responseFilePath = "test-files/s3Response.xml";
        XStream xstream = new XStream(new StaxDriver());
        xstream.processAnnotations(ListBucketResult.class);
        xstream.processAnnotations(Contents.class);
        File responseFile= FileUtils.getResourceFileFromContext(responseFilePath);
        InputStream in =null;
        try {
            in = new FileInputStream(responseFile);
            ListBucketResult lbResult=(ListBucketResult)xstream.fromXML(in);
            in.close();
        } catch (FileNotFoundException e) {
             throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
