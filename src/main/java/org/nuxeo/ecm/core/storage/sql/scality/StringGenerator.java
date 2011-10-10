package org.nuxeo.ecm.core.storage.sql.scality;

import java.security.SignatureException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.common.utils.Base64;

/*
 * A request signature, an HMAC, is calculated by concatenating the values of the
 * Service, Operation, and Timestamp parameters, in that order,
 * and then calculating an RFC 2104-compliant HMAC, using the Secret Access Key as the "key."
 * The computed HMAC value should be base64 encoded,
 * and is passed as the value of the Signature request parameter.
 * For more information, go to http://www.faqs.org/rfcs/rfc2104.html.
 * from:
 * http://docs.amazonwebservices.com/AWSMechTurk/latest/AWSMechanicalTurkRequester/index.html?MakingRequests_RequestAuthenticationArticle.html
 */

/**
 * Provides static helper methods to calculate strings used for authentication
 * with amazon's S3/scality.
 */
public class StringGenerator {

    private static final Log log = LogFactory.getLog(StringGenerator.class);

    private static final String HMAC_ALGORITHM = "HmacSHA1";

    public static String getAuthorizationString(String stringToSign,
            String keyID, String key) throws SignatureException {
        String authString = "AWS " + keyID + ":"
                + getEncodedHMAC(stringToSign, key);
        return authString;
    }

    /*
     * DESCRIPTION: The string to be signed is formed by appending
     *
     * the REST verb, content-md5 value, content-type value, expires
     * parameter value, canonicalized x-amz headers and the resource;
     *
     * all separated by newlines.
     *
     * PUT EXAMPLE:
     *
     * PUT\nc8fdb181845a4ca6b8fec737b3581d76\ntext/html\nThu, 17 Nov 2005
     * 18:49:58
     * GMT\nx-amz-magic:abracadabra\nx-amz-meta-author:foo@bar.com\n
     * /quotes/nelson
     *
     * PUT\n c8fdb181845a4ca6b8fec737b3581d76\n text/html\n Thu, 17 Nov 2005
     * 18:49:58 GMT\n
     * x-amz-magic:abracadabra\nx-amz-meta-author:foo@bar.com\n
     * /quotes/nelson
     *
     * The canonical string to be signed is
     * New line characters are included even if there is no Content-Md5 or Content-Type header in the request:
     * Ex: GET\n\n\n\nx-amz-date:Thu, 17 Nov 2005 18:49:58 GMT\n/doc/path
     */

    public static String getStringToSign(Enum httpMethod, String contentMD5,
            String contentType, String bucketName, String fileName, Date date) {
        // sample string variables to test agains AWS
        String stringToSign = "";
        String canonicalizedAmzHeaders = "x-amz-date:"
                + StringGenerator.getCloudFormattedDateString(date);
        String bucketPrefix="";
        if(bucketName.length()>0){
            bucketPrefix="/"+bucketName;
        }
        String canonicalizedResource = "\n"+bucketPrefix+"/";
        if(fileName.length()>0){
            canonicalizedResource += fileName;
        }
        // build the string
        stringToSign = httpMethod + "\n" + contentMD5 + "\n" + contentType
                + "\n\n" + canonicalizedAmzHeaders + canonicalizedResource;
        log.debug("stringToSign>" + stringToSign + "<");
        return stringToSign;
    }

    /*
     * From Amazon Doc:
     *
     * http://s3.amazonaws.com/doc/s3-developer-guide/RESTAuthentication.html
     *
     * Canonicalization for Authorization Header Authentication
     *
     * When authenticating through the Authorization header, you create the
     * string to be signed by concatenating the request verb with canonicalized
     * headers and the resource that the request is targeting.
     *
     * The headers used for request signing are: content-md5, content-type,
     * date, and anything that starts with x-amz-. The string to be signed is
     * formed by appending the REST verb, content-md5 value, content-type value,
     * date value, canonicalized x-amz headers (see recipe below), and the
     * resource; all separated by newlines. (If you cannot set the Date header,
     * use the x-amz-date header as described below.)
     *
     * The resource is the bucket and key (if applicable), separated by a '/'.
     * If the request you are signing is for an ACL or a torrent file, you
     * should include ?acl or ?torrent in the resource part of the canonical
     * string. No other query string parameters should be included, however.
     * Canonicalization for Query String Authentication
     *
     * When authenticating via query string parameters, you create the string to
     * be signed by concatenating the request verb with canonicalized headers
     * and the resource that the request is targeting.
     *
     * The headers used for request signing are the same as those for
     * authorization header authentication, except that the Date field is
     * replaced by the Expires parameter. The Expires parameter is the time when
     * you want the signature to expire, specified as the number of seconds
     * since the epoch time.
     *
     * Thus, the string to be signed is formed by appending the REST verb,
     * content-md5 value, content-type value, expires parameter value,
     * canonicalized x-amz headers (see recipe below), and the resource; all
     * separated by newlines.
     *
     * The resource is the same as that for authorization header authentication:
     * the bucket and key (if applicable), separated by a '/'. If the request
     * you are signing is for an ACL or a torrent file, you should include ?acl
     * or ?torrent in the resource part of the canonical string. No other query
     * string parameters should be included, however. x-amz headers are
     * canonicalized by:
     *
     * Lower-case header name
     *
     * Headers sorted by header name
     *
     * The values of headers whose names occur more than once should be white
     * space-trimmed and concatenated with comma separators to be compliant with
     * section 4.2 of RFC 2616.
     *
     * remove any whitespace around the colon in the header
     *
     * remove any newlines ('\n') in continuation lines
     *
     * separate headers by newlines ('\n')
     *
     * Some important points:
     *
     * The string to sign (verb, headers, resource) must be UTF-8 encoded.
     *
     * The content-type and content-md5 values are optional, but if you do not
     * include them you must still insert a newline at the point where these
     * values would normally be inserted.
     *
     * Some toolkits may insert headers that you do not know about beforehand,
     * such as adding the header 'Content-Type' during a PUT. In most of these
     * cases, the value of the inserted header remains constant, allowing you to
     * discover the missing headers using tools such as Ethereal or tcpmon.
     *
     * Some toolkits make it difficult to manually set the date. If you have
     * trouble including the value of the 'Date' header in the canonicalized
     * headers, you can include an 'x-amz-date' header prior to
     * canonicalization. The value of the x-amz-date header must be in one of
     * the RFC 2616 formats (http://www.ietf.org/rfc/rfc2616.txt). If S3 sees an
     * x-amz-date header in the request, it will ignore the Date header when
     * validating the request signature. If you include the x-amz-date header,
     * you must still include a newline character in the canonicalized string at
     * the point where the Date value would normally be inserted.
     *
     * The value of the Date or, if applicable, x-amz-date header must specify a
     * time no more than 15 minutes away from the S3 webserver's clock.
     *
     * The hash function to compute the signature is HMAC-SHA1 defined in RFC
     * 2104 (http://www.ietf.org/rfc/rfc2104.txt), using your Secret Access Key
     * as the key.
     */
    public static String getEncodedHMAC(String stringToSign, String signingKey)
            throws java.security.SignatureException {
        String encodedHMAC;
        try {
            SecretKeySpec secretKeySpec = new SecretKeySpec(
                    signingKey.getBytes(), HMAC_ALGORITHM);
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(secretKeySpec);
            byte[] hmac = mac.doFinal(stringToSign.getBytes());
            encodedHMAC = Base64.encodeBytes(hmac);
        } catch (Exception e) {
            throw new SignatureException("Failed to generate HMAC : "
                    + e.getMessage());
        }
        return encodedHMAC;
    }

    /**
     * Formats the date to a GMT string ready to be used with the Amazon S3
     * service
     *
     * @param date
     * @return
     */
    public static String getCloudFormattedDateString(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        SimpleDateFormat sdf = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String dateString = sdf.format(cal.getTime()) + " +0000";
        log.debug(dateString);
        return dateString;
    }

    /**
     * Provides a date object from a date string following a specific date
     * pattern.
     *
     * The dateString argument is interpreted as a GMT timezone date string so that the current
     * system zone is not assumed.
     *
     * @param dateString
     * @param datePattern
     * @return
     */
    public static Date parseGMTDate(String dateString, String datePattern) {
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


    /**
     * Returns a string representing the current date in a format that's used
     * for generating the authentication strings in the Scality system
     *
     * @return
     */
    public static String getCurrentDateString() {
        Date now = new Date();
        return StringGenerator.getCloudFormattedDateString(now);
    }

    /**
     * Determines the file size from a provided string
     *
     * @param string
     * @return
     */
    public static long parseSizeInBytes(String string) {
        String digits = string;
        if (digits.length() == 0) {
            throw new RuntimeException("Invalid empty size");
        }
        char unit = digits.charAt(digits.length() - 1);
        if (unit == 'b' || unit == 'B') {
            digits = digits.substring(0, digits.length() - 1);
            if (digits.length() == 0) {
                throw new RuntimeException("Invalid size: '" + string + "'");
            }
            unit = digits.charAt(digits.length() - 1);
        }
        long mul;
        switch (unit) {
        case 'k':
        case 'K':
            mul = 1024;
            break;
        case 'm':
        case 'M':
            mul = 1024 * 1024;
            break;
        case 'g':
        case 'G':
            mul = 1024 * 1024 * 1024;
            break;
        default:
            if (!Character.isDigit(unit)) {
                throw new RuntimeException("Invalid size: '" + string + "'");
            }
            mul = 1;
        }
        if (mul != 1) {
            digits = digits.substring(0, digits.length() - 1);
            if (digits.length() == 0) {
                throw new RuntimeException("Invalid size: '" + string + "'");
            }
        }
        try {
            return Long.parseLong(digits) * mul;
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid size: '" + string + "'");
        }
    }
}