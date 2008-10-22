/*

   Derby - Class org.apache.derby.impl.jdbc.LOBStoredProcedure

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.derby.iapi.jdbc.EngineLOB;
import org.apache.derby.iapi.reference.Limits;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Contains the stored procedures that will be used in the
 * LOB client side methods.
 */
public class LOBStoredProcedure {

    /**
     * The maximum length of the data returned from the BLOB stored procedures.
     * <p>
     * This value is currently dictated by the maximum length of
     * VARCHAR/VARBINARY, because these are the return types of the stored
     * procedures.
     */
    public static final int MAX_BLOB_RETURN_LEN = Limits.DB2_VARCHAR_MAXWIDTH;

    /**
     * The maximum length of the data returned from the CLOB stored procedures.
     * <p>
     * This value is currently dictated by the maximum length of
     * VARCHAR/VARBINARY, because these are the return types of the stored
     * procedures, and the modified UTF8 encoding used for CLOB data. This
     * threshold value could be higher (equal to {@code MAX_BLOB_RETURN_LEN}),
     * but then the procedure fetching data from the CLOB must be rewritten to
     * have more logic.
     * <p>
     * For now we use the defensive assumption that all characters are
     * represented by three bytes.
     */
    public static final int MAX_CLOB_RETURN_LEN = MAX_BLOB_RETURN_LEN / 3;

    /**
     * Creates a new empty Clob and registers it in the HashMap in the
     * Connection and returns the locator value corresponding to this Clob.
     * @return an integer that maps to the Clob value created.
     * @throws SQLException
     */
    public static int CLOBCREATELOCATOR() throws SQLException {
        EngineLOB clob = (EngineLOB)getEmbedConnection().createClob();
        return clob.getLocator();
    }

    /**
     * Removes the supplied LOCATOR entry from the hash map.
     * @param LOCATOR an integer that represents the locator that needs to be
     *                removed from the hash map.
     * @throws SQLException
     */
    public static void CLOBRELEASELOCATOR(int LOCATOR) throws SQLException {
        Clob clob = (Clob)getEmbedConnection().getLOBMapping(LOCATOR);
        if (clob == null) {
            throw newSQLException(SQLState.LOB_LOCATOR_INVALID);
        }
        EmbedClob embedClob = (EmbedClob)clob;
        embedClob.free();
        getEmbedConnection().removeLOBMapping(LOCATOR);
    }

    /**
     * returns the first occurrence of the given search string from the
     * given start search position inside the Clob.
     *
     * @param LOCATOR an integer that represents the locator of the Clob
     *                in which the given position of the given sub-string
     *                needs to be found.
     *
     * @param searchLiteral a String whose occurence inside the Clob needs to
     *                      be found starting from pos.
     *
     * @param fromPosition an integer that represents the position inside
     *         the Clob from which the search needs to begin.
     *
     * @return an integer that represents the position inside the Clob of the
     *         first occurrence of the sub-string from the given starting
     *         position.
     *
     * @throws SQLException
     */
    public static long CLOBGETPOSITIONFROMSTRING(int LOCATOR, String searchLiteral,
        long fromPosition) throws SQLException {
        return getClobObjectCorrespondingtoLOCATOR(LOCATOR).
            position(searchLiteral, fromPosition);
    }

    /**
     * returns the first occurrence of the given search string from the
     * given start search position inside the Clob.
     *
     * @param LOCATOR an integer that represents the locator of the Clob
     *                in which the given position of the given sub-string
     *                needs to be found.
     *
     * @param searchLocator a Locator representing a Clob whose occurence inside
     *                      the Clob needs to be found starting from pos.
     *
     * @param fromPosition an integer that represents the position inside
     *         the Clob from which the search needs to begin.
     *
     * @return an integer that represents the position inside the Clob of the
     *         first occurrence of the sub-string from the given starting
     *         position.
     *
     * @throws SQLException
     */
    public static long CLOBGETPOSITIONFROMLOCATOR(int LOCATOR, int searchLocator,
        long fromPosition) throws SQLException {
        return getClobObjectCorrespondingtoLOCATOR(LOCATOR).position(
            getClobObjectCorrespondingtoLOCATOR(searchLocator), fromPosition);
    }

    /**
     * returns the length of the Clob corresponding to the LOCATOR value.
     *
     * @param LOCATOR an integer that represents the locator of the Clob whose
     *        length needs to be obtained.
     * @return an integer that represents the length of the Clob.
     * @throws java.sql.SQLException 
     *
     */
    public static long CLOBGETLENGTH(int LOCATOR) throws SQLException {
        return getClobObjectCorrespondingtoLOCATOR(LOCATOR).length();
    }

    /**
     * Returns the {@code String} starting from {@code pos} and consisting of
     * up to {@code len} consecutive characters from the {@code Clob}
     * corresponding to {@code LOCATOR}.
     *
     * @param LOCATOR an integer that represents the LOCATOR used
     *                to retrieve an instance of the LOB.
     * @param pos a long that represents the position from which
     *            the substring begins.
     * @param len an integer representing the maximum length of the substring.
     *      The value will be reduced to the maximum allowed return length if
     *      required (see {@link #MAX_CLOB_RETURN_LEN}).
     * @return A substring from the {@code Clob} starting at the given position,
     *      not longer than {@code len} characters.
     * @throws SQLException
     */
    public static String CLOBGETSUBSTRING(int LOCATOR,
        long pos, int len) throws SQLException {
        // Don't read more than what we can represent as a VARCHAR.
        // See DERBY-3769.
        len = Math.min(len, MAX_CLOB_RETURN_LEN);
        return getClobObjectCorrespondingtoLOCATOR(LOCATOR).getSubString(pos, len);
    }

    /**
     * replaces the characters starting at fromPosition and with length ForLength
     *
     * @param LOCATOR an integer that represents the locator of the Clob in which
     *                the characters need to be replaced.
     *
     * @param pos an integer that represents the position inside the Clob from which
     *            the string needs to be replaced.
     *
     * @param length the number of characters from the string that need to be used for
     *               replacement.
     *
     * @param str the string from which the repalcement characters are built.
     *
     * @throws SQLException
     */
    public static void CLOBSETSTRING(int LOCATOR, long pos, int length,
        String str) throws SQLException {
        getClobObjectCorrespondingtoLOCATOR(LOCATOR).setString(pos, str, 0, length);
    }

    /**
     * truncates the Clob value represented by LOCATOR to have a length
     * of length.
     *
     * @param LOCATOR an integer that represents the LOCATOR used to retrieve an
     *                instance of the LOB.
     * @param length an integer that represents the length to which the Clob
     *               must be truncated to.
     * @throws SQLException
     */
    public static void CLOBTRUNCATE(int LOCATOR, long length) throws SQLException {
        getClobObjectCorrespondingtoLOCATOR(LOCATOR).truncate(length);
    }

    /**
     * returns the Clob object corresponding to the locator.
     * @param LOCATOR an integer that represents the locator corresponding
     *                to the Clob object requested.
     * @return a Clob object that is mapped to the LOCATOR object passed in.
     * @throws a SQLException.
     */
    private static Clob getClobObjectCorrespondingtoLOCATOR(int LOCATOR)
    throws SQLException {
        Clob clob = (Clob)getEmbedConnection().getLOBMapping(LOCATOR);
        if (clob == null) {
            throw newSQLException(SQLState.LOB_LOCATOR_INVALID);
        }
        return clob;
    }

    /**
     * Creates a new empty Blob and registers it in the HashMap in the
     * Connection and returns the locator value corresponding to this Blob.
     * @return an integer that maps to the Blob value created.
     * @throws SQLException
     */
    public static int BLOBCREATELOCATOR() throws SQLException {
        EngineLOB blob = (EngineLOB)getEmbedConnection().createBlob();
        return blob.getLocator();
    }

    /**
     * Removes the supplied LOCATOR entry from the hash map.
     * @param LOCATOR an integer that represents the locator that needs to be
     *                removed from the hash map.
     * @throws SQLException
     */
    public static void BLOBRELEASELOCATOR(int LOCATOR) throws SQLException {
        Blob blob = (Blob)getEmbedConnection().getLOBMapping(LOCATOR);
        if (blob == null) {
            throw newSQLException(SQLState.LOB_LOCATOR_INVALID);
        }
        EmbedBlob embedBlob = (EmbedBlob)blob;
        embedBlob.free();
        getEmbedConnection().removeLOBMapping(LOCATOR);
    }

    /**
     *
     * Returns the first occurrence of locator in the Blob.
     *
     * @param LOCATOR the locator value of the Blob in which the seaching needs
     *                to be done.
     * @param searchLocator the locator value of the Blob whose position needs
     *                      needs to be found.
     * @param pos the position from which the seaching needs to be done.
     * @return the position at which the first occurrence of the Blob is
     *         found.
     * @throws SQLException
     *
     */
    public static long BLOBGETPOSITIONFROMLOCATOR(int LOCATOR,
        int searchLocator, long pos) throws SQLException {
        return getBlobObjectCorrespondingtoLOCATOR(LOCATOR).position(
            getBlobObjectCorrespondingtoLOCATOR(searchLocator), pos);
    }

    /**
     *
     * Returns the first occurrence of the byte array in the Blob.
     *
     * @param LOCATOR the locator value of the Blob in which the seaching needs
     *                to be done.
     * @param searchBytes the byte array whose position needs needs to be found.
     * @param pos the position from which the seaching needs to be done.
     * @return the position at which the first occurrence of the Byte array is
     *         found.
     * @throws SQLException
     *
     */
    public static long BLOBGETPOSITIONFROMBYTES(int LOCATOR,
        byte [] searchBytes, long pos) throws SQLException {
        return getBlobObjectCorrespondingtoLOCATOR(LOCATOR).position(searchBytes, pos);
    }

    /**
     *
     * Returns the length in bytes of the Blob.
     *
     * @param LOCATOR the locator value of the Blob whose length needs to
     *                be found.
     * @return the length of the Blob object mapped to the locator .
     * @throws SQLException
     *
     */
    public static long BLOBGETLENGTH(int LOCATOR) throws SQLException {
        return getBlobObjectCorrespondingtoLOCATOR(LOCATOR).length();
    }

    /**
     * Reads up to len bytes from the associated {@code Blob} and returns a
     * byte array containing the bytes read.
     * <p>
     * Note that a smaller number of bytes than requested might be returned. The
     * number of bytes returned can be found by checking the length of the
     * returned byte array.
     *
     * @param LOCATOR the locator value of the Blob from which the byte array
     *                needs to be retrieved.
     * @param len the maximum number of bytes to read. The value will be
     *      reduced to the maximum allowed return length if required
     *      (see {@link #MAX_BLOB_RETURN_LEN}).
     * @param pos the position from which the bytes from the Blob need to be
     *            retrieved.
     * @return A byte array containing the bytes read, starting from position
     *      {@code pos} in the {@code Blob}.
     * @throws SQLException
     *
     */
    public static byte[] BLOBGETBYTES(int LOCATOR, long pos, int len)
    throws SQLException {
        // Don't read more than what we can represent as a VARBINARY.
        // See DERBY-3769.
        len = Math.min(len, MAX_BLOB_RETURN_LEN);
        return getBlobObjectCorrespondingtoLOCATOR(LOCATOR).getBytes(pos, len);
    }

    /**
     *
     * Replaces the bytes at pos with len bytes
     *
     * @param LOCATOR the integer that represents the Blob in which the bytes
     *                need to be replaced.
     * @param pos the position stating from which the byte replacement needs to
     *            happen.
     * @param len the number of bytes that need to be used in replacement.
     * @param replaceBytes the byte array that contains the bytes that needs to
     *                     be used for replacement.
     * @throws SQLException
     *
     */
    public static void BLOBSETBYTES(int LOCATOR, long pos, int len,
        byte [] replaceBytes) throws SQLException {
        getBlobObjectCorrespondingtoLOCATOR(LOCATOR).setBytes
            (pos, replaceBytes, 0, len);
    }

    /**
     * truncates the Blob value represented by LOCATOR to have a length
     * of length.
     *
     * @param LOCATOR an integer that represents the LOCATOR used to retrieve an
     *                instance of the LOB.
     * @param length an integer that represents the length to which the Blob
     *               must be truncated to.
     * @throws SQLException
     */
    public static void BLOBTRUNCATE(int LOCATOR, long length) throws SQLException {
        getBlobObjectCorrespondingtoLOCATOR(LOCATOR).truncate(length);
    }

    /**
     * returns the Blob object corresponding to the locator.
     * @param LOCATOR an integer that represents the locator corresponding
     *                to the Blob object requested.
     * @return a Blob object that is mapped to the LOCATOR object passed in.
     * @throws SQLException
     */
    private static Blob getBlobObjectCorrespondingtoLOCATOR(int LOCATOR)
    throws SQLException {
        Blob blob = (Blob)getEmbedConnection().getLOBMapping(LOCATOR);
        if (blob == null) {
            throw newSQLException(SQLState.LOB_LOCATOR_INVALID);
        }
        return blob;
    }

    /**
     * Returns the EmbedConnection object.
     * @throws SQLException.
     */
    private static EmbedConnection getEmbedConnection() throws SQLException {
        return (EmbedConnection)DriverManager
            .getConnection("jdbc:default:connection");
    }

    /**
     * Generate the SQLException with the appropriate
     * SQLState.
     *
     * @param messageId The messageId of the message associated with this message.
     * @return a SQLEXception.
     */
    private static SQLException newSQLException(String messageId) {
        return Util.generateCsSQLException(messageId);
    }
}
