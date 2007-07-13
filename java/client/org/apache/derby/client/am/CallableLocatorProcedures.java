/*
 
   Derby - Class org.apache.derby.client.am.CallableLocatorProcedures
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

package org.apache.derby.client.am;

import org.apache.derby.shared.common.error.ExceptionUtil;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * Contains the necessary methods to call the stored procedure that
 * operate on LOBs identified by locators.  An instance of this class
 * will be initialized with a <code>Connection</code> parameter and
 * all calls will be made on that connection.
 * <p>
 * The class makes sure that each procedure call is only prepared once
 * per instance.  Hence, it will keep references to
 * <code>CallableStatement</code> objects for procedures that have
 * been called through this instance.  This makes it possible to
 * prepare each procedure call only once per <code>Connection</code>.
 * <p> 
 * Since LOBs can not be parameters to stored procedures, the
 * framework should make sure that calls involving a byte[] or String
 * that does not fit in a VARCHAR (FOR BIT DATA), are split into
 * several calls each operating on a fragment of the LOB.
 *
 * @see Connection#locatorProcedureCall for an example of how to use
 * this class.
 */
class CallableLocatorProcedures 
{
    //caches the information from a Stored Procedure
    //call as to whether locator support is available in
    //the server or not.
    boolean isLocatorSupportAvailable = true;
    
    // One member variable for each stored procedure that can be called.
    // Used to be able to only prepare each procedure call once per connection.
    private CallableStatement blobCreateLocatorCall;
    private CallableStatement blobReleaseLocatorCall;
    private CallableStatement blobGetPositionFromLocatorCall;
    private CallableStatement blobGetPositionFromBytesCall;
    private CallableStatement blobGetLengthCall;
    private CallableStatement blobGetBytesCall;
    private CallableStatement blobSetBytesCall;
    private CallableStatement blobTruncateCall;
    private CallableStatement clobCreateLocatorCall;
    private CallableStatement clobReleaseLocatorCall;
    private CallableStatement clobGetPositionFromStringCall;
    private CallableStatement clobGetPositionFromLocatorCall;
    private CallableStatement clobGetLengthCall;
    private CallableStatement clobGetSubStringCall;
    private CallableStatement clobSetStringCall;
    private CallableStatement clobTruncateCall;

    /**
     * The connection to be used when calling the stored procedures.
     */
    private final Connection connection; 

    /**
     * Max size of byte[] and String parameters to procedures
     */
    private static final int VARCHAR_MAXWIDTH = 32672;

    //Constant representing an invalid locator value
    private static final int INVALID_LOCATOR = -1;

    /**
     * Create an instance to be used for calling locator-based stored
     * procedures.
     *
     * @param conn the connection to be used to prepare calls.
     */
    CallableLocatorProcedures(Connection conn) 
    {
        this.connection = conn;
    }

    /**
     * Allocates an empty BLOB on server and returns its locator.  Any
     * subsequent operations on this BLOB value will be stored in temporary
     * space on the server.
     *
     * @throws org.apache.derby.client.am.SqlException
     * @return locator that identifies the created BLOB.
     */
    int blobCreateLocator() throws SqlException
    {
        //The information on whether the locator support
        //is available is cached in the boolean
        //isLocatorSupportAvailable. If this is false
        //we can return -1
        if (!isLocatorSupportAvailable) {
            return INVALID_LOCATOR;
        }
        
        try {
            if (blobCreateLocatorCall == null) {
                blobCreateLocatorCall = connection.prepareCallX
                        ("? = CALL SYSIBM.BLOBCREATELOCATOR()",
                        java.sql.ResultSet.TYPE_FORWARD_ONLY,
                        java.sql.ResultSet.CONCUR_READ_ONLY,
                        connection.holdability());
                blobCreateLocatorCall
                        .registerOutParameterX(1, java.sql.Types.INTEGER);
                // Make sure this statement does not commit user transaction
                blobCreateLocatorCall.isAutoCommittableStatement_ = false;
            }
            blobCreateLocatorCall.executeX();
        }
        catch(SqlException sqle) {
            //An exception has occurred while calling the stored procedure
            //used to create the locator value. 
            
            //We verify to see if this SqlException has a SQLState of
            //42Y03(SQLState.LANG_NO_SUCH_METHOD_ALIAS)
            //(corresponding to the stored procedure not being found)
            
            //This means that locator support is not available. 
            
            //This information is cached so that each time to determine
            //if locator support is available we do not have to make a
            //round trip to the server.
            if (sqle.getSQLState().compareTo
                    (ExceptionUtil.getSQLStateFromIdentifier
                    (SQLState.LANG_NO_SUCH_METHOD_ALIAS)) == 0) {
                isLocatorSupportAvailable = false;
                return INVALID_LOCATOR;
            }
            else {
                //The SqlException has not occurred because of the
                //stored procedure not being found. Hence we simply throw
                //it back.
                throw sqle;
            }
        }
        
        return blobCreateLocatorCall.getIntX(1);
    }

    /**
     * This method frees the BLOB and releases the resources that it
     * holds. (E.g., temporary space used to store this BLOB on the server.)
     * @param locator locator that designates the BLOB to be released.
     * @throws org.apache.derby.client.am.SqlException 
     */
    void blobReleaseLocator(int locator) throws SqlException
    {
        if (blobReleaseLocatorCall == null) {
            blobReleaseLocatorCall = connection.prepareCallX
                ("CALL SYSIBM.BLOBRELEASELOCATOR(?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Make sure this statement does not commit user transaction
            blobReleaseLocatorCall.isAutoCommittableStatement_ = false;
        }

        blobReleaseLocatorCall.setIntX(1, locator);
        try {
            blobReleaseLocatorCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
    }

    /**
     * Retrieves the byte position in the BLOB value designated by this
     * <code>locator</code> at which pattern given by
     * <code>searchLocator</code> begins. The search begins at position
     * <code>fromPosition</code>.
     * @param locator locator that identifies the BLOB to be searched.
     * @param searchLocator locator designating the BLOB value for which to
     *        search
     * @param fromPosition the position in the BLOB value
     *        at which to begin searching; the first position is 1
     * @throws org.apache.derby.client.am.SqlException 
     * @return the position at which the pattern begins, else -1
     */
    long blobGetPositionFromLocator(int locator, 
                                    int searchLocator, 
                                    long fromPosition) throws SqlException
    {
        if (blobGetPositionFromLocatorCall == null) {
            blobGetPositionFromLocatorCall = connection.prepareCallX
                ("? = CALL SYSIBM.BLOBGETPOSITIONFROMLOCATOR(?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            blobGetPositionFromLocatorCall
                .registerOutParameterX(1, java.sql.Types.BIGINT);
            // Make sure this statement does not commit user transaction
            blobGetPositionFromLocatorCall.isAutoCommittableStatement_ = false;
        }

        blobGetPositionFromLocatorCall.setIntX(2, locator);
        blobGetPositionFromLocatorCall.setIntX(3, searchLocator);
        blobGetPositionFromLocatorCall.setLongX(4, fromPosition);
        try {
            blobGetPositionFromLocatorCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
        return blobGetPositionFromLocatorCall.getLongX(1);
    }


    /**
     * Retrieves the byte position at which the specified byte array
     * <code>searchLiteral</code> begins within the <code>BLOB</code> value
     * identified by <code>locator</code>.  The search for
     * <code>searchLiteral</code> begins at position <code>fromPosition</code>.
     * <p>
     * If <code>searchLiteral</code> is longer than the maximum length of a
     * VARCHAR FOR BIT DATA, it will be split into smaller fragments, and
     * repeated procedure calls will be made to perform the entire search
     *
     * @param locator locator that identifies the BLOB to be searched.
     * @param searchLiteral the byte array for which to search
     * @param fromPosition the position at which to begin searching; the
     *        first position is 1
     * @throws org.apache.derby.client.am.SqlException 
     * @return the position at which the pattern appears, else -1
     */
    long blobGetPositionFromBytes(int locator, 
                                  byte[] searchLiteral, 
                                  long fromPosition) throws SqlException
    {
        long blobLength = -1;  // Will be fetched from server if needed
        int patternLength = searchLiteral.length;

        // If searchLiteral needs to be partitioned, 
        // we may have to try several start positions
        do { 
            long foundAt = blobGetPositionFromBytes(locator, 
                                                    fromPosition,
                                                    searchLiteral, 
                                                    0,
                                                    VARCHAR_MAXWIDTH);

            // If searchLiteral is longer than VARCHAR_MAXWIDTH, 
            // we need to check the rest
            boolean tryAgain = false;
            if ((patternLength > VARCHAR_MAXWIDTH) && (foundAt > 0)) {
                // First part of searchLiteral matched, check rest
                int comparedSoFar = VARCHAR_MAXWIDTH;
                while (comparedSoFar < patternLength) {
                    int numBytesThisRound 
                        = Math.min(patternLength - comparedSoFar, 
                                   VARCHAR_MAXWIDTH);
                    long pos = blobGetPositionFromBytes(locator,
                                                        foundAt + comparedSoFar,
                                                        searchLiteral,
                                                        comparedSoFar,
                                                        numBytesThisRound);

                    if (pos != (foundAt + comparedSoFar)) { 
                        // This part did not match
                        // Try to find a later match for the same prefix
                        tryAgain = true;
                        fromPosition = foundAt + 1;
                        break;
                    }

                    comparedSoFar += numBytesThisRound;
                }
            }
            
            if (!tryAgain) return foundAt;

            // Need Blob length in order to determine when to stop
            if (blobLength < 0) {  
                blobLength = blobGetLength(locator);
            }
        } while (fromPosition + patternLength <= blobLength);

        return -1;  // No match
    }
        
    
    /**
     * Retrieves the byte position at which the specified part of the byte
     * array <code>searchLiteral</code> begins within the <code>BLOB</code>
     * value identified by <code>locator</code>.  The search for
     * <code>searchLiteral</code> begins at position <code>fromPosition</code>.
     * <p>
     * This is a helper function used by blobGetPositionFromBytes(int, byte[],
     * long) for each call to the BLOBGETPOSITIONFROMBYTES procedure.
     *
     * @param locator locator that identifies the BLOB to be searched.
     * @param searchLiteral the byte array for which to search
     * @param fromPosition the position at which to begin searching; the
     *        first position is 1
     * @param offset the offset into the array <code>searchLiteral</code> at
     *        which the pattern to search for starts
     * @param length the number of bytes from the array of bytes
     *        <code>searchLiteral</code> to use for the pattern to search
     *        for. It is assumed that this length is smaller than the maximum
     *        size of a VARCHAR FOR BIT DATA column.  Otherwise, an exception
     *        will be thrown.
     * @throws org.apache.derby.client.am.SqlException 
     * @return the position at which the pattern appears, else -1
     */
    private long blobGetPositionFromBytes(int locator,
                                          long fromPosition, 
                                          byte[] searchLiteral,
                                          int offset,
                                          int length) throws SqlException
    {
        if (blobGetPositionFromBytesCall == null) {
            blobGetPositionFromBytesCall = connection.prepareCallX
                ("? = CALL SYSIBM.BLOBGETPOSITIONFROMBYTES(?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            blobGetPositionFromBytesCall
                .registerOutParameterX(1, java.sql.Types.BIGINT);
            // Make sure this statement does not commit user transaction
            blobGetPositionFromBytesCall.isAutoCommittableStatement_ = false;
        }
        
        byte[] bytesToBeCompared = searchLiteral;
        int numBytes = Math.min(searchLiteral.length - offset, length);
        if (numBytes != bytesToBeCompared.length) {
            // Need an array that contains just what is to be sent
            bytesToBeCompared = new byte[numBytes];
            System.arraycopy(searchLiteral, offset,
                             bytesToBeCompared, 0, numBytes);
        }

        blobGetPositionFromBytesCall.setIntX(2, locator);
        blobGetPositionFromBytesCall.setBytesX(3, bytesToBeCompared);
        blobGetPositionFromBytesCall.setLongX(4, fromPosition);
        try {
            blobGetPositionFromBytesCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
        return blobGetPositionFromBytesCall.getLongX(1);
    }

    
    /**
     * Returns the number of bytes in the <code>BLOB</code> value
     * designated by this <code>sourceLocator</code>.
     * 
     * @param sourceLocator locator that identifies the BLOB
     * @throws org.apache.derby.client.am.SqlException 
     * @return length of the <code>BLOB</code> in bytes 
     */
    long blobGetLength(int sourceLocator) throws SqlException
    {
        if (blobGetLengthCall == null) {
            blobGetLengthCall = connection.prepareCallX
                ("? = CALL SYSIBM.BLOBGETLENGTH(?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            blobGetLengthCall.registerOutParameterX(1, java.sql.Types.BIGINT);
            // Make sure this statement does not commit user transaction
            blobGetLengthCall.isAutoCommittableStatement_ = false;
        }

        blobGetLengthCall.setIntX(2, sourceLocator);
        try {
            blobGetLengthCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
        return blobGetLengthCall.getLongX(1);
    }

    /**
     * Retrieves all or part of the <code>BLOB</code> value that is identified
     * by <code>sourceLocator</code>, as an array of bytes.  This
     * <code>byte</code> array contains up to <code>forLength</code>
     * consecutive bytes starting at position <code>fromPosition</code>.
     * <p>
     * If <code>forLength</code> is larger than the maximum length of a VARCHAR
     * FOR BIT DATA, the reading of the BLOB will be split into repeated
     * procedure calls.
     *
     * @param sourceLocator locator that identifies the Blob to operate on
     * @param fromPosition the ordinal position of the first byte in the
     *        <code>BLOB</code> value to be extracted; the first byte is at
     *        position 1
     * @param forLength the number of consecutive bytes to be copied; the value
     *        for length must be 0 or greater.  Specifying a length that goes
     *        beyond the end of the BLOB (i.e., <code>fromPosition + forLength
     *        > blob.length()</code>), will result in an error.
     * @throws org.apache.derby.client.am.SqlException 
     * @return a byte array containing up to <code>forLength</code> consecutive
     *         bytes from the <code>BLOB</code> value designated by
     *         <code>sourceLocator</code>, starting with the byte at position
     *         <code>fromPosition</code>
     */
    byte[] blobGetBytes(int sourceLocator, long fromPosition, int forLength) 
        throws SqlException
    {
        if (forLength == 0) return new byte[0];
        
        if (blobGetBytesCall == null) {
            blobGetBytesCall = connection.prepareCallX
                ("? = CALL SYSIBM.BLOBGETBYTES(?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            blobGetBytesCall.registerOutParameterX(1, java.sql.Types.VARBINARY);
            // Make sure this statement does not commit user transaction
            blobGetBytesCall.isAutoCommittableStatement_ = false;
        }

        byte retVal[] = null;
        int gotSoFar = 0;
        while (gotSoFar < forLength) {
            blobGetBytesCall.setIntX(2, sourceLocator);
            blobGetBytesCall.setLongX(3, fromPosition + gotSoFar);
            blobGetBytesCall.setIntX(4, forLength - gotSoFar);
            try {
                blobGetBytesCall.executeX();
            } catch (SqlException sqle) {
                sqle = handleInvalidLocator(sqle);
                throw sqle;
            }
            byte[] result = blobGetBytesCall.getBytesX(1);
            
            if (gotSoFar == 0) {  // First round of reading
                if (result.length == forLength) {  // Got everything
                    return result;
                } else {
                    // Blob is probably greater than MAX VARCHAR length, need to
                    // read in parts, create array for putting pieces together
                    retVal = new byte[forLength];
                }
            }
           
            // If not able to read more, stop
            if (result.length == 0) break;
            
            System.arraycopy(result, 0,
                             retVal, gotSoFar, result.length);
            gotSoFar += result.length;
        }
        return retVal;
    }

    /**
     * Writes all or part of the given <code>byte</code> array to the
     * <code>BLOB</code> value designated by <code>sourceLocator</code>.
     * Writing starts at position <code>fromPosition</code> in the
     * <code>BLOB</code> value; <code>forLength</code> bytes from the given
     * byte array are written. If the end of the <code>Blob</code> value is
     * reached while writing the array of bytes, then the length of the
     * <code>Blob</code> value will be increased to accomodate the extra bytes.
     * <p>
     * If <code>forLength</code> is larger than the maximum length of a VARCHAR
     * FOR BIT DATA, the writing to the BLOB value will be split into repeated
     * procedure calls.
     *
     * @param sourceLocator locator that identifies the Blob to operated on
     * @param fromPosition the position in the <code>BLOB</code> value at which
     *        to start writing; the first position is 1
     * @param forLength the number of bytes to be written to the
     *        <code>BLOB</code> value from the array of bytes
     *        <code>bytes</code>.  Specifying a length that goes beyond the end
     *        of the BLOB (i.e., <code>fromPosition + forLength >
     *        blob.length()</code>, will result in an error.
     * @param bytes the array of bytes to be written
     * @throws org.apache.derby.client.am.SqlException 
     */
    void blobSetBytes(int sourceLocator, 
                      long fromPosition, 
                      int forLength, 
                      byte[] bytes) throws SqlException
    {
        if (blobSetBytesCall == null) {
            blobSetBytesCall = connection.prepareCallX
                ("CALL SYSIBM.BLOBSETBYTES(?, ?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Make sure this statement does not commit user transaction
            blobSetBytesCall.isAutoCommittableStatement_ = false;
        }

        int sentSoFar = 0;
        byte[] bytesToBeSent = bytes;
        while (sentSoFar < forLength) {
            // Only send what can fit in a VARCHAR FOR BIT DATA parameter
            int numBytesThisRound 
                = Math.min(forLength - sentSoFar, VARCHAR_MAXWIDTH);
            if (numBytesThisRound != bytesToBeSent.length) {
                // Need an array that contains just what is to be sent
                bytesToBeSent = new byte[numBytesThisRound];                
            }
            if (bytesToBeSent != bytes) {
                // Need to copy from original array
                System.arraycopy(bytes, sentSoFar,
                                 bytesToBeSent, 0, numBytesThisRound);
            }
            
            blobSetBytesCall.setIntX(1, sourceLocator);
            blobSetBytesCall.setLongX(2, fromPosition + sentSoFar);
            blobSetBytesCall.setIntX(3, numBytesThisRound);
            blobSetBytesCall.setBytesX(4, bytesToBeSent);
            try {
                blobSetBytesCall.executeX();
            } catch (SqlException sqle) {
                sqle = handleInvalidLocator(sqle);
                throw sqle;
            }
            
            sentSoFar += numBytesThisRound;
        }
    }

    /**
     * Truncates the <code>BLOB</code> value identified by
     * <code>sourceLocator</code> to be <code>length</code> bytes.  
     * <p>
     * <b>Note:</b> If the value specified for <code>length</code> is greater
     * than the length+1 of the <code>BLOB</code> value then an
     * <code>SqlException</code> will be thrown.
     * 
     * @param sourceLocator locator identifying the Blob to be truncated
     * @param length the length, in bytes, to which the <code>BLOB</code> value
     *        should be truncated
     * @throws org.apache.derby.client.am.SqlException 
     */
    void blobTruncate(int sourceLocator, long length) throws SqlException
    {
        if (blobTruncateCall == null) {
            blobTruncateCall = connection.prepareCallX
                ("CALL SYSIBM.BLOBTRUNCATE(?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Make sure this statement does not commit user transaction
            blobTruncateCall.isAutoCommittableStatement_ = false;
        }

        blobTruncateCall.setIntX(1, sourceLocator);
        blobTruncateCall.setLongX(2, length);
        try {
            blobTruncateCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
    }

    /**
     * Allocates an empty CLOB on server and returns its locator. Any
     * subsequent operations on this CLOB value will be stored in temporary
     * space on the server.
     * 
     * @throws org.apache.derby.client.am.SqlException 
     * @return locator that identifies the created CLOB.
     */
    int clobCreateLocator() throws SqlException
    {
        //The information on whether the locator support
        //is available is cached in the boolean
        //isLocatorSupportAvailable. If this is false
        //we can return -1
        if (!isLocatorSupportAvailable) {
            return INVALID_LOCATOR;
        }
        
        try {
            if (clobCreateLocatorCall == null) {
                clobCreateLocatorCall = connection.prepareCallX
                        ("? = CALL SYSIBM.CLOBCREATELOCATOR()",
                        java.sql.ResultSet.TYPE_FORWARD_ONLY,
                        java.sql.ResultSet.CONCUR_READ_ONLY,
                        java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
                clobCreateLocatorCall
                        .registerOutParameterX(1, java.sql.Types.INTEGER);
                // Make sure this statement does not commit user transaction
                clobCreateLocatorCall.isAutoCommittableStatement_ = false;
            }
            clobCreateLocatorCall.executeX();
        }
        catch(SqlException sqle) {
            //An exception has occurred while calling the stored procedure
            //used to create the locator value. 
            
            //We verify to see if this SqlException has a SQLState of
            //42Y03(SQLState.LANG_NO_SUCH_METHOD_ALIAS)
            //(corresponding to the stored procedure not being found)
            
            //This means that locator support is not available. 
            
            //This information is cached so that each time to determine
            //if locator support is available we do not have to make a
            //round trip to the server.
            if (sqle.getSQLState().compareTo
                    (ExceptionUtil.getSQLStateFromIdentifier
                    (SQLState.LANG_NO_SUCH_METHOD_ALIAS)) == 0) {
                isLocatorSupportAvailable = false;
                return INVALID_LOCATOR;
            }
            else {
                //The SqlException has not occurred because of the
                //stored procedure not being found. Hence we simply throw
                //it back.
                throw sqle;
            }
        }
        
        return clobCreateLocatorCall.getIntX(1);
    }

    /**
     * This method frees the CLOB and releases the resources that it
     * holds. (E.g., temporary space used to store this CLOB on the server.)
     * @param locator locator that designates the CLOB to be released.
     * @throws org.apache.derby.client.am.SqlException 
     */
    void clobReleaseLocator(int locator) throws SqlException
    {
        if (clobReleaseLocatorCall == null) {
            clobReleaseLocatorCall = connection.prepareCallX
                ("CALL SYSIBM.CLOBRELEASELOCATOR(?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Make sure this statement does not commit user transaction
            clobReleaseLocatorCall.isAutoCommittableStatement_ = false;
        }

        clobReleaseLocatorCall.setIntX(1, locator);
        try {
            clobReleaseLocatorCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
    }


    /**
     * Retrieves the character position at which the specified substring 
     * <code>searchLiteral</code> begins within the <code>CLOB</code> value
     * identified by <code>locator</code>.  The search for
     * <code>searchLiteral</code> begins at position <code>fromPosition</code>.
     * <p>
     * If <code>searchLiteral</code> is longer than the maximum length of a
     * VARCHAR, it will be split into smaller fragments, and
     * repeated procedure calls will be made to perform the entire search
     *
     * @param locator locator that identifies the CLOB to be searched.
     * @param searchLiteral the substring for which to search
     * @param fromPosition the position at which to begin searching; the
     *        first position is 1
     * @throws org.apache.derby.client.am.SqlException 
     * @return the position at which the pattern appears, else -1
     */
    long clobGetPositionFromString(int locator, 
                                   String searchLiteral, 
                                   long fromPosition) throws SqlException
    {
        long clobLength = -1;  // Will be fetched from server if needed
        int patternLength = searchLiteral.length();
        do {
            long foundAt = clobGetPositionFromString(locator, 
                                                     fromPosition,
                                                     searchLiteral, 
                                                     0,
                                                     VARCHAR_MAXWIDTH);

            // If searchLiteral is longer than VARCHAR_MAXWIDTH, 
            // we need to check the rest
            boolean tryAgain = false;
            if ((patternLength > VARCHAR_MAXWIDTH) && (foundAt > 0)) {
                // First part of searchLiteral matched, check rest
                int comparedSoFar = VARCHAR_MAXWIDTH;
                while (comparedSoFar < patternLength) {
                    int numCharsThisRound 
                        = Math.min(patternLength - comparedSoFar, 
                                   VARCHAR_MAXWIDTH);
                    long pos = clobGetPositionFromString(locator,
                                                         foundAt+comparedSoFar,
                                                         searchLiteral,
                                                         comparedSoFar,
                                                         numCharsThisRound);

                    if (pos != (foundAt + comparedSoFar)) { 
                        // This part did not match
                        // Try to find a later match for the same prefix
                        tryAgain = true;
                        fromPosition = foundAt + 1;
                        break;
                    }

                    comparedSoFar += numCharsThisRound;
                }
            }
            
            if (!tryAgain) return foundAt;

            // Need Clob length in order to determine when to stop
            if (clobLength < 0) {  
                clobLength = clobGetLength(locator);
            }
        } while (fromPosition + patternLength <= clobLength);

        return -1;  // No match
    }

    /**
     * 
     * Retrieves the character position at which the specified part of the
     * substring <code>searchLiteral</code> begins within the <code>CLOB</code>
     * value identified by <code>locator</code>.  The search for
     * <code>searchLiteral</code> begins at position <code>fromPosition</code>.
     * <p> 
     * This is a helper function used by clobGetPositionFromString(int,
     * String, long) for each call to the CLOBGETPOSITIONFROMSTRING procedure.
     *
     * @param locator locator that identifies the CLOB to be searched.
     * @param searchLiteral the substring for which to search
     * @param fromPosition the position at which to begin searching; the
     *        first position is 1
     * @param offset the offset into the string <code>searchLiteral</code> at
     *        which the pattern to search for starts
     * @param length the number of characters from the string
     *        <code>searchLiteral</code> to use for the pattern to search
     *        for. It is assumed that this length is smaller than the maximum
     *        size of a VARCHAR column.  Otherwise, an exception will be
     *        thrown.
     * @throws org.apache.derby.client.am.SqlException 
     * @return the position at which the pattern appears, else -1
     */
    private long clobGetPositionFromString(int locator, 
                                           long fromPosition,
                                           String searchLiteral, 
                                           int offset,
                                           int length) throws SqlException
    {
        if (clobGetPositionFromStringCall == null) {
            clobGetPositionFromStringCall = connection.prepareCallX
                ("? = CALL SYSIBM.CLOBGETPOSITIONFROMSTRING(?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            clobGetPositionFromStringCall
                .registerOutParameterX(1, java.sql.Types.BIGINT);
            // Make sure this statement does not commit user transaction
            clobGetPositionFromStringCall.isAutoCommittableStatement_ = false;
        }

        String stringToBeCompared = searchLiteral;
        int numChars = Math.min(searchLiteral.length() - offset, length);
        if (numChars != stringToBeCompared.length()) {
            // Need a String that contains just what is to be sent
            stringToBeCompared 
                = searchLiteral.substring(offset, offset + numChars);
        }

        clobGetPositionFromStringCall.setIntX(2, locator);
        clobGetPositionFromStringCall.setStringX(3, stringToBeCompared);
        clobGetPositionFromStringCall.setLongX(4, fromPosition);
        try {
            clobGetPositionFromStringCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
        return clobGetPositionFromStringCall.getLongX(1);
    }

    /**
     * Retrieves the character position in the CLOB value designated by this
     * <code>locator</code> at which substring given by
     * <code>searchLocator</code> begins. The search begins at position
     * <code>fromPosition</code>.
     * @param locator locator that identifies the CLOB to be searched.
     * @param searchLocator locator designating the CLOB value for which to
     *        search
     * @param fromPosition the position in the CLOB value
     *        at which to begin searching; the first position is 1
     * @throws org.apache.derby.client.am.SqlException 
     * @return the position at which the pattern begins, else -1
     */
    long clobGetPositionFromLocator(int locator, 
                                    int searchLocator, 
                                    long fromPosition) throws SqlException
    {
        if (clobGetPositionFromLocatorCall == null) {
            clobGetPositionFromLocatorCall = connection.prepareCallX
                ("? = CALL SYSIBM.CLOBGETPOSITIONFROMLOCATOR(?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            clobGetPositionFromLocatorCall
                .registerOutParameterX(1, java.sql.Types.BIGINT);
            // Make sure this statement does not commit user transaction
            clobGetPositionFromLocatorCall.isAutoCommittableStatement_ = false;
        }

        clobGetPositionFromLocatorCall.setIntX(2, locator);
        clobGetPositionFromLocatorCall.setIntX(3, searchLocator);
        clobGetPositionFromLocatorCall.setLongX(4, fromPosition);
        try {
            clobGetPositionFromLocatorCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
        return clobGetPositionFromLocatorCall.getLongX(1);
    }

    /**
     * Returns the number of character in the <code>CLOB</code> value
     * designated by this <code>sourceLocator</code>.
     * 
     * @param sourceLocator locator that identifies the CLOB
     * @throws org.apache.derby.client.am.SqlException 
     * @return length of the <code>CLOB</code> in characters 
     */
    long clobGetLength(int sourceLocator) throws SqlException
    {
        if (clobGetLengthCall == null) {
            clobGetLengthCall = connection.prepareCallX
                ("? = CALL SYSIBM.CLOBGETLENGTH(?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            clobGetLengthCall.registerOutParameterX(1, java.sql.Types.BIGINT);
            // Make sure this statement does not commit user transaction
            clobGetLengthCall.isAutoCommittableStatement_ = false;
        }

        clobGetLengthCall.setIntX(2, sourceLocator);
        try {
            clobGetLengthCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
        return clobGetLengthCall.getLongX(1);
    }

    /**
     * Retrieves all or part of the <code>CLOB</code> value that is identified
     * by <code>sourceLocator</code>, as a <code>String</code>.  This
     * <code>String</code> contains up to <code>forLength</code> consecutive
     * characters starting at position <code>fromPosition</code>.  
     * <p> 
     * If <code>forLength</code> is larger than the maximum length of a
     * VARCHAR, the reading of the CLOB will be split into repeated procedure
     * calls.
     *
     * @param sourceLocator locator that identifies the CLOB to operate on
     * @param fromPosition the ordinal position of the first character in the
     *        <code>CLOB</code> value to be extracted; the first character is
     *        at position 1
     * @param forLength the number of consecutive characters to be copied; the
     *        value for length must be 0 or greater.  Specifying a length that
     *        goes beyond the end of the CLOB (i.e., <code>fromPosition +
     *        forLength > clob.length()</code>, will result in an error.
     * @throws org.apache.derby.client.am.SqlException 
     * @return a string containing up to <code>forLength</code> consecutive
     *         characters from the <code>CLOB</code> value designated by
     *         <code>sourceLocator</code>, starting with the character at
     *         position <code>fromPosition</code>
     */
    String clobGetSubString(int sourceLocator, long fromPosition, int forLength)
        throws SqlException
    {
        if (forLength == 0) return "";

        if (clobGetSubStringCall == null) {
            clobGetSubStringCall = connection.prepareCallX
                ("? = CALL SYSIBM.CLOBGETSUBSTRING(?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            clobGetSubStringCall
                .registerOutParameterX(1, java.sql.Types.VARCHAR);
            // Make sure this statement does not commit user transaction
            clobGetSubStringCall.isAutoCommittableStatement_ = false;
        }

        StringBuffer retVal = null;
        int gotSoFar = 0;
        while (gotSoFar < forLength) {
            clobGetSubStringCall.setIntX(2, sourceLocator);
            clobGetSubStringCall.setLongX(3, fromPosition + gotSoFar);
            clobGetSubStringCall.setIntX(4, forLength - gotSoFar);
            try {
                clobGetSubStringCall.executeX();
            } catch (SqlException sqle) {
                sqle = handleInvalidLocator(sqle);
                throw sqle;
            }
            String result =  clobGetSubStringCall.getStringX(1);

            if (gotSoFar == 0) {  // First round of reading
                if (result.length() == forLength) {  // Got everything
                    return result;
                } else {
                    // Clob is probably greater than MAX VARCHAR length, 
                    // need to read it in parts, 
                    // create StringBuffer for putting pieces together
                    retVal = new StringBuffer(forLength);
                }
            }
           
            // If not able to read more, stop
            if (result.length() == 0) break;
            
            retVal.append(result);
            gotSoFar += result.length();
        }
        return retVal.toString();
    }

    /**
     * Writes all or part of the given <code>String</code> to the
     * <code>CLOB</code> value designated by <code>sourceLocator</code>.
     * Writing starts at position <code>fromPosition</code> in the
     * <code>CLOB</code> value; <code>forLength</code> characters from the
     * given string are written. If the end of the <code>Clob</code> value is
     * reached while writing the string, then the length of the
     * <code>Clob</code> value will be increased to accomodate the extra
     * characters.
     * <p> 
     * If <code>forLength</code> is larger than the maximum length of a
     * VARCHAR, the writing to the CLOB value will be split into repeated
     * procedure calls.
     *
     * @param sourceLocator locator that identifies the Clob to operated on
     * @param fromPosition the position in the <code>CLOB</code> value at which
     *        to start writing; the first position is 1
     * @param forLength the number of characters to be written to the
     *        <code>CLOB</code> value from the string <code>string</code>.
     *        Specifying a length that goes beyond the end of the CLOB (i.e.,
     *        <code>fromPosition + forLength > clob.length()</code>, will
     *        result in an error.
     * @param string the string to be written
     * @throws org.apache.derby.client.am.SqlException 
     */
    void clobSetString(int sourceLocator, 
                       long fromPosition, 
                       int forLength, 
                       String string) throws SqlException
    {
        if (clobSetStringCall == null) {
            clobSetStringCall = connection.prepareCallX
                ("CALL SYSIBM.CLOBSETSTRING(?, ?, ?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Make sure this statement does not commit user transaction
            clobSetStringCall.isAutoCommittableStatement_ = false;
        }

        int sentSoFar = 0;
        String stringToBeSent = string;
        while (sentSoFar < forLength) {
            // Only send what can fit in a VARCHAR parameter
            int numCharsThisRound 
                = Math.min(forLength - sentSoFar, VARCHAR_MAXWIDTH);
            if (numCharsThisRound < string.length()) {
                // Need a String that contains just what is to be sent
                stringToBeSent 
                    = string.substring(sentSoFar, sentSoFar+numCharsThisRound);
            }
            
            clobSetStringCall.setIntX(1, sourceLocator);
            clobSetStringCall.setLongX(2, fromPosition + sentSoFar);
            clobSetStringCall.setIntX(3, numCharsThisRound);
            clobSetStringCall.setStringX(4, stringToBeSent);
            try {
                clobSetStringCall.executeX();
            } catch (SqlException sqle) {
                sqle = handleInvalidLocator(sqle);
                throw sqle;
            }

            sentSoFar += numCharsThisRound;
        }
    }

    /**
     * Truncates the <code>CLOB</code> value identified by
     * <code>sourceLocator</code> to be <code>length</code> characters.  
     * <p>
     * <b>Note:</b> If the value specified for <code>length</code> is greater
     * than the length+1 of the <code>CLOB</code> value then an
     * <code>SqlException</code> will be thrown.
     * 
     * @param sourceLocator locator identifying the Clob to be truncated
     * @param length the length, in characters, to which the <code>CLOB</code>
     *        value should be truncated
     * @throws org.apache.derby.client.am.SqlException 
     */
    void clobTruncate(int sourceLocator, long length) throws SqlException
    {
        if (clobTruncateCall == null) {
            clobTruncateCall = connection.prepareCallX
                ("CALL SYSIBM.CLOBTRUNCATE(?, ?)",
                 java.sql.ResultSet.TYPE_FORWARD_ONLY, 
                 java.sql.ResultSet.CONCUR_READ_ONLY, 
                 java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Make sure this statement does not commit user transaction
            clobTruncateCall.isAutoCommittableStatement_ = false;
        }

        clobTruncateCall.setIntX(1, sourceLocator);
        clobTruncateCall.setLongX(2, length);
        try {
            clobTruncateCall.executeX();
        } catch (SqlException sqle) {
            sqle = handleInvalidLocator(sqle);
            throw sqle;
        }
    }

    /**
     * If the given exception indicates that locator was not valid, we
     * assume the locator has been garbage-collected due to
     * transaction commit, and wrap the exception in an exception with
     * SQL state <code>LOB_OBJECT_INVALID</code>.
     * @param sqle Exception to be checked
     * @return If <code>sqle</code> indicates that locator was
     *         invalid, an <code>SqlException</code> with SQL state
     *         <code>LOB_OBJECT_INVALID</code>. Otherwise, the
     *         incoming exception is returned.
     */
    private SqlException handleInvalidLocator(SqlException sqle)
    {
        SqlException ex = sqle;
        while (ex != null) {
            if (ex.getSQLState().compareTo
                (ExceptionUtil.getSQLStateFromIdentifier
                 (SQLState.LOB_LOCATOR_INVALID)) == 0) {
                return new SqlException(connection.agent_.logWriter_,
                               new ClientMessageId(SQLState.LOB_OBJECT_INVALID),
                               null,
                               sqle);
            }
            ex = ex.getNextException();
        }

        // LOB_LOCATOR_INVALID not found, return original exception
        return sqle;
    }
}
