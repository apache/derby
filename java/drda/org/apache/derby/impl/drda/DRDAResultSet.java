/*

   Derby - Class org.apache.derby.impl.drda.DRDAResultSet

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

package org.apache.derby.impl.drda;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import org.apache.derby.iapi.jdbc.EngineResultSet;

/**
    DRDAResultSet holds result set information
*/
class DRDAResultSet
{
    //NOTE!
    //
    // Since DRDAResultSets are reused, ALL variables should be set 
    // to their default values in reset().

    // resultSet states are NOT_OPENED and SUSPENDED
    protected static final int NOT_OPENED = 1;
    protected static final int SUSPENDED = 2;
    public static final int QRYCLSIMP_DEFAULT = CodePoint.QRYCLSIMP_NO;  
    
    boolean explicitlyClosed = false;

    int state;
    protected boolean hasdata = true;
    protected int[] rsLens;             // result length for each column
    private int[] rsDRDATypes;          // DRDA Types of the result set columns
    private int[] rsPrecision;         // result precision for Decimal types
    private int[] rsScale;              // result sale for Decimal types

    protected int [] outovr_drdaType;   // Output override DRDA type and length

    protected int withHoldCursor;           // hold cursor after commit attribute
    protected int scrollType = ResultSet.TYPE_FORWARD_ONLY;         // Sensitive or Insensitive scroll attribute
    protected int concurType;           // Concurency type
    protected long rowCount;            // Number of rows we have processed
    private ResultSet rs;              // Current ResultSet

    protected int blksize;              // Query block size
    protected int maxblkext;            // Maximum number of extra blocks
    protected int outovropt;            // Output Override option
    protected int qryclsimp;            // Implicit Query Close Setting
    protected boolean qryrelscr;        // Query relative scrolling
    protected long qryrownbr;           // Query row number
    protected boolean qryrfrtbl;        // Query refresh answer set table
    protected int qryscrorn;            // Query scroll orientation
    protected boolean qryrowsns;        // Query row sensitivity
    protected boolean qryblkrst;        // Query block reset
    protected boolean qryrtndta;        // Query returns data
    protected int qryrowset;            // Query row set
    private   int qryprctyp;            // Protocol type
    private   boolean gotPrctyp;        // save the result, for performance
    protected int rtnextdta;            // Return of EXTDTA option
    protected int nbrrow;              // number of fetch or insert rows
    protected byte [] rslsetflg;        // Result Set Flags

    /** List of Blobs and Clobs. Return values to send with extdta objects. */
    private ArrayList<Object> extDtaObjects;
    
    private ArrayList<Integer> rsExtPositions;

    protected ConsistencyToken pkgcnstkn; // Unique consistency token for ResultSet 0

    // splitQRYDTA is normally null. If it is non-null, it means that
    // the last QRYDTA response which was sent for this statement was
    // split according to the LMTBLKPRC protocol, and this array contains
    // the bytes that didn't fit. These bytes should be the first bytes
    // emitted in the next QRYDTA response to a CNTQRY request.
    private byte []splitQRYDTA;

    DRDAResultSet()
    {
        state = NOT_OPENED;
        // Initialize qryclsimp to NO. Only result sets requested by
        // an OPNQRY command should be implicitly closed. OPNQRY will
        // set qryclsimp later in setOPNQRYOptions().
        qryclsimp = CodePoint.QRYCLSIMP_NO;
    }

    /**
     * Set result set and initialize type array.
     *
     * @param value
     * 
     */

    void setResultSet(ResultSet value) throws SQLException
    {
        rs = value;
        gotPrctyp = false;
        int numCols= rs.getMetaData().getColumnCount();
        rsDRDATypes = new int[numCols];
        explicitlyClosed = false;
    }


    /**
     * set consistency token for this resultSet
     *
     */
    protected void setPkgcnstkn(ConsistencyToken pkgcnstkn)
    {
        this.pkgcnstkn = pkgcnstkn;
    }


    /**
     * 
     *  @return the underlying java.sql.ResultSet
     */
    protected ResultSet getResultSet()
    {
        return rs;
    }

    public void setSplitQRYDTA(byte []data)
    {
        splitQRYDTA = data;
    }
    public byte[]getSplitQRYDTA()
    {
        return splitQRYDTA;
    }

    /**
     *@return ResultSet DRDA DataTypes
     **/

    protected int[] getRsDRDATypes()
    {
        // use the given override if it is present
        if (outovr_drdaType != null)
            return outovr_drdaType;
        return rsDRDATypes;
    }

    /**
     * set resultset/out parameter precision
     *
     * @param index - starting with 1
     * @param precision
     */
    protected void setRsPrecision(int index, int precision)
    {
        if (rsPrecision == null)
            rsPrecision = new int[rsDRDATypes.length];
        rsPrecision[index -1] = precision;
    }

    /**
     * get resultset /out parameter precision
     * @param index -starting with 1
     * @return precision of column
     */
    protected int getRsPrecision(int index)
    {
        if (rsPrecision == null)
            return 0;
        return rsPrecision[index-1];
    }

    /**
     * set resultset/out parameter scale
     *
     * @param index - starting with 1
     * @param scale
     */
    protected void setRsScale(int index, int scale)
    {
        if (rsScale == null)
            rsScale = new int[rsDRDATypes.length];
        rsScale[index-1] = scale;
    }

    /**
     * get resultset /out parameter scale
     * @param index -starting with 1
     * @return scale of column
     */
    protected int  getRsScale(int index)
    {
        if (rsScale == null)
            return 0;
        
        return rsScale[index -1];
    }
    
    
    /**
     * set resultset/out parameter DRDAType
     *
     * @param index - starting with 1
     * @param type
     */
    protected  void setRsDRDAType(int index, int type)
    {
        rsDRDATypes[index -1] =  type;
        
    }
    
    /**
     * get  resultset/out parameter DRDAType
     *
     * @param index - starting with 1
     * @return  DRDA Type of column
     */
    protected int getRsDRDAType(int index)
    {
        if ((outovr_drdaType != null) && (outovr_drdaType[index-1] != 0)) {
            // Override with requested type.  0 means use default
            return outovr_drdaType[index-1];
        }
        return rsDRDATypes[index -1];
    }
    
    
    /**
     * get  resultset  DRDALen
     * @param index - starting with 1
     * @return  length of column value
     */
    protected int getRsLen(int index)
    {
        return rsLens[index -1];
    }
    

    /**
     * Add extDtaObject
     * @param o - object to  add
     */
    protected void  addExtDtaObject (Object o, int jdbcIndex )
    {
        if (extDtaObjects == null)
            extDtaObjects = new java.util.ArrayList<Object>();
        extDtaObjects.add (o);

        if (rsExtPositions == null)
            rsExtPositions = new java.util.ArrayList<Integer>();
        
        // need to record the 0 based position so subtract 1
        rsExtPositions.add(jdbcIndex - 1);

    }


    /**
     * Clear externalized lob objects in current result set
     */
    protected void  clearExtDtaObjects ()
    {
        if (extDtaObjects != null)
            extDtaObjects.clear();
        if (rsExtPositions != null)
            rsExtPositions.clear();
        
    }
    
    /**
     * Is lob object nullable
     * @param index - offset starting with 0
     * @return true if object is nullable
     */
    protected boolean isExtDtaValueNullable(int index)
    {
        if ((rsExtPositions == null) || 
            rsExtPositions.get(index) == null)
            return false;
        

        // Column number is starting on 1
        int colnum = ((Integer) rsExtPositions.get(index)).intValue() + 1;

        // if there is no type information, then we represent a CallableStatement
        // and all parameters are nullable
        if ( rsDRDATypes == null ) { return true; }
        else if (FdocaConstants.isNullable(getRsDRDAType(colnum)))
            return true;
        else 
            return false;
    }
    

    /**
     * Get the extData Objects
     *
     *  @return ArrayList with extdta
     */
    protected ArrayList<Object> getExtDtaObjects()
    {
        return extDtaObjects;
    }

    /**
     * This method closes the JDBC objects and frees up all references held by
     * this object.
     * 
     * @throws SQLException
     */
    protected void close()  throws SQLException
    {
        if (rs != null)
            rs.close();
        rs = null;
        outovr_drdaType = null;
        rsLens = null;
        rsDRDATypes = null;
        rsPrecision = null;
        rsScale = null;
        extDtaObjects = null;
        splitQRYDTA = null;
        rsExtPositions = null;
    }
    
    /**
     * This method resets the state of this DRDAResultset object so that it can
     * be re-used. This method should reset all variables of this class.
     * 
     */
    protected void reset() {
        explicitlyClosed = false;
        state = NOT_OPENED;
        hasdata = true;
        rsLens = null;
        rsDRDATypes = null;
        rsPrecision = null;
        rsScale = null;
        
        outovr_drdaType = null;
        
        withHoldCursor = 0;
        scrollType = ResultSet.TYPE_FORWARD_ONLY;
        concurType = 0;
        rowCount = 0;
        rs = null;
        
        blksize = 0;
        maxblkext = 0;
        outovropt = 0;
        qryclsimp = CodePoint.QRYCLSIMP_NO;
        qryrelscr = false;
        qryrownbr = 0;
        qryrfrtbl = false;
        qryscrorn = 0;
        qryrowsns = false; 
        qryblkrst = false;
        qryrtndta = false;
        qryrowset = 0;
        qryprctyp = 0;
        gotPrctyp = false;
        rtnextdta = 0;
        nbrrow = 0;
        rslsetflg = null;

        extDtaObjects = null;
        rsExtPositions = null;
        pkgcnstkn = null;
        splitQRYDTA = null;
    }


    /**
     * Explicitly close the result set by CLSQRY
     * needed to check for double close.
     */
    protected void CLSQRY()
    {
        explicitlyClosed = true;
    }

    /* 
     * @return whether CLSQRY has been called on the
     *         current result set.
     */
    protected boolean wasExplicitlyClosed()
    {
        return explicitlyClosed;
    }


    /****
     * Check to see if the result set for this statement
     * has at least one column that is BLOB/CLOB.
     * @return True if the result has at least one blob/clob
     *  column; false otherwise.
     ****/
 
    protected boolean hasLobColumns() throws SQLException
    {
        ResultSetMetaData rsmd = rs.getMetaData();
        int ncols = rsmd.getColumnCount();
        for (int i = 1; i <= ncols; i++)
        {
            int type = rsmd.getColumnType(i);
            if (type == Types.BLOB || type == Types.CLOB)
                return true;
        }
        return false;
    }

    /**
     * Get the cursor name for the ResultSet
     */
    public String getResultSetCursorName() throws SQLException
    {

        if (rs != null)
            return rs.getCursorName();
        else 
            return null;
    }

    protected int getQryprctyp()
        throws SQLException
    {
        if (!gotPrctyp && qryprctyp == CodePoint.LMTBLKPRC)
        {
            gotPrctyp = true;
            if (rs == null || ((EngineResultSet)rs).isForUpdate() ||
                /* for now we are not supporting LOB under LMTBLKPRC.  drda spec only
                 * disallows LOB under LMTBLKPRC if OUTOVR is also for ANY CNTQRY reply.
                 * To support LOB, QRYDTA protocols for LOB will need to be changed.
                 */
                hasLobColumns())
            {
                qryprctyp = CodePoint.FIXROWPRC;
            }
        }
        return qryprctyp;
    }

    protected void setQryprctyp(int qryprctyp)
    {
        this.qryprctyp = qryprctyp;
    }

    /**
     * is ResultSet closed
     * @return whether the resultSet  is closed
     */
    protected boolean isClosed()
    {
        return (state == NOT_OPENED);
    }

    /**
     * Set state to SUSPENDED (result set is opened)
     */
    protected void suspend()
    {
        state = SUSPENDED;
    }


    protected String toDebugString(String indent)
    {
        String s = indent + "***** DRDASResultSet toDebugString ******\n";
        s += indent + "State:" + getStateString(state)+ "\n";
        s += indent + "pkgcnstkn: {" + pkgcnstkn + "}\n"; 
        s += indent + "cursor Name: ";
        String cursorName = null;
        try {
            if (rs != null)
                cursorName = rs.getCursorName();
        }
        catch (SQLException se )
        {
            cursorName = "invalid rs";
        }
        s += indent + cursorName + "\n";
           
        return s;
    }


    private String getStateString( int i )
    {
        switch (i)
        {
            case NOT_OPENED:
                return "NOT_OPENED";
            case SUSPENDED:
                return "SUSPENDED";
            default:
                return "UNKNOWN_STATE";
        }

    }
    
    /**
     * Sets the OPNQRYOptions. For more information on the meaning of these
     * values consult the DRDA Technical Standard document. 
     * 
     * @param blksize Query block Size
     * @param qryblkctl Use to set the query protocol type
     * @param maxblkext Maximum number of extra blocks
     * @param outovropt Output override option
     * @param qryrowset Query row set
     * @param qryclsimpl Implicit query close setting
     */
    protected void setOPNQRYOptions(int blksize, int qryblkctl,
            int maxblkext, int outovropt,int qryrowset,int qryclsimpl)
    {
        this.blksize = blksize;
        setQryprctyp(qryblkctl);
        this.maxblkext = maxblkext;
        this.outovropt = outovropt;
        this.qryrowset = qryrowset;
        this.qryclsimp = (qryclsimpl == CodePoint.QRYCLSIMP_SERVER_CHOICE)
            ? DRDAResultSet.QRYCLSIMP_DEFAULT : qryclsimpl;

        // Assume that we are returning data until a CNTQRY command
        // tells us otherwise. (DERBY-822)
        qryrtndta = true;

        // For scrollable result sets, we don't know the fetch
        // orientation until we get a CNTQRY command. Set orientation
        // and row number to make pre-fetching possible. (DERBY-822)
        qryscrorn = CodePoint.QRYSCRREL;
        qryrownbr = 1;
    }
}
