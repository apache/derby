/*

   Derby - Class org.apache.derby.diag.ErrorMessages

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.diag;

import java.util.Hashtable;
import java.util.Enumeration;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;
import java.lang.Math;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.DB2Limit;
import org.apache.derby.iapi.util.StringUtil;

import org.apache.derby.vti.VTITemplate;
import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import org.apache.derby.impl.jdbc.EmbedResultSetMetaData;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;


/** 
 * ErrorMessage is a VTI that shows all the SQLStates, locale-sensitive error
 * messages, and exception severities for a database.
 * 
 * <p>To use it, query it as an external virtual table:</p>
 * <PRE> SELECT* FROM NEW ErrorMessages() AS EQ; </PRE>
 * <P>The ErrorMessages VTI has the following columns:
 * <UL><LI>SQL_STATE--VARCHAR(5) - nullable.  The SQLState of the SQLException.<br>
 * (The code returned by getSQLState() in SQLException.)</LI>
 * <LI>MESSAGE--VARCHAR(32672) - nullable.  The error message<br>
 * (The code returned by getMessage() in SQLException.)</LI>
 * <LI>SEVERITY--INTEGER - nullable.  The Cloudscape code for the severity.<br>
 * (The code returned by getErrorCode() in SQLException.)</LI>
 * </UL>
 * 
 */
public class ErrorMessages extends VTITemplate implements VTICosting, java.security.PrivilegedAction  {
	
	/* The name of the file containing all the SQLSTate codes.
	 * The class gets the SQLState code from the messages
	 * file (messages_en.properties). Then it uses StandardException to get
	 * the exception severities and locale-sensitive error messages.
	 */
	

        /**          */
	private Properties p;
        /**          */
	private Enumeration keys;
        /**          */
	private String k;
        /**          */
	private String SQLState;
        /**          */
	private String message;
        /**          */
	private int severity;
	

        /**          */
	public ErrorMessages() throws IOException{
		
		loadProperties();
	}

        /**          * 
         * @see java.sql.ResultSet#next
         */
	public boolean next() {
		boolean retCode = true;

		if (!keys.hasMoreElements()) {
			close();
			retCode = false;
			return retCode;
			
		}

		k = (String)keys.nextElement();

		if (notAnException()) {
			retCode = next();
		}

		if (retCode) {
		  SQLState =StandardException.getSQLStateFromIdentifier(k);
		  message = MessageService.getTextMessage(k);
		  message = StringUtil.truncate(message, DB2Limit.DB2_VARCHAR_MAXWIDTH);
		}
		return retCode;
	}
        /**          * 
         * @see java.sql.ResultSet#close
         */
	public void close() {
		p = null;
		k = null;
		keys = null;
	}
        /**          * 
         * @see java.sql.ResultSet#getMetaData
         */
	public ResultSetMetaData getMetaData() {
		return metadata;
	}

    /**      * 
     * @exception SQLException column at index is not found
     * @see java.sql.ResultSet#getString
     */
    public String getString(int columnIndex) throws SQLException {
		switch (columnIndex) {
		case 1: return SQLState;
		case 2: return message;
		default: return super.getString(columnIndex); // throw an exception
		}
	}
    /**      * 
     * @exception SQLException column at index is not found
     * @see java.sql.ResultSet#getInt
     */
    public int getInt(int columnIndex) throws SQLException {
		switch (columnIndex) {
		case 3: return severity;
		default: return super.getInt(columnIndex); // throw an exception
		}
	}
	
	
        /**          */
	private void loadProperties() throws IOException
	{
		p = new Properties();
		for (int i = 0; i < 50; i++) {
			msgFile = i;
			InputStream is = (InputStream) java.security.AccessController.doPrivileged(this);
			if (is == null)
				continue;

			try {
				p.load(is);
			} finally {
				try {
					is.close();
				} catch (IOException ioe) {
				}
			}
		}
		keys = p.keys();
	}

        /**          */
	private boolean notAnException() {

		if (k.length() < 5)
			return true;
        int tempSeverity = StandardException.getSeverityFromIdentifier(k);
		//if the severity is not one of our customer-visible severity
		//levels, it's just a message, not an SQLException
        if (tempSeverity < (ExceptionSeverity.NO_APPLICABLE_SEVERITY + 1))
	  return true;
	severity = tempSeverity;	
	return false;
	}

		
	/*VTICosting methods*/
		

        /**          */
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
	{
		return 1000;
	}

        /**          */
	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
	{
		return 5000;
	}

        /**          */
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
	{
		return true;
	}

	private int msgFile;
	
	public final Object run() {
		InputStream msg = getClass().getResourceAsStream("/org/apache/derby/loc/m" + msgFile + "_en.properties");
		msgFile = 0;
		return msg;

	}

	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor("SQL_STATE",  Types.VARCHAR, true, 5),
		EmbedResultSetMetaData.getResultColumnDescriptor("MESSAGE",    Types.VARCHAR, true, DB2Limit.DB2_VARCHAR_MAXWIDTH),
		EmbedResultSetMetaData.getResultColumnDescriptor("SEVERITY",   Types.INTEGER, true),
	};
	
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);

}
