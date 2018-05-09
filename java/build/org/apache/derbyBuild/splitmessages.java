/*

   Derby - Class org.apache.derbyBuild.splitmessages

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

package org.apache.derbyBuild;

import java.io.*;
import java.util.*;

import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

public class splitmessages
{
	/**
		arg[0] is the destination directory
		arg[1] is the source file.
	*/

    private static final String LOCALE_STUB = "locale";
    
    /** 
     *  This is the list of message ids that are shared between
     *  the network client and the engine.  This is used to generate
     *  a set of 'shared' messages.  This avoids us having to maintain
     *  two separate message files.
     *
     *  NOTE: We already assume all message ids starting with XJ are shared.
     *  This covers 90% of the cases.  Only add ids here if you have a 
     *  message id that is not in the XJ class.
     */
    private static TreeSet<String> clientMessageIds = new TreeSet<String>();
    
    /**
     * Initialize the set of message ids that the network client will use.  
     * <p>
     * Note that all message ids that start with "XJ" are automatically added, 
     * these are just for message ids that have a different prefix.
     */
    static
    {
        // Add message ids that don't start with XJ here
        clientMessageIds.add(SQLState.NO_CURRENT_CONNECTION);
        clientMessageIds.add(SQLState.NOT_IMPLEMENTED);
        clientMessageIds.add(SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION);
        clientMessageIds.add(SQLState.XACT_SAVEPOINT_RELEASE_ROLLBACK_FAIL);
        clientMessageIds.add(SQLState.UNSUPPORTED_ENCODING);
        clientMessageIds.add(SQLState.LANG_FORMAT_EXCEPTION);
        clientMessageIds.add(SQLState.LANG_DATA_TYPE_GET_MISMATCH);
        clientMessageIds.add(SQLState.LANG_DATA_TYPE_SET_MISMATCH);
        clientMessageIds.add(SQLState.LANG_DATE_SYNTAX_EXCEPTION);
        clientMessageIds.add(SQLState.CHARACTER_CONVERTER_NOT_AVAILABLE);
        clientMessageIds.add(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE);
        clientMessageIds.add(SQLState.LANG_STATEMENT_CLOSED_NO_REASON);
        clientMessageIds.add(SQLState.LANG_INVALID_COLUMN_POSITION);
        clientMessageIds.add(SQLState.INVALID_COLUMN_NAME);
        clientMessageIds.add(SQLState.HOLDABLE_RESULT_SET_NOT_AVAILABLE);
        clientMessageIds.add(SQLState.LANG_NULL_INTO_NON_NULL);
        clientMessageIds.add(SQLState.JDBC_METHOD_NOT_IMPLEMENTED);
        clientMessageIds.add(SQLState.JDBC_METHOD_NOT_SUPPORTED_BY_SERVER);
        clientMessageIds.add(SQLState.DRDA_NO_AUTOCOMMIT_UNDER_XA);
        clientMessageIds.add(SQLState.DRDA_INVALID_XA_STATE_ON_COMMIT_OR_ROLLBACK);
        clientMessageIds.add(SQLState.HOLDABLE_RESULT_SET_NOT_AVAILABLE);
        clientMessageIds.add(SQLState.INVALID_RESULTSET_TYPE);
        clientMessageIds.add(SQLState.SCROLL_SENSITIVE_NOT_SUPPORTED);
        clientMessageIds.add(SQLState.UNABLE_TO_OBTAIN_MESSAGE_TEXT_FROM_SERVER );
        clientMessageIds.add(SQLState.NUMBER_OF_ROWS_TOO_LARGE_FOR_INT);
        clientMessageIds.add(SQLState.NOGETCONN_ON_CLOSED_POOLED_CONNECTION);
        clientMessageIds.add(SQLState.LOB_METHOD_ON_CLOSED_CONNECTION);
        clientMessageIds.add(SQLState.QUERY_NOT_QUALIFIED_FOR_UPDATABLE_RESULTSET);
        clientMessageIds.add(SQLState.MULTIPLE_RESULTS_ON_EXECUTE_QUERY);
        clientMessageIds.add(SQLState.USE_EXECUTE_UPDATE_WITH_NO_RESULTS);
        clientMessageIds.add(SQLState.LANG_INVALID_CALL_TO_EXECUTE_UPDATE);
        clientMessageIds.add(SQLState.LANG_CANT_INVALIDATE_OPEN_RESULT_SET);
        clientMessageIds.add(SQLState.YEAR_EXCEEDS_MAXIMUM);
        clientMessageIds.add(SQLState.LANG_INVALID_PARAM_POSITION);
        clientMessageIds.add(SQLState.LANG_MISSING_PARMS);
        clientMessageIds.add(SQLState.LANG_NO_CURRENT_ROW);
        clientMessageIds.add(SQLState.LANG_STREAM_RETRIEVED_ALREADY);
        clientMessageIds.add(SQLState.CONNECTION_FAILED_ON_RESET);
        clientMessageIds.add(SQLState.DECIMAL_TOO_MANY_DIGITS);
        clientMessageIds.add(SQLState.NUMERIC_OVERFLOW);
        clientMessageIds.add(SQLState.UNSUPPORTED_HOLDABILITY_PROPERTY);
        clientMessageIds.add(SQLState.CANCEL_NOT_SUPPORTED_BY_SERVER);
        clientMessageIds.add(SQLState.LANG_INVALID_CALL_STATEMENT);
        clientMessageIds.add(SQLState.LANG_INVALID_SQL_IN_BATCH);
        clientMessageIds.add(SQLState.LANG_RESULT_SET_NOT_OPEN);
        clientMessageIds.add(SQLState.CANT_CONVERT_UNICODE_TO_EBCDIC);
        clientMessageIds.add(SQLState.SECMECH_NOT_SUPPORTED);
        clientMessageIds.add(SQLState.DRDA_COMMAND_NOT_IMPLEMENTED);
        clientMessageIds.add(SQLState.DATA_TYPE_NOT_SUPPORTED);
        clientMessageIds.add(SQLState.JDBC_DRIVER_REGISTER);
        clientMessageIds.add(SQLState.NO_CURRENT_ROW);
        clientMessageIds.add(SQLState.LANG_IDENTIFIER_TOO_LONG);
        clientMessageIds.add(SQLState.DRDA_CURSOR_NOT_OPEN);
        clientMessageIds.add(SQLState.PROPERTY_UNSUPPORTED_CHANGE);
        clientMessageIds.add(SQLState.NET_INVALID_JDBC_TYPE_FOR_PARAM);
        clientMessageIds.add(SQLState.UNRECOGNIZED_JAVA_SQL_TYPE);
        clientMessageIds.add(SQLState.NET_UNRECOGNIZED_JDBC_TYPE);
        clientMessageIds.add(SQLState.NET_SQLCDTA_INVALID_FOR_RDBCOLID);
        clientMessageIds.add(SQLState.NET_SQLCDTA_INVALID_FOR_PKGID);
        clientMessageIds.add(SQLState.NET_PGNAMCSN_INVALID_AT_SQLAM);
        clientMessageIds.add(SQLState.NET_VCM_VCS_LENGTHS_INVALID);
        clientMessageIds.add(SQLState.LANG_STRING_TOO_LONG);
        clientMessageIds.add(SQLState.INVALID_COLUMN_ARRAY_LENGTH);
        clientMessageIds.add(SQLState.PROPERTY_INVALID_VALUE);
        clientMessageIds.add(SQLState.LANG_SUBSTR_START_ADDING_LEN_OUT_OF_RANGE);
        clientMessageIds.add(SQLState.LANG_CURSOR_NOT_FOUND);
    };

	public static void main(String[] args) throws Exception {

		Properties p = new Properties();
        int idx = 0;

		File engineClassesDir = new File(args[idx++]);
		File clientClassesDir = new File(args[idx++]);
		File localesDir = new File(args[idx++]);
		File source = new File(args[idx++]);
        
		String s = source.getName();
		// lose the suffix
		s = s.substring(0, s.lastIndexOf('.'));
		// now get the locale
		String locale = s.substring(s.indexOf('_'));

		boolean addBase = "_en".equals(locale);

        File localizedEngineMessagesDir = new File(localesDir, LOCALE_STUB + locale);
        localizedEngineMessagesDir.mkdir();

		InputStream is = new BufferedInputStream(new FileInputStream(source), 64 * 1024);

		p.load(is);
		is.close();

        
		Properties[] c = new Properties[50];
		for (int i = 0; i < 50; i++) {
			c[i] = new Properties();
		}
        
        Properties clientProps = new Properties();

        String clientPropsFileName = "clientmessages" + locale + ".properties";

		for (Enumeration e = p.keys(); e.hasMoreElements(); ) {
			String key = (String) e.nextElement();

			c[MessageService.hashString50(key)].put(key, p.getProperty(key));
            
            // If we have a match, add it to the list of client messages
            if ( isClientMessage(key) )
            {
                clientProps.put(key, p.getProperty(key));
            }
		}

		for (int i = 0; i < 50; i++) {
			if (c[i].size() == 0)
				continue;
			OutputStream fos = new BufferedOutputStream
              (
				new FileOutputStream
                (new File(localizedEngineMessagesDir, "m"+i+locale+".properties")),
                16 * 1024
               );
            
			c[i].store(fos, (String) null);
			fos.flush();
			fos.close();
            
			if (addBase) {
				// add duplicate english file as the base
				fos = new BufferedOutputStream(
					new FileOutputStream(new File(engineClassesDir, "m"+i+"_en.properties")), 16 * 1024);
				c[i].store(fos, (String) null);
				fos.flush();
				fos.close();
			}


		}
        
		System.out.println("split messages" + locale);

        // Save the client messages (the combination of what was already
        // there and what we added from the engine properties file) into
        // the Derby locales directory
        OutputStream clientOutStream = new BufferedOutputStream(
            new FileOutputStream(new File(clientClassesDir, clientPropsFileName)), 
            16 * 1024);

        clientProps.store(clientOutStream, (String)null);
        clientOutStream.flush();
        clientOutStream.close();
        
        if ( addBase )
        {
            // Save the English messages as the base
            clientOutStream = new BufferedOutputStream(
                new FileOutputStream(new File(clientClassesDir, "clientmessages.properties")), 
                16 * 1024);

            clientProps.store(clientOutStream, (String)null);
            clientOutStream.flush();
            clientOutStream.close();            
        }
        System.out.println("Copied client messages for " + locale);
	}
    
    /**
     * Determine if this is a message that the client is using
     *
     * There are some classes of ids that we assume are client messages
     * (see code below for the definitive list).
     *
     * All other shared message ids should be added to the static array
     * clientMessageIds, defined at the top of this class
     */
    static boolean isClientMessage(String messageId)
    {
        // Look for message ids that we assume are likely to be used
        // on the client.  These ones don't need to be explicitly added
        // to clientMessageIds
        if ( messageId.startsWith("XJ") || messageId.startsWith("J")  ||
             messageId.startsWith("XN") || messageId.startsWith("58") ||
             messageId.startsWith("57") || messageId.startsWith("08") ||
             messageId.startsWith( "XBD" ) )
        {
            return true;
        }
        
        if ( clientMessageIds.contains(messageId))
        {
            return true;
        }
        
        return false;
    }
}
