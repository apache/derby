/*
 
   Derby - Class org.apache.derby.shared.common.error;
 
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

package org.apache.derby.shared.common.error;

import java.lang.StringBuilder;
import java.math.BigDecimal;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLDataException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.apache.derby.shared.common.i18n.MessageService;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.shared.common.reference.SQLState;




public class MessageUtils 
{
    private static final Locale EN = new Locale("en", "US");
	public static final String SQLERRMC_MESSAGE_DELIMITER = new String(new char[] {(char)20,(char)20,(char)20});
	/** 
	 * Pointer to the application requester
     * for the session being serviced.
     */

    public static final int DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH = 2400;

	/** 
   	 * Delimiters for SQLERRMC values.
     * The token delimiter value will be used to parse the MessageId from the 
     * SQLERRMC in MessageService.getLocalizedMessage and the MessageId will be
     * used to retrive the localized message. If this delimiter value is changed
     * please make sure to make appropriate changes in
     * MessageService.getLocalizedMessage that gets called from 
     * SystemProcedures.SQLCAMESSAGE
     * <code>SQLERRMC_TOKEN_DELIMITER</code> separates message argument tokens 
     */
    public static String SQLERRMC_TOKEN_DELIMITER = new String(new char[] {(char)20});

    /**
     * <code>SQLERRMC_PREFORMATTED_MESSAGE_DELIMITER</code>, When full message text is 
     * sent for severe errors. This value separates the messages. 
     */
    private static String SQLERRMC_PREFORMATTED_MESSAGE_DELIMITER = "::";

    protected int supportedMessageParamLength() {
        return DB2_JCC_MAX_EXCEPTION_PARAM_LENGTH;
    }


    /*
     * Get messageId and arguments, messageId is necessary for us to look up
     * localized message from property file.  messageId was sent as the last
     * token in the sqlerrmc.
     * The last element of the returned array contains the MessageId field, that
     * must be extracted by the caller funtion, to get an array of arguments.
     */

    public static Object[] getArgs(String sqlState, String sqlerrmc) 
    {
    	String messageId = sqlState;    //use sqlState if we don't have messageId
        Object[] arguments = null;
        if (sqlerrmc != null && sqlerrmc.length() > 0)
        {
            char [] sqlerrmc_chars = sqlerrmc.toCharArray();
            int numArgs = 0, lastSepIdx = -1; // last separator index
            for (int i = 0; i < sqlerrmc_chars.length; i++)
            {
                if (sqlerrmc_chars[i] == 20)    // separator
                {
                    numArgs++;
                    lastSepIdx = i;
                }
            }
            if (numArgs == 0)
            {
                messageId = new String(sqlerrmc_chars); //no args, only messageId then
                arguments = new Object[1];
            }
            else
            {
                messageId = new String(sqlerrmc_chars, lastSepIdx+1, sqlerrmc_chars.length-lastSepIdx-1);
                arguments = new Object[numArgs + 1];
                for (int start = 0, arg = 0, i = 0; i < lastSepIdx + 1; i++)
                {
                    if (i == lastSepIdx || sqlerrmc_chars[i] == 20) // delimiter 
                    {
                        arguments[arg++] = new String(sqlerrmc_chars, start, i - start);
                        start = i + 1;
                    }
                }
            }
        	arguments[arguments.length - 1] = messageId;
        }
        else 
        {
        	// messageId must be returned for all cases.
        	arguments = new Object[1];
        	arguments[arguments.length - 1] = messageId;
        }

        return arguments;
    }

    public static String encodeMessageAndArgumentsAsSqlerrmc(
                        String messageId,
                        Object []args )
    {
        String sqlerrmc = "";

                // arguments are variable part of a message

        for (int i = 0; args != null &&  i < args.length; i++) {
            sqlerrmc += args[i] + SQLERRMC_TOKEN_DELIMITER;
        }
        sqlerrmc += messageId;

	return sqlerrmc;
    }

    public static String encodeExceptionAsSqlerrmc( SQLException se )
    {
        // this could happen for instance if an SQLException was thrown
        // from a stored procedure.
        StringBuilder sb = new StringBuilder();
        sb.append(se.getLocalizedMessage());
        se = se.getNextException();
        if (se != null) {
            sb.append(SQLERRMC_TOKEN_DELIMITER);
            sb.append("SQLSTATE: ").append(se.getSQLState());
        }
        return sb.toString();
    }

    /**
     * Build the SQLERRMC for a {@code java.sql.DataTruncation} warning.
     * Serialize all the fields of the {@code DataTruncation} instance in the
     * order in which they appear in the parameter list of the constructor.
     *
     * @param dt the {@code DataTruncation} instance to serialize
     * @return the SQLERRMC string with all fields of the warning
     */
    private String buildDataTruncationSqlerrmc(DataTruncation dt) {
        return dt.getIndex() + SQLERRMC_TOKEN_DELIMITER +
               dt.getParameter() + SQLERRMC_TOKEN_DELIMITER +
               dt.getRead() + SQLERRMC_TOKEN_DELIMITER +
               dt.getDataSize() + SQLERRMC_TOKEN_DELIMITER +
               dt.getTransferSize();
    }



	/**
     * Build preformatted SQLException text 
     * for severe exceptions or SQLExceptions that are not Derby exceptions.
     * Just send the message text localized to the server locale.
     * 
     * @param se  SQLException for which to build SQLERRMC
     * @return preformated message text 
     *          with messages separted by SQLERRMC_PREFORMATED_MESSAGE_DELIMITER
     * 
     */
    private String  buildPreformattedSqlerrmc(SQLException se) {
        if (se == null) {
            return "";
        }
        
         // String buffer to build up message
        StringBuilder sb = new StringBuilder();
        sb.append(se.getLocalizedMessage());
        while ((se = se.getNextException()) != null) {
            sb.append(SQLERRMC_PREFORMATTED_MESSAGE_DELIMITER);
            sb.append("SQLSTATE: ");
            sb.append(se.getSQLState());
        }
        return sb.toString();
    }

    /**
     * Hash function to split messages into 50 files based upon the message identifier 
     * or SQLState. We don't use String.hashCode() as it varies between releases and 
     * doesn't provide an even distribution across the 50 files.
     */
    public static int hashString50(String key) {
		int hash = 0;
		int len = key.length();
		if (len > 5)
			len = 5;

		for (int i = 0; i < len; i++) {
			hash += key.charAt(i);
		}
		hash = hash % 50;
		return hash;
	}

	public static String formatMessage(ResourceBundle bundle, String messageId, Object[] arguments, boolean lastChance) {

		if (arguments == null)
			arguments = new Object[0];

		if (bundle != null) {

			try {
				messageId = bundle.getString(messageId);

				try {
					return MessageFormat.format(messageId, arguments);
				}
				catch (IllegalArgumentException iae) {
				}
				catch (NullPointerException npe) {
					//
					//null arguments cause a NullPointerException. 
					//This improves reporting.
				}

			} catch (MissingResourceException mre) {
				// caller will try and handle the last chance
				if (lastChance)
					throw mre;
			} 
		}

		if (messageId == null)
			messageId = "UNKNOWN";

		
		StringBuffer sb = new StringBuffer(messageId);

		int len = arguments.length;
		if (len > 0)
			sb.append(" : ");

		for (int i=0; i < len; i++) {
		    // prepend a comma to all but the first
			if (i > 0)
				sb.append(", ");

			sb.append('[');
			sb.append(i);
			sb.append("] ");
			if (arguments[i] == null)
				sb.append("null");
			else
				sb.append(arguments[i].toString());
		}

		
		return sb.toString();
	}

	/**
     * Method used by Derby Network Server to get localized message
     * @param sqlcode    sqlcode, not used.
     * @param errmcLen   sqlerrmc length
     * @param sqlerrmc   sql error message tokens, variable part of error message (ie.,
     *                   arguments) plus messageId, separated by separator.
     * @param sqlerrp    not used
     * @param errd0      not used
     * @param warn       not used
     * @param sqlState   5-char sql state
     * @param file       not used
     * @param localeStr  client locale in string
     * @param msg        OUTPUT parameter, localized error message
     * @param rc         OUTPUT parameter, return code -- 0 for success
     */

    public static void getLocalizedMessage(int sqlcode, short errmcLen, String sqlerrmc,
                                        String sqlerrp, int errd0, int errd1, int errd2,
                                        int errd3, int errd4, int errd5, String warn,
                                        String sqlState, String file, String localeStr,
                                        String[] msg, int[] rc)
    {
        //figure out client locale from input locale string

        int _pos1 = localeStr.indexOf("_");     // "_" position

        Locale locale = EN;     //default locale
        if (_pos1 != -1)
        {
            int _pos2 = localeStr.lastIndexOf("_");
            String language = localeStr.substring(0, _pos1);
            if (_pos2 == _pos1)
            {
                String country = localeStr.substring(_pos1 + 1);
                locale = new Locale(language, country);
            }
            else
            {
                String country = localeStr.substring(_pos1 + 1, _pos2);
                String variant = localeStr.substring(_pos2 + 1);
                locale = new Locale(language, country, variant);
            }
        }


        String messageId = sqlState;    //use sqlState if we don't have messageId
        
        Object[] args = getArgs(sqlState, sqlerrmc);
        messageId = (String) args[args.length - 1];
        Object[] arguments = new Object[args.length - 1];
        
        for (int i = 0; i < arguments.length; ++i)
        {
        	arguments[i] = args[i];
        }

        try {
            msg[0] = formatMessage
              (MessageService.getBundleForLocale(locale, messageId), messageId, arguments, true);
            rc[0] = 0;
            return;
        } catch (MissingResourceException mre) {
            // message does not exist in the requested locale
            // most likely it does exist in our fake base class _en, so try that.
        } catch (ShutdownException se) {
        }
        msg[0] = formatMessage
          (MessageService.getBundleForLocale(EN, messageId), messageId, arguments, false);
        rc[0] = 0;
    }
    
    /**
     * Method used by Derby Network Server to get localized message 
     * @param locale     locale
     * @param messageId  message id
     * @param args       message arguments
     */
    public static String getLocalizedMessage(Locale locale, String messageId, Object [] args)
    {
        String locMsg = null;

        try {
            locMsg = formatMessage
              (MessageService.getBundleForLocale(locale, messageId), messageId, args, true);
            return locMsg;
        } catch (MissingResourceException mre) {
            // message does not exist in the requested locale
            // most likely it does exist in our fake base class _en, so try that.
        } catch (ShutdownException se) {
        }
        locMsg = formatMessage(MessageService.getBundleForLocale(EN, messageId), messageId, args, false);
        return locMsg;
    }
}
