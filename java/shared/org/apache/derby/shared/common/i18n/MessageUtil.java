/*
   Derby - Class org.apache.derby.common.i18n.MessageUtil
 
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
package org.apache.derby.shared.common.i18n;

import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.sanity.SanityManager;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.text.MessageFormat;

/**
 * Class comments here
 */
public class MessageUtil
{    
    public static final Locale US = new Locale("en", "US");
            
    /** 
     *  The name of the resource bundle we are using to load
     *  messages
     */
    private String resourceBundleName;
    
    /**
     * Create an instance of MessageUtil with a specific resource
     * bundle. This assumes the default locale, which is just fine for
     * users of this class other than the engine (which potentially has
     * a different locale and a different resource bundle for each
     * invocation of getCompleteMessage().
     *
     * @param resourceBundleName
     *      The base name of the resource bundle to use.
     */
    public MessageUtil(String resourceBundleName)
    {
        this.resourceBundleName = resourceBundleName;
    }
    
    /**
     * Get a message with default locale.
     */
    public String getTextMessage(String messageID, Object... args) {
        return getCompleteMessage(messageID, args);
    }
    
    /** 
     * Instance method to get the complete message, using the
     * provided resource bundle name as specified when this
     * instance was constructed
     *
     * If for some reason the message could not be found, we return a
     * default message using the message arguments
     */
    public String getCompleteMessage(String messageID, Object[] args)
    {
        return getCompleteMessage(messageID, resourceBundleName, args);
    }
    
    /**
     * Generic routine to get a message with any number of arguments.
     * 
     * Looks in the provided resource bundle for the message, using the
     * specified locale and then the US locale.  
     * 
     * @param locale
     *      The locale to use when looking for the message.  If the message
     *      is not found using this locale, we attempt to find it using the
     *      US locale (our default).
     * 
     * @param resourceBundleName
     *      The base name for the resource bundle to use. 
     * 
     * @param messageId  
     *      The message identifier for this message
     * 
     * @param arguments
     *      The arguments for the message
     * 
     * @param composeDefault
     *      If this is true, this method will compose a default message if
     *      the message could not be found in the
     *      provided resource bundles.  If it is false, this method will
     *      throw a MissingResourceException if the message could not be
     *      found.
     * 
     * @return
     *      The message for the given message id, with arguments
     *      substituted.
     * 
     * @throws MissingResourceException
     *      If the message could not be found and the 
     *      <code>composeDefault</code> parameter was set to false.  
     */
    public static String getCompleteMessage(Locale locale, 
        String resourceBundleName, String messageId, Object[] arguments, 
        boolean composeDefault) throws MissingResourceException
    {
        try
        {
            return formatMessage(
                ResourceBundle.getBundle(resourceBundleName, locale), messageId,
                arguments, false);
        }
        catch ( MissingResourceException mre )
        {
            // Try the US locale.  Use composeDefault to indicate whether
            // we should compose a default message or throw an exception if
            // the message still is not found.
            return formatMessage(
                    ResourceBundle.getBundle(resourceBundleName, US), 
                    messageId, arguments, composeDefault);
        }
    }
    
    /**
     * This is a wrapper for the getCompleteMessage workhorse routine
     * using some obvious defaults, particularly for non-engine subsystems
     * that only ever use the default locale.
     * 
     * Get a message using the default locale.  If the message is not found 
     * with the default locale, use the US locale.   Do this both for the
     * common bundle and the parent bundle.
     * 
     * If the message is not found in common or in the parent resource
     * bundle, return a default message composed of the message arguments.
     * 
     * @param messageId
     *      The id to use to look up the message
     * 
     * @param resourceBundleName
     *      The base name of the resource bundle to use.
     * 
     * @param arguments
     *      The arguments to the message
     */
    public static String getCompleteMessage(String messageId,
        String resourceBundleName, Object[] arguments) 
        throws MissingResourceException
    {
        return getCompleteMessage(Locale.getDefault(), resourceBundleName,
            messageId, arguments, true);
    }
    
    /**
     * Format a message given a resource bundle and a message id.
     * <p>
     * The arguments to the messages are passed via an object array. The objects
     * in the array WILL be changed by this class. The caller should NOT get the
     * object back from this array.
     *
     * @param bundle
     *      The resource bundle to use to look for the message
     *
     * @param messageId
     *      The message id to use for the message
     *
     * @param arguments
     *      The arguments for the message
     *
     * @param composeDefault
     *      Indicates whether a default message should be composed if
     *      the message can't be found in the resource bundle.
     *      <p>
     *      If composeDefault is false, this method will
     *      throw a MissingResourceException if the message could not be
     *      found.
     *      <p>
     *      If composeDefault is true, then if the message id is not found in
     *      the given bundle, this method composes and returns as helpful a 
     *      message as possible in the format "UNKNOWN : [arg1], [arg2], ..."
     */
    public static String formatMessage(ResourceBundle bundle, String messageId, 
        Object[] arguments, boolean composeDefault) {

        String message = null;
        String badArgsMessage = null;
        
        if (arguments == null)
            arguments = new Object[0];

        if (bundle != null) {

            try {
                message = bundle.getString(messageId);
                
                
                // Ensure that the right number of arguments are passed in.
                if ( SanityManager.DEBUG )
                {
                    int numExpected = countParams(message);
                    SanityManager.ASSERT(numExpected == arguments.length,
                        "Number of parameters expected for message id " +
                        messageId + " (" + numExpected +
                        ") does not match number of arguments received (" +
                        arguments.length + ")");
                }

                try {
                    return MessageFormat.format(message, arguments);
                }
                catch (IllegalArgumentException iae) {
                    if ( !composeDefault || SanityManager.DEBUG )
                        throw iae;
                }
                catch (NullPointerException npe) {
                    //
                    //null arguments cause a NullPointerException. 
                    //This improves reporting.
                    if ( !composeDefault  || SanityManager.DEBUG )
                        throw npe;
                }

            } catch (MissingResourceException mre) {
                // caller will try and handle the last chance
                if (!composeDefault )
                    throw mre;
            } 
        }

        return composeDefaultMessage("UNKNOWN MESSAGE, id " + messageId, arguments);
    }
    
    /**
     * Count the number of substituation parameters in the message
     */
    private static int countParams(String message)
    {
        boolean openFound = false;
        int numparams = 0;
        
        for ( int i = 0 ; i < message.length() ; i++ )
        {
            char ch = message.charAt(i);
            if ( ch == '{' ) {
                openFound = true;
            }
            
            if ( ch == '}' && openFound )
            {
                numparams++;
                openFound = false;
            }
        }
        
        return numparams;
    }

    /**
     * Compose a default message so that the user at least gets
     * *something* useful rather than just a MissingResourceException,
     * which is particularly unhelpful
     *
     * @param message
     *      The message to start with, which often is null
     *
     * @param arguments
     *      The arguments to the message.  
     */
    public static String composeDefaultMessage(String message, Object[] arguments)
    {
        if (message == null)
        {
            message = "UNKNOWN";
        }
        
        StringBuffer sb = new StringBuffer(message);
        
        if ( arguments == null )
        {
            return sb.toString();
        }

        sb.append(" : ");
        int len = arguments.length;

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
}
