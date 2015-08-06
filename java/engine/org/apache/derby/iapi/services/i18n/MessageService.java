/*

   Derby - Class org.apache.derby.iapi.services.i18n.MessageService

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

package org.apache.derby.iapi.services.i18n;

import org.apache.derby.shared.common.error.ShutdownException;
import org.apache.derby.iapi.services.info.JVMInfo;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.text.MessageFormat;

/**
 *  Message Service implementation provides a mechanism for locating
 * messages and substituting arguments for message parameters.
 * It also provides a service for locating property values.
 * <p>
 * It uses the resource bundle mechanism for locating messages based on
 * keys; the preferred form of resource bundle is a property file mapping
 * keys to messages.
 *
 */
public final class MessageService {

	private static final Locale EN = new Locale("en", "US");

	private static BundleFinder finder;

	private MessageService() {}


	public static ResourceBundle getBundleForLocale(Locale locale, String msgId) {
		try {
			return MessageService.getBundleWithEnDefault("org.apache.derby.loc.m"+hashString50(msgId), locale);
		} catch (MissingResourceException mre) {
		}
		return null;
	}


	public static void setFinder(BundleFinder theFinder) {
		finder = theFinder;
	}

	/**
	  Transform the message from messageID to the actual error, warning, or
	  info message using the correct locale.

	  <P>
	  The arguments to the messages are passed via an object array, the objects
	  in the array WILL be changed by this class. The caller should NOT get the
	  object back from this array.

	 */
    public static String getTextMessage(String messageId, Object... arguments) {
		try {
			return formatMessage(getBundle(messageId), messageId, arguments, true);
		} catch (MissingResourceException mre) {
			// message does not exist in the requested locale or the default locale.
			// most likely it does exist in our fake base class _en, so try that.
		} catch (ShutdownException se) {
		}
		return formatMessage(getBundleForLocale(EN, messageId), messageId, arguments, false);
	}



	/**
	 */
	public static String getProperty(String messageId, String propertyName) {

		ResourceBundle bundle = getBundle(messageId);

		try {
			if (bundle != null)
				return bundle.getString(messageId.concat(".").concat(propertyName));
		} catch (MissingResourceException mre) {
		}
		return null;
	}

	//
	// class implementation
	//
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

	private static ResourceBundle getBundle(String messageId) {

		ResourceBundle bundle = null;

		if (finder != null)
			bundle = finder.getBundle(messageId);

		if (bundle == null) {
			bundle = MessageService.getBundleForLocale(Locale.getDefault(), messageId);
		}

		return bundle;
	}

	/**
		Method to use instead of ResourceBundle.getBundle().
		This method acts like ResourceBundle.getBundle() but if
		the resource is not available in the requested locale,
		default locale or base class the one for en_US is returned.
	*/

	public static ResourceBundle getBundleWithEnDefault(String resource, Locale locale) {

		try {
			return ResourceBundle.getBundle(resource, locale);
		} catch (MissingResourceException mre) {

			// This covers the case where neither the
			// requested locale or the default locale
			// have a resource.

			return ResourceBundle.getBundle(resource, EN);
		}
	}

	/**
		Hash function to split messages into 50 files based
		upon the message identifier or SQLState. We don't use
		String.hashCode() as it varies between releases and
		doesn't provide an even distribution across the 50 files.

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
}
