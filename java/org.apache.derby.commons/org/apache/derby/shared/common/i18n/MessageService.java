/*

   Derby - Class org.apache.derby.shared.common.i18n.MessageService

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

import org.apache.derby.shared.common.error.ShutdownException;
import org.apache.derby.shared.common.info.JVMInfo;
import org.apache.derby.shared.common.reference.ModuleUtil;

import java.io.InputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
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
    private static final String LOCALE_STUB = "locale_";
    private static final String CLIENT_MESSAGES = "clientmessages";
    private static final String TOOLS_MESSAGES = "toolsmessages";
    private static final String SYSINFO_MESSAGES = "sysinfoMessages";
    private static final String SERVER_MESSAGES = "drda";

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
	  <P>
	  Transform the message from messageID to the actual error, warning, or
	  info message using the correct locale.
	  </P>

	  <P>
	  The arguments to the messages are passed via an object array, the objects
	  in the array WILL be changed by this class. The caller should NOT get the
	  object back from this array.
	  </P>

      @param messageId The message handle
      @param arguments The arguments to the message

      @return the text for the message with arguments plugged in
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
     * Append a property name onto the end of a message
     *
     * @param messageId The handle on the message
     * @param propertyName The property to append
     *
     * @return the resulting new property name
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

        @param resource The name of the message resource
        @param locale The preferred locale

        @return the best message bundle to localize the messages
	*/
	public static ResourceBundle getBundleWithEnDefault(String resource, Locale locale)
    {
        ResourceBundle retval = null;

        retval = getBundle(resource, locale);

        if (retval == null)
        {
            //
            // This can happen if the database territory overrides
            // the default Locale, but the territory isn't supported.
            // Try the default Locale.
            //
            Locale defaultLocale = Locale.getDefault();

            if (!defaultLocale.equals(locale))
            {
                retval = getBundle(resource, defaultLocale);
            }
        }

        //
        // Ok, fallback on English, the localization bundled into the base Derby jars.
        // This throws MissingResourceException if the situation is completely
        // confused.
        //
        if (retval == null)
        {
            retval = lookupBundle(resource, EN);
        }

        return retval;
	}

	/**
		Look up a bundle in the correct package. Returns
        null if the bundle can't be found.
	*/
	private static ResourceBundle getBundle(String resource, Locale locale)
    {
        ResourceBundle retval = null;

		try {
            retval = lookupBundle(localizeResourceName(resource, locale.toString()), locale);
		} catch (MissingResourceException mre) {}

        // just try the language. it's better than nothing.
        if (retval == null)
        {
            try {
                retval = lookupBundle(localizeResourceName(resource, locale.getLanguage()), locale);
            } catch (MissingResourceException mre) {}
        }

        return retval;
	}
  
	/**
		Use the JVM to lookup a ResourceBundle
	*/
	private static ResourceBundle lookupBundle(String resource, Locale locale)
    {
        if (JVMInfo.isModuleAware()) { return lookupBundleInModule(resource, locale); }
        else { return ResourceBundle.getBundle(resource, locale); }
	}
  
	/**
		Lookup the message in a Jigsaw module.
	*/
	public static ResourceBundle lookupBundleInModule(String resource, Locale locale)
    {
        String moduleName;
        boolean useEnglish = locale.getLanguage().equals(EN.getLanguage());
        boolean useEngineModule = false;
        boolean localizationModule = false;

        //
        // The message localizations should be in one of the following modules:
        //
        //   engine jar
        //   client jar
        //   tools jar
        //   locale-specific message jar
        //

        if (resource.contains(CLIENT_MESSAGES)) { moduleName = ModuleUtil.CLIENT_MODULE_NAME; }
        else if (resource.contains(SERVER_MESSAGES)) { moduleName = ModuleUtil.SERVER_MODULE_NAME; }
        else if (resource.contains(TOOLS_MESSAGES) || resource.contains(SYSINFO_MESSAGES))
        {
            if (useEnglish){ moduleName = ModuleUtil.TOOLS_MODULE_NAME; }
            else
            {
                moduleName = ModuleUtil.localizationModuleName(locale.toString());
                localizationModule = true;
            }
        }
        else // must be engine messages
        {
            if (useEnglish)
            {
                moduleName = ModuleUtil.ENGINE_MODULE_NAME;
                useEngineModule = true;
            }
            else
            {
                moduleName = ModuleUtil.localizationModuleName(locale.toString());
                localizationModule = true;
            }
        }

        Module messageModule = ModuleUtil.derbyModule(moduleName);
        if (messageModule == null)
        {
            if (localizationModule)
            {
                // retry with just the language as the suffix of the localization module
                moduleName = ModuleUtil.localizationModuleName(locale.getLanguage());
                messageModule = ModuleUtil.derbyModule(moduleName);
            }
        }
        if (messageModule == null)
        {
            return null;
        }

        // first try with whole locale string
        ResourceBundle result = lookupBundleInModule
          (messageModule, resource, locale.toString(), useEnglish, useEngineModule);

        // if that fails, just use the language code without the country code
        if ( result == null)
        {
            result = lookupBundleInModule
              (messageModule, resource, locale.getLanguage(), useEnglish, useEngineModule);
        }

        return result;
    }

    /** Lookup a message in a given module */
    private static ResourceBundle lookupBundleInModule
      (Module messageModule, String resource, String localeSuffix, boolean useEnglish, boolean useEngineModule)
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(resource.replace('.', '/'));
        if (!useEnglish)
        {
            buffer.append("_");
            buffer.append(localeSuffix);
        }
        if (useEngineModule && useEnglish)
        {
            buffer.append("_en");
        }
        buffer.append(".properties");
        String fullResourceName = buffer.toString();

        return getModuleResourceBundle(fullResourceName, messageModule);
    }

    /**
     * Get a resource bundle from a module.
     *
     * @param resourceName The name of the resource
     * @param module The module where it lives
     *
     * @return the corresponding resource bundle
     */
    private static PropertyResourceBundle getModuleResourceBundle
       (final String resourceName, final Module module)
    {
        try
        {
            InputStream is = AccessController.doPrivileged
                (
                 new PrivilegedExceptionAction<InputStream>()
                 {
                    public InputStream run() throws IOException
                    {
                        return module.getResourceAsStream(resourceName);
                    }
                });

            //System.out.println("        XXX stream = " + is + " for resourceName " + resourceName + " in module " + module);

            if (is != null)
            {
                return new PropertyResourceBundle(is);
            }
            else { return null; }
        }
        catch (Exception ioe)
        {
            System.out.println(ioe.getMessage());
            return null;
        }
    }

    /**
        Add a directory level named locale_xx_YY to the resource name
        if it is not the clientmessages resource bundle. So, for instance,
        "org.apache.derby.loc.tools.toolsmessages" becomes
        "org.apache.derby.loc.tools.locale_de_DE.toolsmessages".
    */
    private static String localizeResourceName(String original, String localeName)
    {
        if (
            (original == null) ||
            (original.contains(CLIENT_MESSAGES)) ||
            (original.contains(SERVER_MESSAGES))
            )
        { return original; }


        // American English messages are not re-located to a subdirectory
        if (EN.toString().equals(localeName))
        { return original; }

        int lastDotIdx = original.lastIndexOf('.');
        String retval =
          original.substring(0, lastDotIdx + 1) +
          LOCALE_STUB + localeName +
          original.substring(lastDotIdx, original.length());

        return retval;
    }

	/**
		Hash function to split messages into 50 files based
		upon the message identifier or SQLState. We don't use
		String.hashCode() as it varies between releases and
		doesn't provide an even distribution across the 50 files.

        @param key A key to hash
        @return the corresponding hash
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
