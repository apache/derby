/*

   Derby - Class org.apache.derbyBuild.splitmessages

   Copyright 2000, 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyBuild;

import java.io.*;
import java.util.*;

import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

public class splitmessages {
	/**
		arg[0] is the destination directory
		arg[1] is the source file.
	*/
    
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
    private static TreeSet clientMessageIds = new TreeSet();
    
    /**
     * Initialize the set of shared message ids
     */
    static void initClientMessageIds()
    {
        // Add message ids that don't start with XJ here
        clientMessageIds.add(SQLState.NO_CURRENT_CONNECTION);
        clientMessageIds.add(SQLState.NOT_IMPLEMENTED);
        clientMessageIds.add(SQLState.CANNOT_CLOSE_ACTIVE_XA_CONNECTION);
        clientMessageIds.add(SQLState.XACT_SAVEPOINT_RELEASE_ROLLBACK_FAIL);
    }

	public static void main(String[] args) throws Exception {

        initClientMessageIds();

		Properties p = new Properties();

		File dir = new File(args[0]);

		File source = new File(args[1]);
        File clientDir = new File(args[2]);
        
		String s = source.getName();
		// loose the suffix
		s = s.substring(0, s.lastIndexOf('.'));
		// now get the locale
		String locale = s.substring(s.indexOf('_'));

		boolean addBase = "_en".equals(locale);


		InputStream is = new BufferedInputStream(new FileInputStream(source), 64 * 1024);

		p.load(is);
		is.close();

        
		Properties[] c = new Properties[50];
		for (int i = 0; i < 50; i++) {
			c[i] = new Properties();
		}
        
        Properties clientProps = new Properties();

        // Open the client properties file for the given locale
        // from the client locales directory, and then intialize
        // clientProps with what we find in there
        String clientPropsFileName = "clientmessages" + locale + ".properties";
        try
        {
            InputStream clientInStream = new FileInputStream(
                    new File(clientDir, clientPropsFileName));
            clientProps.load(clientInStream);
            clientInStream.close();
        }
        catch ( FileNotFoundException fnfe )
        {
            // That's fine, it just means there are no client-specfic messages
            // for this locale yet
        }

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
			OutputStream fos = new BufferedOutputStream(
				new FileOutputStream(new File(dir, "m"+i+locale+".properties")), 16 * 1024);

            
			c[i].save(fos, (String) null);
			fos.flush();
			fos.close();
            
			if (addBase) {
				// add duplicate english file as the base
				fos = new BufferedOutputStream(
					new FileOutputStream(new File(dir, "m"+i+".properties")), 16 * 1024);
				c[i].save(fos, (String) null);
				fos.flush();
				fos.close();
			}


		}
        
		System.out.println("split messages" + locale);

        // Save the client messages (the combination of what was already
        // there and what we added from the engine properties file) into
        // the Derby locales directory
        OutputStream clientOutStream = new BufferedOutputStream(
            new FileOutputStream(new File(dir, clientPropsFileName)), 
            16 * 1024);

        clientProps.save(clientOutStream, (String)null);
        clientOutStream.flush();
        clientOutStream.close();
        
        if ( addBase )
        {
            // Save the English messages as the base
            clientOutStream = new BufferedOutputStream(
                new FileOutputStream(new File(dir, "clientmessages.properties")), 
                16 * 1024);

            clientProps.save(clientOutStream, (String)null);
            clientOutStream.flush();
            clientOutStream.close();            
        }
        System.out.println("Copied client messages for " + locale);
	}
    
    /**
     * Determine if this is a message that the client is using
     *
     * We assume all message ids starting with "XJ" are client messages
     * (even though many of them may not be, it saves the coder the effort
     * of explicitly adding each XJ shared message, and covers 90% of the
     * shared messages
     *
     * All other shared message ids should be added to the static array
     * clientMessageIds, defined at the top of this class
     */
    static boolean isClientMessage(String messageId)
    {
        if ( messageId.startsWith("XJ") )
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
