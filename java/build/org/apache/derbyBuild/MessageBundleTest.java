/*
 
   Derby - Class org.apache.derbyMessageBundleTest
 
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

import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;

import java.util.HashSet;
import java.lang.reflect.Field;
import java.util.ResourceBundle;
import java.util.Locale;
import java.util.Iterator;


/**
 * This class does everything we can to validate that the messages_en.properties
 * file is in synch with SQLState.java and MessageId.java.  We want to make sure
 * that message ids defined in SQLState and MessageId have matching messages
 * in the messages properties file, and also find out if there are any messages
 * that don't have matching ids in the SQLState and MessageId files.   The
 * first is a bug, the second is something to be aware of.
 */
public class MessageBundleTest {

    static boolean failbuild = false;

    /**
     * <p>
     * Let Ant conjure us out of thin air.
     * </p>
     */
    public MessageBundleTest()
    {}
    
    public static void main(String [] args) throws Exception
    {
        MessageBundleTest t = new MessageBundleTest();
        try {
            t.testMessageBundleOrphanedMessages();
            t.testMessageIdOrphanedIds();
            t.testSQLStateOrphanedIds();
        } catch (Exception e) {
            System.out.println("Message check failed: ");
            e.printStackTrace();
        }
        if (failbuild) 
            throw new Exception("Message check failed. \n" +
                "See error in build output or call ant runmessagecheck.");
    }    
    
    // The list of ids.  We use a HashSet so we can detect duplicates easily
    static HashSet<String> sqlStateIds  = new HashSet<String>();
    static HashSet<String> messageIdIds = new HashSet<String>();
    static HashSet<String> messageBundleIds = new HashSet<String>();
    
    static {
        try {
            // Load all the ids for the SQLState class
            loadClassIds(SQLState.class, sqlStateIds);

            // Load all the ids for the MessageId class
            loadClassIds(MessageId.class, messageIdIds);

            // Load all the ids for the messages_en properties file
            loadMessageBundleIds();
        } catch ( Exception e ) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
    
    static void loadClassIds(Class idclass, HashSet<String> set)
            throws Exception {
        Field[] fields = idclass.getFields();
        
        int length = fields.length;
        for ( int i = 0; i < length ; i++ )
        {
            String id = (String)fields[i].get(null);
            
            if ( id.length() == 2 ) {
                // Skip past identifiers that are just categories
                continue;
            }
            
            // Skip past "special" SQL States that are not expected
            // to have messages
            if ( id.equals("close.C.1") )   continue;            
            if ( id.equals("rwupd" ) )      continue;
            if ( id.equals("02502" ) )      continue;
            if ( id.equals("XSAX0") )       continue;
            
            if ( ! set.add(id) )
            {
                failbuild=true;
                System.err.println("ERROR: The id " + id + 
                    " was found twice in " + idclass.getName());
            }
        }
    }
            
    /** 
     * Load all the message ids from messages_en.properties into a HashSet.
     * This assumes its available on the classpath
     */
    static void loadMessageBundleIds() throws Exception {
        // The messages_*.properties files are split into fifty separate
        // message bundle files.  We need to load each one in turn
        int numBundles = 50;
        
        for ( int i=0 ; i < numBundles ; i++ ) {
            loadMessageBundle(i);
        }
    }
    
    static void loadMessageBundle(int index) {
        String bundleName = "org.apache.derby.loc.m" + index;
        
        ResourceBundle bundle = 
            ResourceBundle.getBundle(bundleName, Locale.ENGLISH);

        java.util.Enumeration keys = bundle.getKeys();

        while ( keys.hasMoreElements() ) {
            String key = (String)keys.nextElement();                

            if ( ! messageBundleIds.add(key) ) {
                failbuild=true;
                System.err.println("ERROR: the key " + key +
                    " exists twice in messages_en.properties");
            }
        }        
    }

    /**
     * See if there are any message ids in SQLState.java that are
     * not in the message bundle
     */
    public void testSQLStateOrphanedIds() throws Exception {
        Iterator it = sqlStateIds.iterator();
        
        while ( it.hasNext() ) {
            String sqlStateId = (String)it.next();
            
            if ( ! messageBundleIds.contains(sqlStateId) ) {
                // there are some error messages that do not need to be in 
                // messages.xml:
                // XCL32: will never be exposed to users (see DERBY-1414)
                // XSAX1: shared SQLState explains; not exposed to users. 
                // 01004: automatically assigned by java.sql.DataTruncation and
                //        never used to generate a message
                if (!(sqlStateId.equalsIgnoreCase("XCL32.S") ||
                      sqlStateId.equalsIgnoreCase("XSAX1")   ||
                      sqlStateId.equalsIgnoreCase("01004"))) {
                // Don't fail out on the first one, we want to catch
                // all of them.  Just note there was a failure and continue
                    failbuild=true;
                    System.err.println("ERROR: Message id " + sqlStateId +
                        " in SQLState.java was not found in" +
                        " messages_en.properties");         
                }
             }
        }
    }

    /**
     * See if there are any message ids in MessageId.java not in
     * the message bundle
     */
    public void testMessageIdOrphanedIds() throws Exception {
        Iterator it = messageIdIds.iterator();
        
        while ( it.hasNext() ) {
            String sqlStateId = (String)it.next();
            
            if ( ! messageBundleIds.contains(sqlStateId) ) {
                // Don't fail out on the first one, we want to catch
                // all of them.  Just note there was a failure and continue
                failbuild=true;
                System.err.println("ERROR: Message id " + sqlStateId +
                    " in MessageId.java was not found in" +
                    " messages_en.properties");                    
             }
        }
    }
     
    /**
     * See if there are any message ids in the message bundle that
     * are <b>not</b> in SQLState.java or MessageId.java
     */
    public void testMessageBundleOrphanedMessages() throws Exception {
        Iterator it = messageBundleIds.iterator();
        
        while (it.hasNext() ) {
            String msgid = (String)it.next();
            
            if ( sqlStateIds.contains(msgid)) {
                continue;
            }
            
            if ( messageIdIds.contains(msgid)) {
                continue;
            }
            
            // Don't fail out on the first one, we want to catch
            // all of them.  Just note there was a failure and continue
            failbuild=true;
            System.err.println("WARNING: Message id " + msgid + 
                " in messages_en.properties is not " +
                "referenced in either SQLState.java or MessageId.java");
        }        
    }
}
