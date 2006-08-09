/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.xaJNDI

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.apache.derby.jdbc.EmbeddedXADataSource;

import java.util.Hashtable;

import javax.naming.*;
import javax.naming.directory.*;

//The test compares the xa data source which is bound to jndi
//with the xa data source which is returned from the jndi lookup.
public class xaJNDI
{ 
	public static void main(String[] args)
	{
		try
		{
			Hashtable env = new Hashtable();
			env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
			// using a fictional name - host and port & url will have to be 
			// modified to use a real ldap server machine & port & url
			env.put(Context.PROVIDER_URL, "ldap://thehost.opensource.apache.com:389");
			env.put(Context.SECURITY_AUTHENTICATION, "simple");
			InitialDirContext ic = new InitialDirContext(env);

			EmbeddedXADataSource rxads =  new EmbeddedXADataSource();
			rxads.setDatabaseName("rxads");
			rxads.setCreateDatabase("create");
			rxads.setDescription("XA DataSource");
			ic.rebind("cn=compareDS, o=opensource.apache.com",rxads);
      javax.sql.XADataSource ads =
      (javax.sql.XADataSource)ic.lookup("cn=compareDS, o=opensource.apache.com");
      if (rxads.equals(ads))
        System.out.println("SUCCESS:The 2 data sources are same");
      else
	      System.out.println("FAILURE:The 2 data sources should be same");

  		rxads.setCreateDatabase("");
      if (rxads.equals(ads))
        System.out.println("FAILURE:The 2 data sources should be different");
      else
	      System.out.println("SUCCESS:The 2 data sources are different");

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("caught " + e);
		}
	}

}
