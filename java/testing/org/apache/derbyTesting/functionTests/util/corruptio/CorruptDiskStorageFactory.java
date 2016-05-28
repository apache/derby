/*

   Derby - Class org.apache.derbyTesting.functionTests.util.corruptio.CorruptDiskStorageFactory

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

package org.apache.derbyTesting.functionTests.util.corruptio;
import org.apache.derby.io.WritableStorageFactory;
import org.apache.derby.io.StorageFactory;
import org.apache.derby.iapi.services.info.JVMInfo;

/**
 * This class provides proxy implementation of the StorageFactory
 * interface for testing. 
 *
 * Storage Factory is used by the database engine to access 
 * persistent data and transaction logs. By default all the method calls
 * delegate the work to the real disk storage factory
 * (org.apache.derby.impl.io.DirStorageFactory)
 * based on the classes in the java.io packgs. In some cases this  factory
 * instruments some methods to corrupt the io to simulate disk corruptions for
 * testing. For example to simulate out of order partial writes to disk before
 * the crash. 
 * 
 * Derby by default uses the storage factory implementation in 
 * DirStorageFactory/DirStorageFactory4 when a database is accessed with 
 * "jdbc:derby:<databaseName>". This factory can be specified instead using 
 * derby.subSubProtocol.<sub protocol name>  For example:
 *
 *  derby.subSubProtocol.csf=org.apache.derbyTesting.functionTests.
 *             util.corruptio.CorruptDiskStorageFactory
 *  database need to be accessed by specifying the subporotocol name like
 *  'jdbc:derby:csf:wombat'.
 *
 * Interaction between the tests that requires instrumenting the i/o and 
 * this factory is through the flags in CorruptibleIo class. Tests should not 
 * call the methods in this factory directly. Database engine invokes the 
 * methods in this factory, so they can instrumented to do whatever is 
 * required for testing.
 * 
 * @version 1.0
 * @see CorruptibleIo
 * @see WritableStorageFactory
 * @see StorageFactory
 * 
 */

public class CorruptDiskStorageFactory extends CorruptBaseStorageFactory
{
	/*
	 * returns the real storage factory to which all the call should be 
	 * delegated from the proxy methods.  
	 */
	WritableStorageFactory getRealStorageFactory()
	{
		String dirStorageFactoryClass =
                "org.apache.derby.impl.io.DirStorageFactory";
		
		WritableStorageFactory storageFactory = null;
		try{
			Class<?> storageFactoryClass = Class.forName(dirStorageFactoryClass);
			storageFactory = 
                (WritableStorageFactory) storageFactoryClass.getConstructor().newInstance();
		}catch(Exception e)
		{
			System.out.println(
                "Failed to instantiate the disk storeage classes");
			e.printStackTrace();
		}
		
		return  storageFactory;
	}
}
