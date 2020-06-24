/*

   Derby - Class org.apache.derby.iapi.services.loader.ClassFactory

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

package org.apache.derby.iapi.services.loader;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.util.ByteArray;

import java.io.ObjectStreamClass;


/**
	A class factory module to handle application classes
	and generated classes.
*/

public interface ClassFactory {

	/**
		Add a generated class to the class manager's class repository.

        @param fullyQualifiedName Full name of the class
        @param classDump The byte code of the class
        @return the corresponding generated class
		@exception 	StandardException	Standard Derby error policy

	*/
	public GeneratedClass loadGeneratedClass(String fullyQualifiedName, ByteArray classDump)
		throws StandardException;

	/**
		Return a ClassInspector object

        @return the class inspector object
	*/
	public ClassInspector	getClassInspector();

	/**
		Load an application class, or a class that is potentially an application class.

        @param className The name of the class to load
        @return the corresponding class object
		@exception ClassNotFoundException Class cannot be found, or
//IC see: https://issues.apache.org/jira/browse/DERBY-485
		a SecurityException or LinkageException was thrown loading the class.
	*/
	public Class loadApplicationClass(String className)
		throws ClassNotFoundException;

	/**
		Load an application class, or a class that is potentially an application class.

        @param classDescriptor Descriptor for class
        @return the corresponding class
		@exception ClassNotFoundException Class cannot be found, or
//IC see: https://issues.apache.org/jira/browse/DERBY-485
		a SecurityException or LinkageException was thrown loading the class.
	*/
	public Class loadApplicationClass(ObjectStreamClass classDescriptor)
		throws ClassNotFoundException;

	/**
		Was the passed in class loaded by a ClassManager.

        @param theClass The class in question
		@return true if the class was loaded by a Derby class manager,
		false it is was loaded by the system class loader, or another class loader.
	*/
	public boolean isApplicationClass(Class theClass);

	/**
		Notify the class manager that a jar file has been modified.
		@param reload Restart any attached class loader

		@exception StandardException thrown on error
	*/
	public void notifyModifyJar(boolean reload) throws StandardException ;

	/**
		Notify the class manager that the classpath has been modified.

        @param classpath The classpath being modified
		@exception StandardException thrown on error
	*/
	public void notifyModifyClasspath(String classpath) throws StandardException ;

	/**
		Return the in-memory "version" of the class manager. The version
		is bumped everytime the classes are re-loaded.

        @return the version of the class loader
	*/
	public int getClassLoaderVersion();
}
