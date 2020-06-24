/*

   Derby - Class org.apache.derby.iapi.services.loader.GeneratedByteCode

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
import org.apache.derby.iapi.services.context.Context;

/**
	Generated classes must implement this interface.

*/
public interface GeneratedByteCode {

	/**
		Initialize the generated class from a context.
		Called by the class manager just after
		creating the instance of the new class.

        @param context The context to use
        @throws StandardException on error
	*/
	public void initFromContext(Context context)
		throws StandardException;
//IC see: https://issues.apache.org/jira/browse/DERBY-418
//IC see: https://issues.apache.org/jira/browse/DERBY-1142

	/**
		Set the Generated Class. Call by the class manager just after
		calling initFromContext.

        @param gc A generated class
	*/
	public void setGC(GeneratedClass gc);

	/**
		Called by the class manager just after calling setGC().

        @throws StandardException on error
	*/
	public void postConstructor() throws StandardException;

	/**
		Get the GeneratedClass object for this object.

        @return a generated class
	*/
	public GeneratedClass getGC();

	public GeneratedMethod getMethod(String methodName) throws StandardException;


	public Object e0() throws StandardException ; 
	public Object e1() throws StandardException ;
	public Object e2() throws StandardException ;
	public Object e3() throws StandardException ;
	public Object e4() throws StandardException ; 
	public Object e5() throws StandardException ;
	public Object e6() throws StandardException ;
	public Object e7() throws StandardException ;
	public Object e8() throws StandardException ; 
	public Object e9() throws StandardException ;
}
