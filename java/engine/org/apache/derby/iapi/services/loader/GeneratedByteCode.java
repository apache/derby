/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.loader
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.loader;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.Context;

/**
	Generated classes must implement this interface.

*/
public interface GeneratedByteCode { 

	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

	/**
		Initialize the generated class from a context.
		Called by the class manager just after
		creating the instance of the new class.
	*/
	public void initFromContext(Context context);

	/**
		Set the Generated Class. Call by the class manager just after
		calling initFromContext.
	*/
	public void setGC(GeneratedClass gc);

	/**
		Called by the class manager just after calling setGC().
	*/
	public void postConstructor() throws StandardException;

	/**
		Get the GeneratedClass object for this object.
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
