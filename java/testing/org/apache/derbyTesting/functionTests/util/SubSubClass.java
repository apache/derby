/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;

/**
 * This class is for testing whether methods in sub-classes are found.
 */

public class SubSubClass extends SubClass
{ 
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	public static int OVEROVERLOADED_INTSTATIC = 3;

	public SubSubClass(int value)
	{
		super(value);
	}

	public String parmType(Integer value)
	{
		return "java.lang.Integer parameter in SubSubClass";
	}

	public String parmType(Boolean value)
	{
		return "java.lang.Boolean parameter in SubSubClass";
	}

	public static int overloadedStaticMethod()
	{
		return 3;
	}
}
