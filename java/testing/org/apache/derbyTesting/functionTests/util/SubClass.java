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

public class SubClass extends ManyMethods
{

	static int OVERLOADED_INTSTATIC = 2;
	public static int OVEROVERLOADED_INTSTATIC = 2;

	public int intSubClassOnly;

	public SubClass(int value)
	{
		super(value);
		intSubClassOnly = value * value;
	}

	public static SubClass staticSubClass(Integer value)
	{
		return new SubClass(value.intValue());
	}

	public String parmType(Double value)
	{
		return "java.lang.Double parameter in SubClass";
	}

	public String parmType(Integer value)
	{
		return "java.lang.Integer parameter in SubClass";
	}

	public static int overloadedStaticMethod()
	{
		return 2;
	}
}
