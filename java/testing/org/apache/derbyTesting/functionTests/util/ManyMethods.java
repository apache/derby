/*

   Derby - Class org.apache.derbyTesting.functionTests.util.ManyMethods

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import java.math.BigDecimal;

import java.io.Serializable;

/**
 * This class is for testing method calls on user-defined types.  It has
 * many different methods for testing different cases.
 */

public class ManyMethods implements Serializable
{

	int	value;
	protected int protectedValue;
	private int privateValue;
	public int publicValue;
	public short publicshort;
	public Short publicShort;
	public byte publicbyte;
	public Byte publicByte;
	public int publicint;
	public Integer publicInteger;
	public long publiclong;
	public Long publicLong;
	public boolean publicboolean;
	public Boolean publicBoolean;
	public float publicfloat;
	public Float publicFloat;
	public double publicdouble;
	public Double publicDouble;
	public String publicString;
	public Date publicDate;
	public Time publicTime;
	public Timestamp publicTimestamp;
	public ManyMethods myself;

	public static int NONOVERLOADED_INTSTATIC = 1;
	public static int OVERLOADED_INTSTATIC = 1;
	public static int OVEROVERLOADED_INTSTATIC = 1;

	public ManyMethods(int value)
	{
		this.value = value;
		this.myself = this;
		protectedValue = value;
		privateValue = value;
		publicValue = value;
		publicint = value;
		publicInteger = new Integer(value);
		publicshort = (short) value;
		publicShort = new Short((short) value);
		publicbyte = (byte) value;
		publicByte = new Byte((byte) value);
		publiclong = (long) value;
		publicLong = new Long((long) value);
		publicboolean = booleanMethod();
		publicBoolean = BooleanMethod();
		publicfloat = floatMethod();
		publicFloat = FloatMethod();
		publicdouble = doubleMethod();
		publicDouble = DoubleMethod();
		publicString = StringMethod();
		publicDate = DateMethod();
		publicTime = TimeMethod();
		publicTimestamp = TimestampMethod();
	}

	/*
	** The following methods are for testing signature matching.  Each method
	** takes a single parameter.  The parameter types vary by method.  All
	** of the Java primitive types are covered as well as their wrapper classes.
	** All of the Java classes corresponding to the currently supported SQL
	** types are covered.
	*/

	public String parmType(byte value)
	{
		return "byte parameter";
	}

	public String parmType(byte[][][] value)
	{
		return "byte[][][] parameter";
	}

	public String parmType(Byte value)
	{
		return "java.lang.Byte parameter";
	}

	public String parmType(char value)
	{
		return "char parameter";
	}

	public String parmType(Character value)
	{
		return "java.lang.Character parameter";
	}

	public String parmType(double value)
	{
		return "double parameter";
	}

	public String parmType(Double value)
	{
		return "java.lang.Double parameter";
	}

	public String parmType(BigDecimal value)
	{
		return "java.math.BigDecimal parameter";
	}

	public String parmType(float value)
	{
		return "float parameter";
	}

	public String parmType(Float value)
	{
		return "java.lang.Float parameter";
	}

	public String parmType(int value)
	{
		return "int parameter";
	}

	public String parmType(Integer value)
	{
		return "java.lang.Integer parameter";
	}

	public String parmType(long value)
	{
		return "long parameter";
	}

	public String parmType(Long value)
	{
		return "java.lang.Long parameter";
	}

	public String parmType(short value)
	{
		return "short parameter";
	}

	public String parmType(Short value)
	{
		return "java.lang.Short parameter";
	}

	public String parmType(boolean value)
	{
		return "boolean parameter";
	}

	public String parmType(Boolean value)
	{
		return "java.lang.Boolean parameter";
	}

	public String parmType(String value)
	{
		return "java.lang.String parameter";
	}

	public String parmType(Date value)
	{
		return "java.sql.Date parameter";
	}

	public String parmType(Time value)
	{
		return "java.sql.Time parameter";
	}

	public String parmType(Timestamp value)
	{
		return "java.sql.Timestamp parameter";
	}

	/*
	** The following methods return all of the java primitive types and
	** their wrapper classes, plus all of the types corresponding to the
	** built-in SQL types.
	*/
	public byte byteMethod()
	{
		return 1;
	}

	public byte[][][] byteArrayArrayArrayMethod()
	{
		return new byte[3][][];
	}

	public Byte ByteMethod()
	{
		return new Byte((byte) 1);
	}

	public char charMethod()
	{
		return 'a';
	}

	public Character CharacterMethod()
	{
		return new Character('a');
	}

	public double doubleMethod()
	{
		return 1.5;
	}

	public Double DoubleMethod()
	{
		return new Double(1.5);
	}

	public BigDecimal BigDecimalMethod()
	{
		return new BigDecimal(1.4d);
	}

	public float floatMethod()
	{
		return 2.5F;
	}

	public Float FloatMethod()
	{
		return new Float(2.5F);
	}

	public int intMethod()
	{
		return 2;
	}

	public Integer IntegerMethod()
	{
		return new Integer(2);
	}

	public long longMethod()
	{
		return 3L;
	}

	public Long LongMethod()
	{
		return new Long(3L);
	}

	public short shortMethod()
	{
		return (short) 4;
	}

	public Short ShortMethod()
	{
		return new Short((short) 4);
	}

	public boolean booleanMethod()
	{
		return true;
	}

	public Boolean BooleanMethod()
	{
		return new Boolean(true);
	}

	public String StringMethod()
	{
		return "A String";
	}

	public Date DateMethod()
	{
		/* July 2, 1997 */
		// deprecated...note, that it was actually august, not july.
		// return new Date(97, 7, 2);
		return new Date(870505200000L);
	}

	public Time TimeMethod()
	{
		/* 10:58:33 AM */
		// deprecated...
		// return new Time(10, 58, 33);
		return new Time(68313000L);
	}

	public Timestamp TimestampMethod()
	{
		/* July 2, 1997 10:59:15.0 AM */
		// deprecated...note, actually August, not July, 1997
		// return new Timestamp(97, 7, 2, 10, 59, 15, 0);
		return new Timestamp(870544755000L);
	}

	public ManyMethods ManyMethodsMethod()
	{
		return this;
	}

	/*
	** The following methods are for testing null arguments.  These methods
	** return Strings with the names of the parameter types, so we can be
	** sure the right method was called.
	*/
	public String isNull(Boolean value)
	{
		if (value == null)
			return "Boolean is null";
		else
			return "Boolean is not null";
	}

	public String isNull(String value)
	{
		if (value == null)
			return "String is null";
		else
			return "String is not null";
	}

	public String isNull(Double value)
	{
		if (value == null)
			return "Double is null";
		else
			return "Double is not null";
	}

	public String isNull(BigDecimal value)
	{
		if (value == null)
			return "BigDecimal is null";
		else
			return "BigDecimal is not null";
	}

	public String isNull(Integer value)
	{
		if (value == null)
			return "Integer is null";
		else
			return "Integer is not null";
	}

	public String isNull(Float value)
	{
		if (value == null)
			return "Float is null";
		else
			return "Float is not null";
	}

	public String isNull(Short value)
	{
		if (value == null)
			return "Short is null";
		else
			return "Short is not null";
	}

	public String isNull(Date value)
	{
		if (value == null)
			return "Date is null";
		else
			return "Date is not null";
	}

	public String isNull(Time value)
	{
		if (value == null)
			return "Time is null";
		else
			return "Time is not null";
	}

	public String isNull(Timestamp value)
	{
		if (value == null)
			return "Timestamp is null";
		else
			return "Timestamp is not null";
	}

	/* Methods with more than one parameter */
	public String integerFloatDouble(Integer parm1, Float parm2, Double parm3)
	{
		return "integerFloatDouble method";
	}

	public String stringDateTimeTimestamp(String parm1, Date parm2, Time parm3,
											Timestamp parm4)
	{
		return "stringDateTimeTimestamp method";
	}

	/* Static methods */
	public static int staticMethod()
	{
		return 1;
	}

	public static int overloadedStaticMethod()
	{
		return 1;
	}

	public static int overOverloadedStaticMethod()
	{
		return 1;
	}

	public static Byte staticByteMethod()
	{
		return new Byte((byte) 1);
	}

	public static Character staticCharacterMethod()
	{
		return new Character('a');
	}

	public static Double staticDoubleMethod()
	{
		return new Double(1.5);
	}

	public static BigDecimal staticBigDecimalMethod()
	{
		return new BigDecimal(1.1d);
	}

	public static Float staticFloatMethod()
	{
		return new Float(2.5F);
	}

	public static Long staticLongMethod()
	{
		return new Long(3L);
	}

	public static Short staticShortMethod()
	{
		return new Short((short) 4);
	}

	public static Integer staticIntegerMethod()
	{
		return new Integer(2);
	}

	public static Boolean staticBooleanMethod()
	{
		return new Boolean(true);
	}

	public static String staticStringMethod()
	{
		return "A String";
	}

	public static Date staticDateMethod()
	{
		/* July 2, 1997 */
		return new Date(97, 7, 2);
	}

	public static Time staticTimeMethod()
	{
		/* 10:58:33 AM */
		return new Time(10, 58, 33);
	}

	public static Timestamp staticTimestampMethod()
	{
		/* July 2, 1997 10:59:15.0 AM */
		return new Timestamp(97, 7, 2, 10, 59, 15, 0);
	}

	public static ManyMethods staticManyMethods(Integer value)
	{
		return new ManyMethods(value.intValue());
	}

	/* "Cast to sub class" */
	public SubClass subClass()
	{
		if (this instanceof SubClass)
		{
			return (SubClass) this;
		}
		else
		{
			return null;
		}
	}

	public int[] getIntArray() {
		return new int[0];
	}

	public Object[] getObjectArray() {
		return new String[0];
	}

	/* Methods for negative testing */
	protected int protectedMethod()
	{
		return 1;
	}

	private int privateMethod()
	{
		return 1;
	}

	int packageMethod()
	{
		return 1;
	}

	public int exceptionMethod() throws Throwable
	{
		throw new Throwable("This exception should be caught by the runtime system.");
	}

	/*
	** Some methods for testing interface resolution
	*/

	public static NoMethodInterface getNoMethodInterface() {
		return new SubInterfaceClass(67);
	}
	public static Runnable getRunnable() {
		return new SubInterfaceClass(89);
	}
	public static ExtendingInterface getExtendingInterface() {
		return new SubInterfaceClass(235);
	}
}
