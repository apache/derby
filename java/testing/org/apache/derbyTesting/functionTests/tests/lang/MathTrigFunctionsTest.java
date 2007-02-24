/* Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class MathTrigFunctionsTest extends BaseJDBCTestCase {

	private static final boolean debugFlag = false;

	private static final double SMALLEST_NEG_DERBY_DOUBLE = -1.79769E+308;

	private static final double SMALL_NEG_DOUBLE = -1.79768E+308;

	private static final double SMALLEST_POS_DERBY_DOUBLE = 2.225E-307;

	private static final double LARGEST_POS_DERBY_DOUBLE = 1.79769E+308;

	private static final double LARGEST_NEG_DERBY_DOUBLE = -2.225E-307;

	private static final double[] testRadians = { -0.000000001, -0.25,
			0.000000001, 0.25, 0.5, 0.0, 1.0, 2.0, java.lang.StrictMath.PI,
			java.lang.StrictMath.PI };

	private static final double[] testArcValues = { 0.000000001, -0.000000001,
			0.25, -0.25, 0.5, 0.0, -0.0, 1.0, -1.0 };

	private static final double[] logValues = { 0.000000001, 0.25, 0.5, 1.0,
			45.0, 90.0, 135.0, 180.0, 270, SMALLEST_POS_DERBY_DOUBLE,
			LARGEST_POS_DERBY_DOUBLE };

	private static final double[] testValues = { SMALLEST_NEG_DERBY_DOUBLE,
			SMALL_NEG_DOUBLE, SMALLEST_POS_DERBY_DOUBLE,
			LARGEST_POS_DERBY_DOUBLE, LARGEST_NEG_DERBY_DOUBLE, 0.000000001,
			-0.000000001, 0.25, -0.25, 0.5, 0.0, -0.0, 1.0, -1.0, 2.0, 3.0,
			java.lang.StrictMath.PI, 2 * java.lang.StrictMath.PI, 4.0, 45.0,
			90.0, 135.0, 180.0, 270 };

	private static final double[] testValuesTwo = { SMALLEST_NEG_DERBY_DOUBLE,
			SMALL_NEG_DOUBLE, SMALLEST_POS_DERBY_DOUBLE,
			LARGEST_NEG_DERBY_DOUBLE, 0.000000001, -0.000000001, 0.25, -0.25,
			0.5, 0.0, -0.0, 1.0, -1.0, 2.0, 3.0, java.lang.StrictMath.PI,
			2 * java.lang.StrictMath.PI, 4.0, 45.0, 90.0, 135.0, 180.0, 270 };

	/**
	 * Tests the ACOS function which returns the arc cosine of a specified
	 * number.
	 * <p>
	 * Acceptable input values to the ACOS function are DOUBLE PRECISION values
	 * from -1 to 1. NULL returns NULL and if the absolute value of the input is
	 * greater than 1 an SQL state of 22003 is returned.
	 * <p>
	 * The return value from the ACOS function is a DOUBLE PRECISION number in
	 * radians with a range of zero to PI.
	 * 
	 */
	public void testAcos() throws SQLException {
		// test the case where the input value is null
		executeNullValues("ACOS");
		executeNullFn("ACOS");
		debug();
        
        PreparedStatement ps =
            prepareStatement("VALUES ACOS(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn ACOS(?)}");
         
		for (int i = 0; i < testArcValues.length; i++) {
			double expected = java.lang.StrictMath.acos(testArcValues[i]);
			double rValue = getValue(ps, testArcValues[i]);
			debug("ACOS: input value: " + testArcValues[i]
					+ " expected value: " + expected + " return value: "
					+ rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testArcValues[i]);
			assertEquals(expected, fValue, 0.0);

		}
		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.acos(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		/* test the case where the input value is out of range */
		try {
			getValue(ps, 2.0);
			fail("ACOS: Out of range test failed, input value is: " + 2.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

		/* test the case where the input value is out of range */
		try {
			getValue(psFN, 2.0);
			fail("ACOS: Out of range test failed, input value is: " + 2.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the ASIN function which returns the arc sine of a specified number.
	 * <p>
	 * Acceptable input values to the ASIN function are DOUBLE PRECISION values
	 * from -1 to 1.
	 * <p>
	 * If the specified number is zero (0), the result of this function is zero.
	 * Note: Derby does not support negative zero.
	 * <p>
	 * An input value of NULL returns NULL.
	 * <p>
	 * If the absolute value of the input is greater than 1 an SQL state of
	 * 22003 is returned.
	 * <p>
	 * The return value from the ASIN function is a DOUBLE PRECISION number in
	 * radians with a range of -PI/2 to PI/2.
	 * 
	 */
	public void testAsin() throws SQLException {
		executeNullValues("ASIN");
		executeNullFn("ASIN");
		debug();
        PreparedStatement ps =
            prepareStatement("VALUES ASIN(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn ASIN(?)}");

		for (int i = 0; i < testArcValues.length; i++) {
			double expected = java.lang.StrictMath.asin(testArcValues[i]);
			double rValue = getValue(ps, testArcValues[i]);
			debug("ASIN: input value: " + testArcValues[i]
					+ " expected value: " + expected + " return value: "
					+ rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testArcValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.asin(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		try {
			getValue(ps, 2.0);
			fail("ASIN: Out of range test failed, input value is: " + 2.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(psFN, 2.0);
			fail("ASIN: Out of range test failed, input value is: " + 2.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the ATAN function which returns the arc tangent of a specified
	 * number. Acceptable input values to the ATAN function are DOUBLE PRECISION
	 * values.
	 * <p>
	 * If the specified number is zero (0), the result of this function is zero.
	 * An input value of NULL returns NULL.
	 * <p>
	 * The return value from the ATAN function is a DOUBLE PRECISION number in
	 * radians with a range of -PI/2 to PI/2.
	 * 
	 */
	public void testAtan() throws SQLException {
		executeNullValues("ATAN");
		executeNullFn("ATAN");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES ATAN(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn ATAN(?)}");

        for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.atan(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("ATAN: input value: " + testValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.atan(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}
        
        ps.close();
        psFN.close();

	}

	/**
	 * Tests the COS function which returns the cosine of a specified number.
	 * <p>
	 * Acceptable input values to the COS function are DOUBLE PRECISION values.
	 * <p>
	 * An input value of NULL returns NULL.
	 */
	public void testCos() throws SQLException {
		executeNullValues("COS");
		executeNullFn("COS");
		debug();
        PreparedStatement ps =
            prepareStatement("VALUES COS(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn COS(?)}");
        
		for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.cos(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("COS: input value: " + testValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.cos(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the SIN function which returns the sine of a specified number.
	 * <p>
	 * Acceptable input values to the SIN function are DOUBLE PRECISION values.
	 * <p>
	 * An input value of NULL returns NULL.
	 * <p>
	 * If the argument is zero, then the result is zero.
	 * <p>
	 * The data type of the returned value is a DOUBLE PRECISION number.
	 */
	public void testSin() throws SQLException {
		executeNullValues("SIN");
		executeNullFn("SIN");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES SIN(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn SIN(?)}");
		for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.sin(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("SIN: input value: " + testValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.sin(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the TAN function which returns the tangent of a specified number.
	 * <p>
	 * Acceptable input values to the TAN function are DOUBLE PRECISION values.
	 * <p>
	 * An input value of NULL returns NULL.
	 * <p>
	 * If the argument is zero, then the result is zero.
	 * <p>
	 * The data type of the returned value is a DOUBLE PRECISION number.
	 */
	public void testTan() throws SQLException {
		executeNullValues("TAN");

		executeNullFn("TAN");

		debug();
        
        PreparedStatement ps =
            prepareStatement("VALUES TAN(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn TAN(?)}");
        
		for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.tan(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("TAN: input value: " + testValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.tan(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the PI function which returns a value that is closer than any other
	 * value to pi.
	 * <p>
	 * The constant pi is the ratio of the circumference of a circle to the
	 * diameter of a circle.
	 * <p>
	 * No input values are allowed for the PI function.
	 */

	public void testPI() throws SQLException {
		double value = executeValues("PI");
		assertEquals(java.lang.StrictMath.PI, value, 0.0);
		double fValue = executeFn("PI");
		assertEquals(java.lang.StrictMath.PI, fValue, 0.0);

		try {
			executeValues("PI", 2.0);
			fail("PI: Out of range test failed, input value is: " + 2.0);
		} catch (SQLException sqlE) {
			// '<statement>' is not recognized as a function or procedure.
			assertSQLState("42Y03", sqlE);
		}

		try {
			executeFn("PI", 2.0);
			fail("PI: Out of range test failed, input value is: " + 2.0);
		} catch (SQLException sqlE) {
			// '<statement>' is not recognized as a function or procedure.
			assertSQLState("42Y03", sqlE);
		}

	}

	/**
	 * Tests the DEGREES function which converts a DOUBLE PRECISION number from
	 * radians to degrees.
	 * <p>
	 * The input is an angle measured in radians, which is converted to an
	 * approximately equivalent angle measured in degrees.
	 * <p>
	 * The conversion from radians to degrees is not exact. You should not
	 * expect that the COS(DEGREES(PI/2)) to exactly equal 0.0.
	 * <p>
	 * The return value is a DOUBLE PRECISION number.
	 */
	public void testDegrees() throws SQLException {
		executeNullValues("DEGREES");
		executeNullFn("DEGREES");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES DEGREES(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn DEGREES(?)}");
        
		for (int i = 0; i < testRadians.length; i++) {
			double expected = java.lang.StrictMath.toDegrees(testRadians[i]);
			double rValue = getValue(ps, testRadians[i]);
			// rValue = executeValues("DEGREES", SMALL_NEG_DOUBLE);
			debug("DEGREES: input value: " + testRadians[i]
					+ " expected value: " + expected + " return value: "
					+ rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testRadians[i]);
			assertEquals(expected, fValue, 0.0);

		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.toDegrees(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		try {
			getValue(ps, SMALLEST_NEG_DERBY_DOUBLE);
			fail("DEGREES: Out of range test failed, input value is: "
					+ SMALLEST_NEG_DERBY_DOUBLE);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(psFN, SMALLEST_NEG_DERBY_DOUBLE);
			fail("DEGREES: Out of range test failed, input value is: "
					+ SMALLEST_NEG_DERBY_DOUBLE);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
        
        ps.close();
        psFN.close();
	}

	/**
	 * Tests the RADIANS function which converts a DOUBLE PRECISION number from
	 * degrees to radians.
	 * <p>
	 * The input is an angle measured in degrees, which is converted to an
	 * approximately equivalent angle measured in radians.
	 * <p>
	 * The conversion from radians to degrees is not exact. You should not
	 * expect that the COS(RADIANS(90.0)) to exactly equal 0.0.
	 * <p>
	 * The return value is a DOUBLE PRECISION number.
	 */
	public void testRadians() throws SQLException {
		executeNullValues("RADIANS");

		executeNullFn("RADIANS");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES RADIANS(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn RADIANS(?)}");
        
		for (int i = 0; i < testArcValues.length; i++) {
			double expected = java.lang.StrictMath.toRadians(testArcValues[i]);
			double rValue = getValue(ps, testArcValues[i]);
			debug("RADIANS: input value: " + testArcValues[i]
					+ " expected value: " + expected + " return value: "
					+ rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testArcValues[i]);
			assertEquals(expected, fValue, 0.0);

		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.toRadians(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		try {
			getValue(ps, SMALLEST_POS_DERBY_DOUBLE);
			fail("RADIANS: Out of range test failed, input value is: "
					+ SMALLEST_NEG_DERBY_DOUBLE);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(psFN, SMALLEST_POS_DERBY_DOUBLE);
			fail("RADIANS: Out of range test failed, input value is: "
					+ SMALLEST_NEG_DERBY_DOUBLE);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
        ps.close();
        psFN.close();
	}

	/**
	 * Tests the EXP function which returns e raised to the power of the input
	 * DOUBLE PRECISION number. The input number is the exponent to raise e to.
	 * <p>
	 * The constant e is the base of the natural logarithms.
	 * <p>
	 * The return value is a DOUBLE PRECISION number.
	 * 
	 * @throws SQLException
	 */
	public void testExp() throws SQLException {
		executeNullValues("EXP");
		executeNullFn("EXP");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES EXP(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn EXP(?)}");
        
		for (int i = 0; i < testValuesTwo.length; i++) {
			double expected = java.lang.StrictMath.exp(testValuesTwo[i]);
			double rValue = getValue(ps, testValuesTwo[i]);
			debug("EXP: input value: " + testValuesTwo[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValuesTwo[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.exp(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		try {
			getValue(ps, LARGEST_POS_DERBY_DOUBLE);
			fail("EXP: Out of range test failed, input value is: "
					+ LARGEST_POS_DERBY_DOUBLE);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(psFN, LARGEST_POS_DERBY_DOUBLE);
			fail("EXP: Out of range test failed, input value is: "
					+ LARGEST_POS_DERBY_DOUBLE);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the LOG10 function which returns the base-10 logarithm of a DOUBLE
	 * PRECISION number that is greater than zero.
	 * <p>
	 * If the input value is NULL, the result of this function is NULL.
	 * <p>
	 * If the input value is zero or a negative number, an exception is returned
	 * that indicates that the value is out of range (SQL state 22003).
	 * <p>
	 * The return type is a DOUBLE PRECISION number.
	 */

	public void testLog10() throws SQLException {
		executeNullValues("LOG10");
		executeNullFn("LOG10");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES LOG10(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn LOG10(?)}");
		for (int i = 0; i < logValues.length; i++) {
			// ln 10 = y * (log base 10 (10))
			// 2.3025850929940456840179914546844 = y * 1
			double expected = java.lang.StrictMath.log(logValues[i]) / 2.3025850929940456840179914546844;
			double rValue = getValue(ps, logValues[i]);
			debug("LOG10: input value: " + logValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, logValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.log(randD) / 2.3025850929940456840179914546844;
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		try {
			getValue(ps, 0.0);
			fail("LOG10: Out of range test failed, input value is: " + 0.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(ps, -1.0);
			fail("LOG10: Out of range test failed, input value is: " + -1.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

		try {
			getValue(psFN, 0.0);
			fail("LOG10: Out of range test failed, input value is: " + 0.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(psFN, -1.0);
			fail("LOG10: Out of range test failed, input value is: " + -1.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the LOG function which returns the natural logarithm (base e) of a
	 * DOUBLE PRECISION number that is greater than zero (0).
	 * <p>
	 * If the specified number is NULL, the result of these functions is NULL.
	 * If the specified number is zero or a negative number, an exception is
	 * returned that indicates that the value is out of range (SQL state 22003).
	 * <p>
	 * The data type of the returned value is a DOUBLE PRECISION number.
	 */
	public void testLog() throws SQLException {
		executeNullValues("LOG");
		executeNullFn("LOG");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES LOG(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn LOG(?)}");
        
		for (int i = 0; i < logValues.length; i++) {
			double expected = java.lang.StrictMath.log(logValues[i]);
			double rValue = getValue(ps, logValues[i]);
			debug("LOG: input value: " + logValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, logValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.log(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

		try {
			getValue(ps, 0.0);
			fail("LOG: Out of range test failed, input value is: " + 0.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
		try {
			getValue(psFN, 0.0);
			fail("LOG: Out of range test failed, input value is: " + 0.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}
        
        ps.close();
        psFN.close();

	}

	/**
	 * Tests the LN function which returns the natural logarithm (base e) of a
	 * DOUBLE PRECISION number that is greater than zero (0).
	 * <p>
	 * If the specified number is NULL, the result of these functions is NULL.
	 * If the specified number is zero or a negative number, an exception is
	 * returned that indicates that the value is out of range (SQL state 22003).
	 * <p>
	 * The data type of the returned value is a DOUBLE PRECISION number.
	 */
	public void testLn() throws SQLException {
		executeNullValues("LN");
		// Note: the syntax 'values {fn ln(value)}' is not supported
        // because it is not defined by JDBC.
		// Object fnVal = executeNullFn("LN");
		debug();
        PreparedStatement ps =
            prepareStatement("VALUES LN(?)");
		for (int i = 0; i < logValues.length; i++) {
			double expected = java.lang.StrictMath.log(logValues[i]);
			double rValue = getValue(ps, logValues[i]);
			debug("LOG: input value: " + logValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.log(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
		}

		try {
			getValue(ps, 0.0);
			fail("LOG: Out of range test failed, input value is: " + 0.0);
		} catch (SQLException sqlE) {
			// "ERROR 22003: The resulting value is outside the range for the
			// data type DOUBLE.";
			assertSQLState(
					SQLStateConstants.DATA_EXCEPTION_NUMERIC_VALUE_OUT_OF_RANGE,
					sqlE);
		}

        ps.close();
	}

	/**
	 * Tests the CEIL function which rounds a DOUBLE PRECISION number up, and
	 * return the smallest number that is greater than or equal to the input
	 * number.
	 * <p>
	 * If the input number is NULL, the result of these functions is NULL. If
	 * the input number is equal to a mathematical integer, the result of these
	 * functions is the same as the input number. If the input number is zero
	 * (0), the result of these functions is zero. If the input number is less
	 * than zero but greater than -1.0, then the result of these functions is
	 * zero.
	 * <p>
	 * The returned value is the smallest (closest to negative infinity) double
	 * floating point value that is greater than or equal to the specified
	 * number. The returned value is equal to a mathematical integer.
	 * <p>
	 * The data type of the returned value is a DOUBLE PRECISION number.
	 */

	public void testCeil() throws SQLException {
		executeNullValues("CEIL");

		// Note: the syntax 'values {fn CEIL(value)}' is not supported
        // because it is not specified by JDBC
		// Object fnVal = executeNullFn("CEIL");


		debug();
        PreparedStatement ps =
            prepareStatement("VALUES CEIL(?)");
        
		for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.ceil(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("CEIL: input value: " + testValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.ceil(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
		}

        ps.close();
	}

	/**
	 * Tests the CEILING function which rounds a DOUBLE PRECISION number up, and
	 * return the smallest number that is greater than or equal to the input
	 * number.
	 * <p>
	 * If the input number is NULL, the result of these functions is NULL. If
	 * the input number is equal to a mathematical integer, the result of these
	 * functions is the same as the input number. If the input number is zero
	 * (0), the result of these functions is zero. If the input number is less
	 * than zero but greater than -1.0, then the result of these functions is
	 * zero.
	 * <p>
	 * The returned value is the smallest (closest to negative infinity) double
	 * floating point value that is greater than or equal to the specified
	 * number. The returned value is equal to a mathematical integer.
	 * <p>
	 * The data type of the returned value is a DOUBLE PRECISION number.
	 */
	public void testCeiling() throws SQLException {
		executeNullValues("CEILING");

		executeNullFn("CEILING");
        
        PreparedStatement ps =
            prepareStatement("VALUES CEILING(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn CEILING(?)}");

		debug();
		for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.ceil(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("CEILING: input value: " + testValues[i]
					+ " expected value: " + expected + " return value: "
					+ rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.ceil(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}

        ps.close();
        psFN.close();
	}

	/**
	 * Tests the FLOOR function which rounds the input value which must be a
	 * DOUBLE PRECISION number down, and returns the largest number that is less
	 * than or equal to the input value.
	 * <p>
	 * If the input value is NULL, the result of this function is NULL. If the
	 * input value is equal to a mathematical integer, the result of this
	 * function is the same as the input number. If the input value is zero (0),
	 * the result of this function is zero.
	 * <p>
	 * The returned value is the largest (closest to positive infinity) double
	 * floating point value that is less than or equal to the input value. The
	 * returned value is equal to a mathematical integer. The data type of the
	 * returned value is a DOUBLE PRECISION number.
	 * 
	 * @throws SQLException
	 */

	public void testFloor() throws SQLException {
		executeNullValues("FLOOR");

		executeNullFn("FLOOR");

		debug();
        PreparedStatement ps =
            prepareStatement("VALUES FLOOR(?)");
        PreparedStatement psFN =
            prepareStatement("VALUES {fn FLOOR(?)}");
        
		for (int i = 0; i < testValues.length; i++) {
			double expected = java.lang.StrictMath.floor(testValues[i]);
			double rValue = getValue(ps, testValues[i]);
			debug("FLOOR: input value: " + testValues[i] + " expected value: "
					+ expected + " return value: " + rValue);
			assertEquals(expected, rValue, 0.0);
			double fValue = getValue(psFN, testValues[i]);
			assertEquals(expected, fValue, 0.0);
		}

		Random rand = new java.util.Random();
		for (int i = 0; i < 100; i++) {
			double randD = rand.nextDouble();
			double expect = java.lang.StrictMath.floor(randD);
			double rVal = getValue(ps, randD);
			assertEquals(expect, rVal, 0.0);
			double fVal = getValue(psFN, randD);
			assertEquals(expect, fVal, 0.0);

		}
        ps.close();
        psFN.close();
	}

	private double executeValues(String functionName) throws SQLException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("values " + functionName + "()");
		double rValue = 0.0;
		while (rs.next()) {
			rValue = rs.getDouble(1);
		}
		rs.close();
		stmt.close();
		return rValue;
	}

	private double executeValues(String functionName, double value)
			throws SQLException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("values " + functionName + "(" + value
				+ ")");
		double rValue = 0.0;
		while (rs.next()) {
			rValue = rs.getDouble(1);
		}
		rs.close();
		stmt.close();
		return rValue;
	}
    
    /**
     * Execute a prepared statement with a single double argument
     * and return the double value from the single row returned.
     */
    private double getValue(PreparedStatement ps, double value)
            throws SQLException {
        ps.setDouble(1, value);
        ResultSet rs = ps.executeQuery();
        rs.next(); // we know a single value will be returned.
        double rValue = rs.getDouble(1);
        rs.close();
        return rValue;
    }

	private void executeNullValues(String functionName) throws SQLException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("values " + functionName + "(null)");
		rs.next(); // we know a single value will be returned.
		assertNull(rs.getObject(1));
		assertTrue(rs.wasNull());
		rs.close();
		stmt.close();
	}

	private double executeFn(String functionName) throws SQLException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("values {fn " + functionName + "()}");
		double rValue = 0.0;
		while (rs.next()) {
			rValue = rs.getDouble(1);
		}
		rs.close();
		stmt.close();
		return rValue;
	}

	private double executeFn(String functionName, double value)
			throws SQLException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("values {fn  " + functionName + "("
				+ value + ")}");
		double rValue = 0.0;
		while (rs.next()) {
			rValue = rs.getDouble(1);
		}
		rs.close();
		stmt.close();
		return rValue;
	}

	private void executeNullFn(String functionName) throws SQLException {
		Statement stmt = createStatement();
		ResultSet rs = stmt.executeQuery("values {fn  " + functionName
				+ "(null)}");
        rs.next(); // we know a single value will be returned.
        assertNull(rs.getObject(1));
        assertTrue(rs.wasNull());
		rs.close();
		stmt.close();
	}

	private void debug(String message) {
		if (debugFlag) {
			System.out.println(message);
		}
	}

	private void debug() {
		if (debugFlag) {
			System.out.println();
		}
	}

	public MathTrigFunctionsTest(String name) {
		super(name);
	}

    /**
     * Runs the tests in the embedded and client server configuration
     * as the JDBC escape function testing is relevant for both drivers.
     */
	public static Test suite() {
        return TestConfiguration.defaultSuite(MathTrigFunctionsTest.class);
	}

}
