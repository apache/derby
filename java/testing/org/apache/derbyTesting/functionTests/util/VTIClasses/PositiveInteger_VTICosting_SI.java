/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util.VTIClasses
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util.VTIClasses;

import org.apache.derby.vti.VTICosting;
import org.apache.derby.vti.VTIEnvironment;

import java.io.InputStream;

import java.math.BigDecimal;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * A class that takes an Integer as a parameter.
 * This class implements the optional static getResultSetMetaData()
 * method.
 */
public class PositiveInteger_VTICosting_SI extends AbstractAllNegativeNoStatic implements VTICosting
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	private boolean returnedRow = false;
	private Integer integer;
	private int[]	results;
	private int		currentElem;
	private boolean supportsMultipleInstantiations;

	public PositiveInteger_VTICosting_SI(int length, 
										 boolean supportsMultipleInstantiations) 
		throws SQLException
	{
		// call the non-public constructor of the super class
		// which does not throw an exception
		super(1);
		this.supportsMultipleInstantiations = supportsMultipleInstantiations;
		results = new int[(length > 0) ? length : 0];
		for (int index = 0; index < length; index++)
		{
			results[index] = 1 + (index * 2);
		}
		currentElem = -1;
	}

	// java.sql.ResultSet interface

	public ResultSetMetaData getMetaData()
		throws SQLException
	{
		int[] nullable = new int[1];
		String[] columnName = new String[1];
		int[] precision = new int[1];
		int[] scale = new int[1];
		int[] columnType = new int[1];
		int[] columnDisplaySize = new int[1];

		// 1 column - non-nullable integer
		nullable[0] = ResultSetMetaData.columnNoNulls;
		columnName[0] = "Column1";
		precision[0] = 0;
		scale[0] = 0;
		columnType[0] = Types.INTEGER;
		columnDisplaySize[0] = 4;

		// Return RSMD with 1 column
		return new ResultSetMetaDataPositive(
						1,	// columnCount
						nullable,
						columnName,
						precision,
						scale,
						columnType,
						columnDisplaySize
					);
	}

	public boolean next()
	{
		currentElem++;
		return (currentElem < results.length);
	}

	public void close()
	{
	}

	public boolean wasNull()
	{
		return false;
	}

	public int getInt(int columnNumber)
	{
		return results[currentElem];
	}

	// org.apache.derby.iapi.db.VTICosting interface
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
		throws SQLException
	{
		// return the default if length is 0
		return (results.length == 0) ? VTICosting.defaultEstimatedRowCount : (double) results.length;
	}

	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
		throws SQLException
	{
		// return the default if length is 0
		return (results.length == 0) ? VTICosting.defaultEstimatedCost : 1d;
	}

	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
		throws SQLException
	{
		return supportsMultipleInstantiations;
	}
}
