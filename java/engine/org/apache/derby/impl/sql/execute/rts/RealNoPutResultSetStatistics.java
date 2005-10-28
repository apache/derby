/*

   Derby - Class org.apache.derby.impl.sql.execute.rts.RealNoPutResultSetStatistics

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute.rts;

import org.apache.derby.iapi.services.io.Formatable;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
  ResultSetStatistics implemenation for NoPutResultSetImpl.

  @author jerry

*/
abstract class RealNoPutResultSetStatistics 
	extends RealBasicNoPutResultSetStatistics
{
	/* Leave these fields public for object inspectors */
	public int resultSetNumber;

	/* fields used for formating run time statistics output */
	protected String indent;
	protected String subIndent;
	protected int sourceDepth;

	// CONSTRUCTORS

	/**
	 * 
	 *
	 */
    public	RealNoPutResultSetStatistics(
										int numOpens,
										int rowsSeen,
										int rowsFiltered,
										long constructorTime,
										long openTime,
										long nextTime,
										long closeTime,
										int resultSetNumber,
										double optimizerEstimatedRowCount,
										double optimizerEstimatedCost
										)
	{
		super(
				numOpens,
				rowsSeen,
				rowsFiltered,
				constructorTime,
				openTime,
				nextTime,
				closeTime,
				optimizerEstimatedRowCount,
				optimizerEstimatedCost
				);

		this.resultSetNumber = resultSetNumber;
	}
 
	/**
	 * Initialize the format info for run time statistics.
	 */
	protected void initFormatInfo(int depth)
	{
		char[] indentchars = new char[depth];
		char[] subIndentchars = new char[depth + 1];
		sourceDepth = depth + 1;

		/*
		** Form an array of tab characters for indentation.
		*/
		subIndentchars[depth] = '\t';
		while (depth > 0)
		{
			subIndentchars[depth - 1] = '\t';
			indentchars[depth - 1] = '\t';
			depth--;
		}
                // convert char[] to String to avoid problems during 
                // String concatenation.
                indent = new String(indentchars);
                subIndent = new String(subIndentchars);
	}
}
