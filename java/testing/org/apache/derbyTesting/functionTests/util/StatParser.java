/*

   Derby - Class org.apache.derbyTesting.functionTests.util.StatParser

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

/**
 * Utilities for parsing runtimestats
 *
 * RESOLVE: This class should be internationalized.
 */
public class StatParser
{
	public static String getScanCols(String runTimeStats)
		throws Throwable
	{
		if (runTimeStats == null)
		{
			return "The RunTimeStatistics string passed in is null";
		}

		int startIndex;
		int endIndex = 0;
		int indexIndex;

		StringBuffer strbuf = new StringBuffer();

		/*
		** We need to know if we used an index
		*/
		if ((indexIndex = runTimeStats.indexOf("Index Scan ResultSet")) != -1)
		{
			int textend = runTimeStats.indexOf("\n", indexIndex);
			strbuf.append(runTimeStats.substring(indexIndex, textend+1));
		}
		else
		{
			strbuf.append("TableScan\n");
		}

		int count = 0;
		while ((startIndex = runTimeStats.indexOf("Bit set of columns fetched", endIndex)) != -1)
		{
			count++;
			endIndex = runTimeStats.indexOf("}", startIndex);
			if (endIndex == -1)
			{
				endIndex = runTimeStats.indexOf("All", startIndex);
				if (endIndex == -1)
				{
					throw new Throwable("couldn't find the closing } on "+
						"columnFetchedBitSet in "+runTimeStats);
				}
				endIndex+=5;
			}
			else
			{
				endIndex++;
			}
			strbuf.append(runTimeStats.substring(startIndex, endIndex));
			strbuf.append("\n");
		}
		if (count == 0)
		{
			throw new Throwable("couldn't find string 'Bit set of columns fetched' in :\n"+
				runTimeStats);
		}

		return strbuf.toString();
	}
}	
