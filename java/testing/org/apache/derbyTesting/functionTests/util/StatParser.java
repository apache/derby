/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;

/**
 * Utilities for parsing runtimestats
 *
 * RESOLVE: This class should be internationalized.
 */
public class StatParser
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
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
