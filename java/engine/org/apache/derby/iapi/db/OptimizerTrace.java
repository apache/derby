/*

   Derby - Class org.apache.derby.iapi.db.OptimizerTrace

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

/**
  <P>
  This  class provides static methods for controlling the
  optimizer tracing in a Cloudscape database.
  
  <P>
  <i>
  Cloudscape reserves the right to change, rename, or remove this interface
  at any time. </i>
  */
public class OptimizerTrace
{
	/**
	 * Control whether or not optimizer trace is on.
	 *
	 * @param onOrOff    Whether to turn optimizer trace on (true) or off (false).
	 *
	 * @return Whether or not the call was successful.  (false will be returned when optimizer tracing is not supported.)
	 */
	public static boolean setOptimizerTrace(boolean onOrOff)
	{
		boolean retCode = false;

		try
		{
			// Get the current language connection context.  This is associated
			// with the current database.
			LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
			retCode = lcc.setOptimizerTrace(onOrOff);
		}
		catch (Throwable t)
		{
			// eat all exceptions, simply return false
		}

		return retCode;
	}

	/**
	 * Control whether or not optimizer trace is generated in html.
	 *
	 * @param onOrOff    Whether or not optimizer trace will be in html (true) or not (false).
	 *
	 * @return Whether or not the call was successful.  (false will be returned when optimizer tracing is not supported.)
	 */
	public static boolean setOptimizerTraceHtml(boolean onOrOff)
	{
		boolean retCode = false;

		try
		{
			// Get the current language connection context.  This is associated
			// with the current database.
			LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
			retCode = lcc.setOptimizerTraceHtml(onOrOff);
		}
		catch (Throwable t)
		{
			// eat all exceptions, simply return false
		}

		return retCode;
	}

	/**
	 * Get the optimizer trace output for the last optimized query as a String.  If optimizer trace
	 * html is on, then the String will contain the html tags.
	 *
	 * @return The optimizer trace output for the last optimized query as a String.
	 *    Null will be returned if optimizer trace output is off or not supported 
	 *    or no trace output was found or an exception occurred.
	 */
	public static String getOptimizerTraceOutput()
	{
		String retCode = null;

		try
		{
			// Get the current language connection context.  This is associated
			// with the current database.
			LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
			retCode = lcc.getOptimizerTraceOutput();
		}
		catch (Throwable t)
		{
			// eat all exceptions, simply return null
		}

		return retCode;
	}

	/**
	 * Send the optimizer trace output for the last optimized query to a file with a .html extension.  
	 * If optimizer trace html is on, then the output will contain the html tags.
	 *
	 * @param fileName    The name of the file to write to.  (.html extension will be added.)
	 *
	 * @return Whether or not the request was successful.
	 *    false mayl be returned for a number of reasons, including if optimizer trace output is off or not supported 
	 *    or no trace output was found or an exception occurred.
	 */
	public static boolean writeOptimizerTraceOutputHtml(String fileName)
	{
		boolean retCode = true;

		try
		{
		String output = getOptimizerTraceOutput();
		//RESOLVEOPTIMIZERTRACE - need to write out the html
		}
		catch (Throwable t)
		{
			// eat all exceptions, simply return false
			retCode = false;
		}

		return retCode;
	}

}
