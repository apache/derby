/*

   Derby - Class org.apache.derby.iapi.db.OptimizerTrace

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.db;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import org.apache.derby.iapi.sql.compile.OptTrace;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.impl.sql.compile.DefaultOptTrace;

/**
  <P>
  This  class provides static methods for controlling the
  optimizer tracing in a Derby database.
  
  */
public class OptimizerTrace
{
	/**
	 * Turn default optimizer tracing on or off.
	 *
	 * @param onOrOff    Whether to turn optimizer tracing on (true) or off (false).
	 */
	public static void setOptimizerTrace( boolean onOrOff )
	{
        OptTrace    optimizerTracer = onOrOff ? new DefaultOptTrace() : null;

        setOptimizerTracer( optimizerTracer );
	}

	/**
	 * Install an optimizer tracer (to enable tracing) or uninstall the current optimizer tracer
     * (to disable tracing).
	 *
	 * @param tracer    Null if tracing is being turned off, otherwise an optimizer tracer
	 */
	public static   void setOptimizerTracer( OptTrace tracer )
	{
		try
		{
            ConnectionUtil.getCurrentLCC().setOptimizerTracer( tracer );
		}
		catch (Throwable t) {}
	}

	/**
	 * Get the current optimizer tracer, if any.
	 */
	public static   OptTrace getOptimizerTracer()
	{
		try
		{
            return ConnectionUtil.getCurrentLCC().getOptimizerTracer();
		}
		catch (Throwable t) { return null; }
	}


	/**
	 * Get the optimizer trace output for the last optimized query as a String.
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
            OptTrace    tracer = lcc.getOptimizerTracer();

            if ( tracer != null )
            {
                StringWriter    sw = new StringWriter();
                PrintWriter     pw = new PrintWriter( sw );

                tracer.printToWriter( pw );
                pw.flush();
                sw.flush();

                retCode = sw.toString();
            }
		}
		catch (Throwable t)
		{
			// eat all exceptions, simply return null
		}

		return retCode;
	}

}
