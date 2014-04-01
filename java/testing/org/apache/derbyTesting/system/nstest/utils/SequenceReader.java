/*
 
 Derby - Class org.apache.derbyTesting.system.nstest.utils.SequenceReader
 
 Licensed to the Apache Software Foundation (ASF) under one or more
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

package org.apache.derbyTesting.system.nstest.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import org.apache.derbyTesting.system.nstest.NsTest;

/**
 * SequenceReader - a background thread that checks the state of the sequence counter.
 */
public class SequenceReader extends Thread
{
    private Connection  conn;
	private int delay = 60000;
    

	public boolean stopNow = false;

	public SequenceReader( Connection connection, int num )
    {
        conn = connection;
		delay = num;
	}

	/*
	 * Implementation of run() method to check the sequence counter.
	 * 
	 */
	public void run() {
        NsTest.logger.println( "Starting the sequence reader thread with delay = " + delay );
		while (stopNow == false)
        {
			try {
				readSequenceValue();
				sleep( delay );
                
				// first check if there are still active tester threads.
				if (NsTest.numActiveTestThreads() != 0 && NsTest.numActiveTestThreads() > 1)
				{
					continue;
				}
				else
				{
					NsTest.logger.println("no more test threads, finishing SequenceReader thread also");
                    readSequenceValue();
					stopNow=true;
				}
			} catch (java.lang.InterruptedException ie) {
				NsTest.logger.println("SequenceReader: unexpected error in sleep");
			}
		}
	}

	/*
	 * Print the current memory status
	 */
	private void readSequenceValue()
    {
        try {
            PreparedStatement   ps = conn.prepareStatement
                ( "values syscs_util.syscs_peek_at_sequence( 'NSTEST', 'NSTESTTAB_SEQ' )" );
            ResultSet       rs = ps.executeQuery();
            rs.next();
            long    nextSequenceValue = rs.getLong( 1 );
            NsTest.logger.println( "Next sequence number = " + nextSequenceValue );
            NsTest.updateSequenceTracker( nextSequenceValue );
            rs.close();
            ps.close();
        }
        catch (Exception e)
        {
            NsTest.printException( SequenceReader.class.getName(), e );
        }
	}

}

