/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogAccessFileBuffer

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

package org.apache.derby.impl.store.raw.log;


/**

A single buffer of data.

**/

final class LogAccessFileBuffer
{


    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    protected byte[]    buffer;
    protected int       bytes_free;
    protected int       position;
	protected int       length;
    protected long      greatest_instant;

    LogAccessFileBuffer next;
    LogAccessFileBuffer prev;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public LogAccessFileBuffer(
    int size)
    {
        buffer      = new byte[size];
        prev        = null;
        next        = null;

        init(0);
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */
    public void init(int reserve)
    {
		length =  buffer.length - reserve;
        bytes_free  = length;
        position    = reserve;
        greatest_instant = -1;
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
