/*

   Derby - Class org.apache.derby.impl.store.raw.log.LogAccessFileBuffer

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

        init();
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */
    public void init()
    {
        bytes_free  = buffer.length;
        position    = 0;
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
