/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.log
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.log;


/**

A single buffer of data.

**/

final class LogAccessFileBuffer
{
    /**
       IBM Copyright &copy notice.
    */
    public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;


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
