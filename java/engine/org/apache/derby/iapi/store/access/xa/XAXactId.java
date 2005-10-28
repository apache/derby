/*

   Derby - Class org.apache.derby.iapi.store.access.xa.XAXactId

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

package org.apache.derby.iapi.store.access.xa;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.GlobalXact;

import javax.transaction.xa.Xid;
import javax.transaction.xa.XAException;

/**

The XAXactId class is a specific implementation of the JTA Xid interface.  It
is only used by the TransactionTable.restore() interface to return an array
of Xid's back to the caller, as part of serving the XAresource.restore() 
interface.
<P>
It is NOT the object that is stored in the log.  One reason for this is that
the Formattable and Xid interface's define two different return values for
the getFormatId() interface.

**/

public class XAXactId extends GlobalXact implements Xid
{
    /**************************************************************************
     * Private Fields of the class
     **************************************************************************
     */
	private static final char COLON = ':';

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**
     * initialize by making array copies of appropriate fields.
     * <p>
     **/
    private void copy_init_xid(
							   int     format_id,
							   byte[]  global_id,
							   byte[]  branch_id)
    {
		this.format_id = format_id;
		this.global_id = new byte[global_id.length];
		System.arraycopy(global_id, 0, this.global_id, 0, global_id.length);
		this.branch_id = new byte[branch_id.length];
		System.arraycopy(branch_id, 0, this.branch_id, 0, branch_id.length);
    }
    
    /**
     * Construct from given pieces of an Xid.  Makes copies of arrays.
     * <p>
     **/
    public XAXactId(
					int     format_id,
					byte[]  global_id,
					byte[]  branch_id)
    {
        copy_init_xid(format_id, global_id, branch_id);
    }

    /**
     * Construct an Xid using an external Xid.
     * <p>
     * @exception XAException invalid external xid
     */
    public XAXactId(Xid xid) throws XAException
    {
		if (xid == null)
			throw new XAException(XAException.XAER_NOTA);
	
        copy_init_xid(
					  xid.getFormatId(),
					  xid.getGlobalTransactionId(),
					  xid.getBranchQualifier());
    }





    public String toHexString()
    {
		// the ascii representation of xid where xid is of 
		// 		format_id = f
		//		global_id = byte[N]
		//		branch_id = byte[M]
		//
		// :xx:yy:ffffffff:n...n:mmmm...m:
		// where	xx = N (up to 64 max)
		//			yy = M (up to 64 max)
		//			n..n = hex dump of global_id (0 to 128 bytes max)
		//			m..m = hex dump of branch_qualifier (0 to 128 bytes max)
	
	// 1+2+1+2+1+9+1+1+1
		int maxLength = 20+(global_id.length+branch_id.length)*2;
		
		StringBuffer hexdump = new StringBuffer(maxLength);
		hexdump.append(COLON).
			append(Integer.toString(global_id.length)).append(COLON).
			append(Integer.toString(branch_id.length)).append(COLON).
			append(Integer.toString(format_id, 16)).append(COLON).
			append(org.apache.derby.iapi.util.StringUtil.toHexString(global_id, 0, global_id.length)).append(COLON).
			append(org.apache.derby.iapi.util.StringUtil.toHexString(branch_id, 0, branch_id.length)).append(COLON);
	
		return hexdump.toString();

    }

    public XAXactId(String xactIdString)
    {
		// extract it in pieces delimited by COLON
		int start, end, length;
	
	// xx
		start = 1;
		end = xactIdString.indexOf(COLON, start);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(end != -1, "illegal string format");
	
		String xx = xactIdString.substring(start, end);
		int N = Integer.parseInt(xx);
	
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(N > 0 && N <= Xid.MAXGTRIDSIZE, "illegal gtrid size");
		}
	
	// yy
		start = end+1;			// skip the COLON
		end = xactIdString.indexOf(COLON, start);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(end != -1, "illegal string format");
	
		String yy = xactIdString.substring(start,end);
		int M = Integer.parseInt(yy);
	
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(M > 0 && N <= Xid.MAXBQUALSIZE, "illegal bqual size");
	
	// ffffffff
		start = end+1;			// skip the COLON
		end = xactIdString.indexOf(COLON, start);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(end != -1, "illegal string format");
	
		String f = xactIdString.substring(start,end);
		format_id = Integer.parseInt(f, 16);

	// n...n
		start = end+1;			// skip the COLON
		end = xactIdString.indexOf(COLON, start);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(end != -1, "illegal string format");
	
		global_id = org.apache.derby.iapi.util.StringUtil.fromHexString(xactIdString, start, (end-start));
	
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(global_id.length == N, "inconsistent global_id length");
			
	
	// m...m
		start = end+1;			// skip the COLON
		end = xactIdString.indexOf(COLON, start);
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(end != -1, "illegal string format");
	
		branch_id = org.apache.derby.iapi.util.StringUtil.fromHexString(xactIdString, start, (end-start));
	
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(branch_id.length == M, 
								 "inconsistent branch_id length, expect " + M + " got " +
								 branch_id.length);
	
    }



    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods implementing the Xid interface: 
     **************************************************************************
     */

    /**
     * Obtain the format id part of the Xid.
     * <p>
     *
     * @return Format identifier. O means the OSI CCR format.
     **/
    public int getFormatId()
    {
        return(format_id);
    }

    /**
     * Obtain the global transaction identifier part of XID as an array of 
     * bytes.
     * <p>
     *
	 * @return A byte array containing the global transaction identifier.
     **/
    public byte[] getGlobalTransactionId()
    {
        return(global_id);
    }

    /**
     * Obtain the transaction branch qualifier part of the Xid in a byte array.
     * <p>
     *
	 * @return A byte array containing the branch qualifier of the transaction.
     **/
    public byte[] getBranchQualifier()
    {
        return(branch_id);
    }



    public boolean equals(Object other) 
    {
		if (other == this)
			return true;

		if (other == null)
			return false;
	
		try
	    {
			if (other instanceof GlobalXact)
				return super.equals(other);
			// Just cast it and catch the exception rather than doing the type
			// checking twice.
			Xid other_xid = (Xid) other;
		
			return(
				   java.util.Arrays.equals(
									other_xid.getGlobalTransactionId(),
									this.global_id)          &&
				   java.util.Arrays.equals(
									other_xid.getBranchQualifier(),
									this.branch_id)          &&
				   other_xid.getFormatId() == this.format_id);
		
	    }
		catch(ClassCastException cce)
	    {
			// this class only knows how to compare with other Xids
			if (SanityManager.DEBUG)
				SanityManager.THROWASSERT("comparing XAXactId with " + 
										  other.getClass().getName()); 
		
			return false;
	    }
    }


}



