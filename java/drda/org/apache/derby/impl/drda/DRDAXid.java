/*

   Derby - Class org.apache.derby.impl.drda.DRDAXid.java

   Copyright 2004 The Apache Software Foundation or its licensors, as applicable.

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

/**
 * This class provides an Xid implementation for Network Server XA
 */

package org.apache.derby.impl.drda;
import javax.transaction.xa.Xid;

class DRDAXid implements Xid
{

	private final int format_id;
	private final byte[] global_id;
	private final byte[] branch_id;


	DRDAXid(int formatid, byte[] globalid, byte[] branchid)
	{

		format_id = formatid;
		global_id = globalid;
		branch_id = branchid;
		
	}

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

	public String toString()
	{
		
	   String s =  "{DRDAXid: " +
		   "formatId("     + format_id   + "), " +
		   "globalTransactionId(" +  convertToHexString(global_id) + ")" +
		   "branchQualifier(" +  convertToHexString(branch_id) + ")";
	   return s;
	}


	/**
	 * convert byte array to a Hex string
	 * 
	 * @param buf buffer to  convert
	 * @return hex string representation of byte array
	 */
	private static String convertToHexString(byte [] buf)
	{
		if (buf == null)
			return null;
		StringBuffer str = new StringBuffer();
		str.append("0x");
		String val;
		int byteVal;
		for (int i = 0; i < buf.length; i++)
		{
			byteVal = buf[i] & 0xff;
			val = Integer.toHexString(byteVal);
			if (val.length() < 2)
				str.append("0");
			str.append(val);
		}
		return str.toString();
	}
}









