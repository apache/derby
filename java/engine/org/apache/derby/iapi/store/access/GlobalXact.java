/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;


/**

This abstract class represents a global transaction id which can be tested
for equality against other transaction ids, which can be hashed into a
hash table, and which can be output as a string. 
<P>
This class has 2 direct subclasses. 
<UL>
<LI> org.apache.derby.iapi.store.access.xa.XAXactId : 
this class is a specific implementation of the JTA Xid interface
<LI> org.apache.derby.impl.store.access.GlobalXactId : 
this class represents internal cloudscape transaction ids
</UL>
<P>
The main reason for this class is to ensure that equality etc. works in a
consistent way across both subclasses. 
**/

public abstract class GlobalXact {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
    
    /**************************************************************************
     * Protected Fields of the class
     **************************************************************************
     */
    protected int     format_id;
    protected byte[]  global_id;
    protected byte[]  branch_id;

    public boolean equals(Object other) 
    {
		if (other == this)
			return true;

		if (other instanceof GlobalXact) {
	
			GlobalXact other_xact = (GlobalXact) other;
		
			return(
				   java.util.Arrays.equals(
									other_xact.global_id,
									this.global_id)          &&
				   java.util.Arrays.equals(
									other_xact.branch_id,
									this.branch_id)          &&
				   other_xact.format_id == this.format_id);
		
	    }

		return false;	
    }

    public String toString()
    {
		String globalhex = "";
		String branchhex = "";
		if (global_id != null) 
	    {
			int mask = 0;
			for (int i = 0; i < global_id.length; i++)
		    {
				mask = (global_id[i] & 0xFF);
				globalhex += Integer.toHexString(mask);
		    }
	    }
	
		if (branch_id != null)
	    {
			int mask = 0;
			for (int i = 0; i < branch_id.length; i++)
		    {
				mask = (branch_id[i] & 0xFF);
				branchhex += Integer.toHexString(mask);
		    }
	    }

		return("(" + format_id + "," + globalhex + "," + branchhex + ")");
	
    }


    /**
       Provide a hashCode which is compatable with the equals() method.
       
       @see java.lang.Object#hashCode
    **/
    public int hashCode()
    {
		// make sure hash does not overflow int, the only unknown is
		// format_id.  Lop off top bits.
		int hash = global_id.length + branch_id.length + (format_id & 0xFFFFFFF);

		for (int i = 0; i < global_id.length; i++) 
	    {
			hash += global_id[i];
	    }
		for (int i = 0; i < branch_id.length; i++) 
	    {
			hash += branch_id[i];
	    }
	
		return(hash);
    }
    
}



