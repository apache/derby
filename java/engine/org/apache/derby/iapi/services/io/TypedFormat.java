/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;
/**
  Cloudscape interface for identifying the format id for the
  stored form of an object. Objects of different classes may
  have the same format id if:

  <UL>
  <LI> The objects read and write the same stored forms.
  <LI> The object's getTypeId() method returns the same
  identifier.
  <LI> The objects support all the interfaces the type
  implies.
  </UL>
  */
public interface TypedFormat
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	  Get a universally unique identifier for the type of
	  this object. 

	  @return The identifier. (A UUID stuffed in an array
	  of 16 bytes).
	 */	
	int getTypeFormatId();
}
