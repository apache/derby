/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.io
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.io;

import java.io.Externalizable;

/**
  Cloudscape interface for creating a stored form for
  an object and re-constructing an equivalent object
  from this stored form. The object which creates the
  stored form and the re-constructed object need not be
  the same or related classes. They must share the same
  TypedFormat.

 */
public interface Formatable
extends Externalizable, TypedFormat
{ 
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
}
