/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.types.DataValueDescriptor;
/**

  Holds the location of a row within a given conglomerate.
  A row location is not valid except in the conglomerate
  from which it was obtained.  They are used to identify
  rows for fetches, deletes, and updates through a 
  conglomerate controller.
  <p>
  See the conglomerate implementation specification for
  information about the conditions under which a row location
  remains valid.

**/

public interface RowLocation extends DataValueDescriptor, CloneableObject
{
}
