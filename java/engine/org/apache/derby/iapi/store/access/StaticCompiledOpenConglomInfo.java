/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;


/**

  Information that can be "compiled" once and then used over and over again
  at execution time.  This information is read only by both the caller and
  the user, thus can be shared by multiple threads/transactions once created.

  This information is obtained from the getStaticCompiledConglomInfo(conglomid)
  method call.  It can then be used in openConglomerate() and openScan() calls
  for increased performance.  The information is only valid until the next
  ddl operation is performed on the conglomerate.  It is up to the caller to
  provide an invalidation methodology.

  The static info would be valid until any ddl was executed on the conglomid,
  and would be up to the caller to throw away when that happened.  This ties in
  with what language already does for other invalidation of static info.  The
  type of info in this would be containerid and array of format id's from which
  templates can be created.  The info in this object is read only and can
  be shared among as many threads as necessary.

**/

public interface StaticCompiledOpenConglomInfo extends Storable
{
    /**
     * routine for internal use of store only.
     **/
    DataValueDescriptor  getConglom();
}
