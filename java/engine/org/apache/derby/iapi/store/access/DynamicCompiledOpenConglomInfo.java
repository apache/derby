/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;


/**

  Information that can be "compiled" and reused per transaction per 
  open operation.  This information is read only by the caller and
  written by user.  Likely information kept in this object is a set of
  scratch buffers which will be used by openScan() and thus must not be
  shared across multiple threads/openScan()'s/openConglomerate()'s.  The
  goal is to optimize repeated operations like btree inserts, by allowing a
  set of scratch buffers to be reused across a repeated execution of a statement
  like an insert/delete/update.

  This information is obtained from the getDynamicCompiledConglomInfo(conglomid)
  method call.  It can then be used in openConglomerate() and openScan() calls
  for increased performance.  The information is only valid until the next
  ddl operation is performed on the conglomerate.  It is up to the caller to
  provide an invalidation methodology.
  
  The dynamic info is a set of variables to be used in a given ScanController
  or ConglomerateController.  It can only be used in one controller at a time.
  It is up to the caller to insure the correct thread access to this info.  The
  type of info in this is a scratch template for btree traversal, other scratch
  variables for qualifier evaluation, ...

**/

public interface DynamicCompiledOpenConglomInfo
{
}
