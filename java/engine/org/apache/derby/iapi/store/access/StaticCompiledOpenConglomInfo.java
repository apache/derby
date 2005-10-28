/*

   Derby - Class org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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
