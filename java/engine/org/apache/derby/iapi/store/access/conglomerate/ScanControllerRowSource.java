/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.ScanControllerRowSource

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.store.access.conglomerate;

import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;

/**

  A ScanControllerRowSource is both a RowSource and a ScanController.  This
  interface is internal to Access for use in the case of RowSource which are
  implemented on top of a ScanController.

**/
public interface ScanControllerRowSource 
    extends ScanController, RowLocationRetRowSource
{
}
