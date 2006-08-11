/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.WarehouseVTI

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.SQLException;

/**
 * Cooked up VTI to test SYSCS_BULK_INSERT  
 */
public class WarehouseVTI extends TableVTI {
    private int maxRows;
    private int row = 0;
    
public WarehouseVTI(String schemaName,String tableName,String maxRows)
    throws SQLException
{
    super(tableName);
    this.maxRows = Integer.parseInt(maxRows);
}
 
 public boolean next() {

     if (++row <= maxRows)
         return true;
     else
         return false;

 }

 public int getInt(int col)

 {
     switch (col) {
     case 1:
         return row;
     default:
         System.out.println("ERROR! INVALID COLUMN");
     }
     return 0;
 }

}  