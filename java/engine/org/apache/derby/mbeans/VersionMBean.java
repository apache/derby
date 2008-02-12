/*

   Derby - Class org.apache.derby.mbeans.VersionMBean

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

package org.apache.derby.mbeans;

/** 
* This interface defines a Standard MBean for exposing the version information
* of the running Derby engine.
* 
* Refer to the getters of the interface for defined attributes. All attributes
* are read-only.
*
* The MBean does not define any operations.
*/
public interface VersionMBean {
    // attributes
    
    public String getProductName();
    public String getProductTechnologyName();
    public String getProductVendorName();
    
    public int getMajorVersion();
    public int getMinorVersion();
    public int getMaintVersion();
    
    public String getBuildNumber();
    public int getBuildNumberAsInt();
    
    public boolean getIsBeta();
    public boolean isAlpha();
    
}
