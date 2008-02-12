/*

   Derby - Class org.apache.derby.mbeans.Version

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

import org.apache.derby.iapi.services.info.ProductVersionHolder;

/**
 * This implementation of VersionMBean instruments a
 * ProductVersionHolder object. The MBean interface is implemented with 
 * callbacks to the wrapped object which gives detailed version information.
 *
 * @see org.apache.derby.iapi.services.info.ProductVersionHolder
 */
public class Version implements VersionMBean {
    
    private final ProductVersionHolder engineVersionRef;
    
    public Version(ProductVersionHolder pvh) {
        engineVersionRef = pvh;
    }
    
    // ------------------------- MBEAN ATTRIBUTES  ----------------------------
    
    public String getProductName(){
        return engineVersionRef.getProductName();
    }
    
     public String getProductTechnologyName(){
        return engineVersionRef.getProductTechnologyName();
    }
    
    public String getProductVendorName(){
        return engineVersionRef.getProductVendorName();
    }
    
    public int getMajorVersion(){
        return engineVersionRef.getMajorVersion();
    }
    
    public int getMinorVersion(){
        return engineVersionRef.getMinorVersion();
    }
    
    public int getMaintVersion(){
        return engineVersionRef.getMaintVersion();
    }
    
    public String getBuildNumber(){
        return engineVersionRef.getBuildNumber();
    }
    
    public int getBuildNumberAsInt(){
        return engineVersionRef.getBuildNumberAsInt();
    }
    
    public boolean getIsBeta(){
        return engineVersionRef.isBeta();
    }
    
    public boolean isAlpha(){
        return engineVersionRef.isAlpha();
    }
  
}
