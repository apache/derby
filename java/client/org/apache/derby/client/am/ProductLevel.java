/*

   Derby - Class org.apache.derby.client.am.ProductLevel

   Copyright (c) 2001, 2005 The Apache Software Foundation or its licensors, where applicable.

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
package org.apache.derby.client.am;

public class ProductLevel {
    public String databaseProductName_;
    public int versionLevel_;
    public int releaseLevel_;
    public int modificationLevel_;

    // The following value is sent in the srvrlslv level
    public String databaseProductVersion_;

    // The productID is set by the constructors.
    // dabaseProductVersion added by derby to include  srvrlslv
    public ProductLevel(String productID, String databaseProductName,
                        String srvrlslv) {
        // this.productID has the following format
        //   CSS for Derby
        // vv = version id
        // rr = release id
        // m = modification level
        versionLevel_ = Integer.parseInt(productID.substring(3, 5));
        releaseLevel_ = Integer.parseInt(productID.substring(5, 7));
        modificationLevel_ = Integer.parseInt(productID.substring(7, 8));
        databaseProductName_ = (databaseProductName == null) ?
                "Derby" : databaseProductName; // This is the srvclsnm in PROTOCOL.

        // databaseProductVersion - extracted from the srvrlslv.
        // srvrlslv has the format <PRDID>/<ALTERNATE VERSION FORMAT>
        // for example Derby has a four part verison number so might send
        // CSS10000/10.0.1.1 beta. If the alternate version format is not
        // specified,
        // databaseProductVersion_ will just be set to the srvrlslvl.
        // final fallback will be the product id.
        // this is the value returned with the getDatabaseProductVersion()
        // metadata call
        int dbVersionOffset = 0;
        if (srvrlslv != null) {
            dbVersionOffset = srvrlslv.indexOf('/') + 1;
            // if there was no '/' dbVersionOffset will just be 0
            databaseProductVersion_ = srvrlslv.substring(dbVersionOffset);
        }
        if (databaseProductVersion_ == null) {
            databaseProductVersion_ = productID;
        }
    }

    public boolean greaterThanOrEqualTo(int versionLevel, int releaseLevel, int modificationLevel) {
        if (versionLevel_ > versionLevel) {
            return true;
        } else if (versionLevel_ == versionLevel) {
            if (releaseLevel_ > releaseLevel) {
                return true;
            } else if (releaseLevel_ == releaseLevel) {
                if (modificationLevel_ >= modificationLevel) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean lessThan(int versionLevel, int releaseLevel, int modificationLevel) {
        if (versionLevel_ < versionLevel) {
            return true;
        } else if (versionLevel_ == versionLevel) {
            if (releaseLevel_ < releaseLevel) {
                return true;
            } else if (releaseLevel_ == releaseLevel) {
                if (modificationLevel_ < modificationLevel) {
                    return true;
                }
            }
        }
        return false;
    }
}
