/*

   Derby - Class org.apache.derby.impl.tools.sysinfo.ZipInfoProperties

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

//ZipInfoProperties

package org.apache.derby.impl.tools.sysinfo;

import java.util.Properties;
import java.io.OutputStream;
import org.apache.derby.iapi.services.info.PropertyNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;

public class ZipInfoProperties // extends Properties
{
	private final ProductVersionHolder	version;
    /**
        full path to zip (or expanded zip)
        C:/cloudscape/lib/tools.zip
            -or-
        D:\myWorkDir\cloudscape\lib\ *expanded*

        The base name (at the end) should be the same as the zipNameString
     */
    private  String location;

	ZipInfoProperties(ProductVersionHolder version) {
		this.version = version;
	}

	/**
		Method to get only the "interesting" pieces of information
        for the customer, namely the version number (2.0.1) and
		the beta status and the build number
		@return a value for displaying to the user via Sysinfo
    */
    public String getVersionBuildInfo()
    {
        if (version == null)
		{
			return Main.getTextMessage ("SIF04.C");
		}

		if ("DRDA:jcc".equals(version.getProductTechnologyName()))
			return version.getSimpleVersionString() + " - (" + version.getBuildNumber() + ")";

		return version.getVersionBuildString(true);

    }

    public String getLocation()
    {
		if (location == null)
			return Main.getTextMessage ("SIF01.H");
        return location;
    }

	void setLocation(String location) {
		this.location = location;
	}



}


