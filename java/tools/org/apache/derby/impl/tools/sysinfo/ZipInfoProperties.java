/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.sysinfo
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

//ZipInfoProperties

package org.apache.derby.impl.tools.sysinfo;

import java.util.Properties;
import java.io.OutputStream;
import org.apache.derby.iapi.services.info.PropertyNames;
import org.apache.derby.iapi.services.info.ProductVersionHolder;

public class ZipInfoProperties // extends Properties
	/**
		IBM Copyright &copy notice.
	*/

{ private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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


