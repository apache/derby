/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.info
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.services.info;

/**
  This class defines the names of the properties to use when
  you extract the parts of a product version from a properties
  file.
  */
public abstract class PropertyNames
{
	public final static String
	PRODUCT_VENDOR_NAME   = "derby.product.vendor",
	PRODUCT_TECHNOLOGY_NAME = "derby.product.technology.name",
    PRODUCT_EXTERNAL_NAME = "derby.product.external.name",
    PRODUCT_EXTERNAL_VERSION = "derby.product.external.version",
	PRODUCT_MAJOR_VERSION = "derby.version.major",
	PRODUCT_MINOR_VERSION = "derby.version.minor",
	PRODUCT_MAINT_VERSION = "derby.version.maint",
	PRODUCT_DRDA_MAINT_VERSION = "derby.version.drdamaint",
    PRODUCT_BETA_VERSION  = "derby.version.beta",
	PRODUCT_BUILD_NUMBER  = "derby.build.number",
    PRODUCT_WHICH_ZIP_FILE = "derby.product.file";
}

