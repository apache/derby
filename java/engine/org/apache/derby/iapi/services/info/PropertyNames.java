/*

   Derby - Class org.apache.derby.iapi.services.info.PropertyNames

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

