/*

   Derby - Class org.apache.derby.iapi.services.info.ProductGenusNames

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
  Holder class for cloudscape genus names.

  <P>
  A product genus defines a product's category (tools, DBMS etc). For
  some categories, Cloudscape ships more than one product. Each product,
  within the genus has a unique product species.

  <P>
  A correct run time environment should include at most one Cloudscape
  product of a given genus. This helps avoid situations in which the
  environment loads classes from more than one product. a user runs
  with a mix of classes from different

  <P>
  Please not that the list provided here serves to document product
  genus names and to facile consistent naming in code. Because the
  list of supported Cloudscape genus names will change with time, the
  code in this package does *NOT* restrict users to the product genus
  names listed here.
  */
public interface ProductGenusNames
{

	/**Genus name for dbms products.*/
	public static String DBMS = "DBMS";
	public static String DBMS_INFO = "/org/apache/derby/info/DBMS.properties";

	/**Genus name for tools products.*/
	public static String TOOLS = "tools";
	public static String TOOLS_INFO = "/org/apache/derby/info/tools.properties";

	/**Genus name for net products.*/
	public static String NET = "net";
	public static String NET_INFO = "/org/apache/derby/info/net.properties";
        }

