/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.services.info
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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

