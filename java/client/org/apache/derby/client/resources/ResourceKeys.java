/*

   Derby - Class org.apache.derby.client.resources.ResourceKeys

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

package org.apache.derby.client.resources;


/**
 * <p>
 * These keys are provided only as a convenience for managing
 * locale-specific resource strings.
 * These keys will change with each release,
 * some may be added and some may be deleted,
 * so do not use these keys for any purpose other than a version-
 * dependent resource bundle.
 * <p>
 * The resource for any key can be obtained as follows:
 * <pre>
 * java.util.ResourceBundle resources =
 *   java.util.ResourceBundle.getBundle ("org.apache.derby.client.resources.Resources");
 * resources.getString (<i>key</i>);
 * </pre>
 *
 * @see Resources
 **/
public class ResourceKeys
{
  // Define a private constructor to prevent default public constructor
  private ResourceKeys () {}


  //-----------------Miscellaneous text keys------------------------------------
  final static public String driverOriginationIndicator = "1";
  final static public String engineOriginationIndicator = "2";
  final static public String companyName = "3";

  //-----------------Driver.getPropertyInfo() descriptions----------------------
  final static public String propertyDescription__user = "7";
  final static public String propertyDescription__password = "8";
  final static public String propertyDescription__characterEncoding = "9";
  final static public String propertyDescription__planName = "10";

  //-----------------java.util.MissingResourceException-------------------------
  final static public String missingResource__01 = "4";
}
