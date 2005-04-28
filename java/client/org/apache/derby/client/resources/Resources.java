/*

   Derby - Class org.apache.derby.client.resources.Resources

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
 * This is the default (English) resource bundle.
 * You can provide your own locale-specific bundle
 * with an ISO language suffix appended to the class name for
 * your language.  Additional or replacement locale-specific
 * resource bundles must reside in the <code>org.apache.derby.client.resources</code>
 * package.
 * <p>
 * The driver locates the appropriate resource bundle for your locale by using
 * <pre>
 * java.util.ResourceBundle.getBundle ("org.apache.derby.client.resources.Resources");
 * </pre>
 * Using the ClassLoader, this call in turn looks for the appropriate bundle
 * as a .class file in the following order based on your locale:
 * <ol>
 * <li>Resources_language_country_variant
 * <li>Resources_language_country
 * <li>Resources_language
 * <li>Resources (this is the default bundle shipped with the Derby Client)
 * </ol>
 * <p>
 * <b>Note for bundle designers:</b>
 * All resource strings are processed by
 * {@link java.text.MessageFormat#format(String, Object[]) java.text.MessageFormat.format(String, Object[])},
 * so all occurrences of the {, } and ' characters in resource strings must
 * be delimited with quotes as '{', or '}' for the { and } characters,
 * and '' for the ' character.
 * For details see the Sun javadocs for class {@link java.text.MessageFormat java.text.MessageFormat}.
 *
 * @see java.util.Locale
 * @see java.util.ResourceBundle
 **/
public class Resources extends java.util.ListResourceBundle
{

  final static private Object[][] resources__ =
  {
    // *******************************
    // *** Miscellaneous text keys ***
    // *******************************
    {ResourceKeys.driverOriginationIndicator,
     "[derby] "},

    {ResourceKeys.engineOriginationIndicator,
     "[derby] "},

    {ResourceKeys.companyName,
     "Apache Software Foundation"},

    // ********************************
    // *** Driver.getPropertyInfo() descriptions ***
    // ********************************

    {ResourceKeys.propertyDescription__user,
     "The user name for the connection"},

    {ResourceKeys.propertyDescription__password,
     "The user''s password for the connection"},

    {ResourceKeys.propertyDescription__characterEncoding,
     "The character encoding for the connection"},

    {ResourceKeys.propertyDescription__planName,
     "The plan name for the connection"},

    // ********************************
    // *** Missing Resource Exception ***
    // ********************************
    {ResourceKeys.missingResource__01,
     "No resource for key {0} could be found in resource bundle {1}."},
  };

  /**
   * Extracts an array of key, resource pairs for this bundle.
   **/
  public Object[][] getContents()
  {
    return resources__;
  }
}
