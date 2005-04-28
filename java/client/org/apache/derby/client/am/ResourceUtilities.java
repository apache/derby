/*

   Derby - Class org.apache.derby.client.am.ResourceUtilities

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

import org.apache.derby.client.resources.ResourceKeys;

public final class ResourceUtilities
{
  private final static Object[] emptyArgs__ = new Object[] {};

  // Get resourced text and substitute arguments into text delimited with {i}
  // using Java's builtin message formatter.
  static public String getResource (String key, Object[] args)
  {
    try {
      return java.text.MessageFormat.format (Configuration.dncResources__.getString (key), args);
    }
    catch (java.util.MissingResourceException e) {
      try {
        return java.text.MessageFormat.format (
          Configuration.dncResources__.getString (ResourceKeys.missingResource__01),
          new Object[] {e.getKey(), e.getClassName ()});
      }
      catch (java.util.MissingResourceException e2) {
        return java.text.MessageFormat.format (
          "No resource for key {0} could be found in resource bundle {1}.",
          new Object[] {e.getKey(), e.getClassName ()});
      }
    }
  }

  static public String getResource (String key)
  {
    return getResource (key, emptyArgs__);
  }

  // This method is necessary for java.text.MessageFormat.format to work
  // properly because arguments may not be null.
  static String getMessage (java.lang.Exception e)
  {
    return (e.getMessage() == null) ? "" : e.getMessage();
  }

}

