/*

   Derby - Class org.apache.derby.client.am.GetResourceBundleAction

   Copyright (c) 2002, 2005 The Apache Software Foundation or its licensors, where applicable.

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

// Java 2 PrivilegedExceptionAction encapsulation of java.util.ResourceBundle.getBundle() action

public class GetResourceBundleAction implements java.security.PrivilegedExceptionAction {
    private String resourceBundleName_ = null; // class name for resources

    // the base name of the resource bundle, a fully qualified class name
    public GetResourceBundleAction(String resourceBundleName) {
        resourceBundleName_ = resourceBundleName;
    }

    public Object run() throws NullPointerException, java.util.MissingResourceException {
        return java.util.ResourceBundle.getBundle(resourceBundleName_);
    }

    public void setResourceBundleName(String resourceBundleName) {
        resourceBundleName_ = resourceBundleName;
    }
}
