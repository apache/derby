/*

   Derby - Class org.apache.derby.client.am.GetFileInputStreamAction

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

/**
 * Java 2 PrivilegedExceptionAction encapsulation of creating a new FileInputStream throws FileNotFoundException
 */
public class GetFileInputStreamAction implements java.security.PrivilegedExceptionAction {
    // the pathname used by the input file in the file system
    private String filePathName_ = null;

    private String canonicalPath_ = null;

    //-------------------- Constructors --------------------

    public GetFileInputStreamAction(String filePathName) {
        filePathName_ = filePathName;
    }

    //-------------------- methods --------------------

    public Object run() throws java.io.IOException {
        java.io.File file = new java.io.File(filePathName_);
        java.io.FileInputStream fileInputStream = new java.io.FileInputStream(file);
        canonicalPath_ = file.getCanonicalPath();
        return fileInputStream;
    }

    public String getCanonicalPath() {
        return canonicalPath_;
    }
}
