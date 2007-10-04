/*

  Derby - Class org.apache.derby.ui.container.DerbyClasspathContainer

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

*/

package org.apache.derby.ui.container;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.common.Messages;
import org.apache.derby.ui.util.Logger;
import org.eclipse.core.runtime.FileLocator;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.eclipse.jdt.core.IClasspathContainer;
import org.eclipse.jdt.core.IClasspathEntry;
import org.eclipse.jdt.core.JavaCore;
import org.osgi.framework.Bundle;

public class DerbyClasspathContainer implements IClasspathContainer {
    public static final Path CONTAINER_ID = new Path("DERBY_CONTAINER"); //$NON-NLS-1$
    private IClasspathEntry[] _entries;
   
    public DerbyClasspathContainer() {
        List<IClasspathEntry> entries = new ArrayList<IClasspathEntry>();
        Bundle bundle = Platform.getBundle(CommonNames.CORE_PATH);
        Enumeration en = bundle.findEntries("/", "*.jar", true);
        String rootPath = null;
        try { 
            rootPath = FileLocator.resolve(FileLocator.find(bundle, new Path("/"), null)).getPath();
        } catch(IOException e) {
            Logger.log(e.getMessage(), IStatus.ERROR);
        }
        while(en.hasMoreElements()) {
            IClasspathEntry cpe = JavaCore.newLibraryEntry(new Path(rootPath+'/'+((URL)en.nextElement()).getFile()), null, null);
            entries.add(cpe);
        }    
        IClasspathEntry[] cpes = new IClasspathEntry[entries.size()];
        _entries = (IClasspathEntry[])entries.toArray(cpes);
    }

    public IClasspathEntry[] getClasspathEntries() {      
        return _entries;       
    }

    public String getDescription() {
        return Messages.DERBY_CONTAINER_DESC;
    }

    public int getKind() {
        return K_APPLICATION;
    }

    public IPath getPath() {
        return CONTAINER_ID;
    }
}
