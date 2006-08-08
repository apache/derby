/*

	Derby - Class org.apache.derby.ui.util.SelectionUtil
	
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

package org.apache.derby.ui.util;

import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;


public class SelectionUtil {
    public static IProject findSelectedProject(ISelection selection) {
    	IProject currentProject = null;
    	if (selection != null) {
    		if (selection instanceof IStructuredSelection) {
    			IStructuredSelection ss = (IStructuredSelection)selection;
    			Object obj = ss.getFirstElement();
    			if (obj instanceof IProject) {
    				currentProject = (IProject)obj;
    			}
    		}
    	}
    	return currentProject;
    }
	
     public static IJavaProject findSelectedJavaProject(ISelection selection) {
    	IJavaProject currentProject = null;
    	if (selection != null) {
    		if (selection instanceof IStructuredSelection) {
    			IStructuredSelection ss = (IStructuredSelection)selection;
    			Object obj = ss.getFirstElement();
    			if (obj instanceof IJavaProject) {
    				currentProject = (IJavaProject)obj;
    			}
    		}
    	}
    	return currentProject;
    }
    
    public static String getStatusMessages(Exception e) {
    	String msg = e.getMessage();
    	if (e instanceof CoreException) {
    		CoreException ce = (CoreException)e;	
			IStatus status = ce.getStatus();
			IStatus[] children = status.getChildren();
			for (int i = 0; i < children.length; i++)
				msg += "\n" + children[i].getMessage();
			System.err.println(msg);
			ce.printStackTrace(System.err);
    	}
    	return msg;
    }
}
