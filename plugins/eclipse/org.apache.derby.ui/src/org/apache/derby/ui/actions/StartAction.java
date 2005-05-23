/*

	Derby - Class org.apache.derby.ui.actions.StartAction
	
	Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.
	
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

package org.apache.derby.ui.actions;

import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.common.Messages;
import org.apache.derby.ui.util.DerbyServerUtils;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IWorkbenchPart;

;

public class StartAction implements IObjectActionDelegate {

	private IJavaProject currentJavaProject;
	private IProject currentProject;
	
	public StartAction() {
		super();
	}

	public void setActivePart(IAction action, IWorkbenchPart targetPart) {
	}

	public void run(IAction action) {
		try {
			if(currentJavaProject!=null){
				currentProject=currentJavaProject.getProject();
			}
			DerbyServerUtils.getDefault().startDerbyServer(currentProject);
			
		}
		catch (CoreException e) {
			Shell shell = new Shell();
			MessageDialog.openInformation(
				shell,
				CommonNames.PLUGIN_NAME,
				Messages.D_NS_START_ERROR +
				org.apache.derby.ui.util.SelectionUtil.getStatusMessages(e));
		}
	}

	public void selectionChanged(IAction action, ISelection selection) {
		currentJavaProject = org.apache.derby.ui.util.SelectionUtil.findSelectedJavaProject(selection);
		
		if(currentJavaProject==null){
			currentProject=org.apache.derby.ui.util.SelectionUtil.findSelectedProject(selection);
		}
		
	}

}
