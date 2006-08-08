/*

	Derby - Class org.apache.derby.ui.actions.IJAction
	
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

package org.apache.derby.ui.actions;


import org.apache.derby.ui.DerbyPlugin;
import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.common.Messages;
import org.apache.derby.ui.util.DerbyUtils;
import org.apache.derby.ui.util.Logger;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IActionDelegate;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IWorkbenchPart;


public class IJAction implements IObjectActionDelegate {

	private IFile currentScript;
	private IJavaProject currentJavaProject;
	private IProject currentProject;
	/**
	 * Constructor for IJAction.
	 */
	public IJAction() {
		super();
	}

	/**
	 * @see IObjectActionDelegate#setActivePart(IAction, IWorkbenchPart)
	 */
	public void setActivePart(IAction action, IWorkbenchPart targetPart) {
	}

	/**
	 * @see IActionDelegate#run(IAction)
	 */
	public void run(IAction action) {
		
		Shell shell = new Shell();
		DerbyPlugin plugin = DerbyPlugin.getDefault();
		if (plugin== null) {
			MessageDialog.openInformation(shell,
				CommonNames.PLUGIN_NAME,
				Messages.NO_ACTION);
		}
		else {
			try {
				if(currentJavaProject!=null){
					currentProject=currentJavaProject.getProject();
				}
				if(currentProject.isNatureEnabled(CommonNames.DERBY_NATURE)){
					DerbyUtils.runIJ(currentScript,currentProject);
				}else{
					shell = new Shell();
					MessageDialog.openInformation(
						shell,
						CommonNames.PLUGIN_NAME,
						Messages.NO_DERBY_NATURE+"\n"+
						Messages.ADD_N_TRY);
				}
			}catch(Exception e){
				Logger.log("IAction.run() error "+e,IStatus.ERROR);
			}
		}
	}

	/**
	 * @see IActionDelegate#selectionChanged(IAction, ISelection)
	 */
	public void selectionChanged(IAction action, ISelection selection) {
		currentJavaProject = org.apache.derby.ui.util.SelectionUtil.findSelectedJavaProject(selection);
		if(currentJavaProject==null){
			currentProject=org.apache.derby.ui.util.SelectionUtil.findSelectedProject(selection);
		}
		currentScript = null;
		if (selection != null) {
			if (selection instanceof IStructuredSelection) {
				IStructuredSelection ss = (IStructuredSelection)selection;
				// get the first element, since selection is for single object
				Object obj = ss.getFirstElement();
				if (obj instanceof IFile) {
					currentScript = (IFile)obj;
				}
				if(currentScript!=null){
					currentProject=currentScript.getProject();
				}
			}
		}
		// To turn off the action item if the DERBY nature is not set	
		// We decided to go with the pop-up dialog way with the message to 
		// add the Derby nature and try.
		
//		try{
//			if((currentScript!=null)&&(currentProject!=null)){
//				if(currentScript.getName().toLowerCase().endsWith(".sql")&&(!currentProject.isNatureEnabled(CommonNames.DERBY_NATURE))){
//					action.setEnabled(false);
//				}
//			}
//		}catch(CoreException ce){
//			Logger.log("IAction.selectionChanged() method error "+ce,IStatus.ERROR);
//		}
	}

}
