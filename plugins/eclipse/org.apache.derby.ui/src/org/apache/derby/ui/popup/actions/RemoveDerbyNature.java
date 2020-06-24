/*

	Derby - Class org.apache.derby.ui.popup.actions.RemoveDerbyNature
	
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
package org.apache.derby.ui.popup.actions;

import java.util.ArrayList;
import java.util.List;

import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.common.Messages;
import org.apache.derby.ui.container.DerbyClasspathContainer;
import org.apache.derby.ui.util.DerbyServerUtils;
import org.apache.derby.ui.util.Logger;
import org.apache.derby.ui.util.SelectionUtil;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IProjectDescription;
import org.eclipse.core.resources.IResource;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jdt.core.IClasspathEntry;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.window.ApplicationWindow;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.PlatformUI;

public class RemoveDerbyNature implements IObjectActionDelegate {

	private IJavaProject currentJavaProject;
	private IProject currentProject;
	/* (non-Javadoc)
	 * @see org.eclipse.ui.IObjectActionDelegate#setActivePart(org.eclipse.jface.action.IAction, org.eclipse.ui.IWorkbenchPart)
	 */
	public void setActivePart(IAction action, IWorkbenchPart targetPart) {
		}


	private static String[] removeDerbyNature(String [] natures){
		ArrayList arrL=new ArrayList();
		
		for (int i=0;i<natures.length;i++){
			if(!(natures[i].equalsIgnoreCase(CommonNames.DERBY_NATURE))){
				arrL.add(natures[i]);
			}
		}
		String [] newNatures= new String [arrL.size()];
		for(int i=0;i<arrL.size();i++){
			newNatures[i]=(String)arrL.get(i);
		}
		return newNatures;
	}
	/* (non-Javadoc)
	 * @see org.eclipse.ui.IActionDelegate#run(org.eclipse.jface.action.IAction)
	 */	
	public void run(IAction action) {
		IWorkbenchWindow window = PlatformUI.getWorkbench().getActiveWorkbenchWindow();
		try {
			((ApplicationWindow)window).setStatus(Messages.REMOVING_NATURE);
			
			if(currentJavaProject==null){
				currentJavaProject=JavaCore.create(currentProject);
			}
			//Shutdown server if running for the current project
			if(DerbyServerUtils.getDefault().getRunning(currentJavaProject.getProject())){
				DerbyServerUtils.getDefault().stopDerbyServer(currentJavaProject.getProject());
			}
			IClasspathEntry[] rawClasspath = currentJavaProject.getRawClasspath();
			
//IC see: https://issues.apache.org/jira/browse/DERBY-1931
			List<IClasspathEntry> newEntries = new ArrayList<IClasspathEntry>();
			for(IClasspathEntry e: rawClasspath) {
				if(e.getEntryKind()!=IClasspathEntry.CPE_CONTAINER) {
					newEntries.add(e);
				} else if(!e.getPath().equals(DerbyClasspathContainer.CONTAINER_ID)) {
					newEntries.add(e);
				}
			}
			
			IClasspathEntry[] newEntriesArray = new IClasspathEntry[newEntries.size()];
			newEntriesArray = (IClasspathEntry[])newEntries.toArray(newEntriesArray);			
			currentJavaProject.setRawClasspath(newEntriesArray, null);
			
			IProjectDescription description = currentJavaProject.getProject().getDescription();
			String[] natures = description.getNatureIds();

			description.setNatureIds(removeDerbyNature(natures));
			currentJavaProject.getProject().setDescription(description, null);
			// refresh project so user sees changes
			currentJavaProject.getProject().refreshLocal(IResource.DEPTH_INFINITE, null);
			((ApplicationWindow)window).setStatus(Messages.DERBY_NATURE_REMOVED);
		}catch (Exception e) {
			Logger.log(Messages.ERROR_REMOVING_NATURE+" '"+currentJavaProject.getProject().getName()+"': "+e,IStatus.ERROR);

			Shell shell = new Shell();
			MessageDialog.openInformation(
				shell,
				CommonNames.PLUGIN_NAME,
				Messages.ERROR_REMOVING_NATURE+":\n" +
				 SelectionUtil.getStatusMessages(e));
		}

	}

	/* (non-Javadoc)
	 * @see org.eclipse.ui.IActionDelegate#selectionChanged(org.eclipse.jface.action.IAction, org.eclipse.jface.viewers.ISelection)
	 */
	public void selectionChanged(IAction action, ISelection selection) {
		currentJavaProject = SelectionUtil.findSelectedJavaProject(selection);
		if(currentJavaProject==null){
			currentProject=org.apache.derby.ui.util.SelectionUtil.findSelectedProject(selection);
		}
	}

}
