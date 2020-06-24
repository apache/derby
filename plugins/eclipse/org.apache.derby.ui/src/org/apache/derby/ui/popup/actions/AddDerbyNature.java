/*

	Derby - Class org.apache.derby.ui.popup.actions.AddDerbyNature

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
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.ui.IObjectActionDelegate;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.IWorkbenchWindow;
import org.eclipse.ui.PlatformUI;

public class AddDerbyNature implements IObjectActionDelegate
{

    private IJavaProject currentJavaProject;
    private IProject currentProject;

    /*
     * (non-Javadoc)
     * 
     * @see org.eclipse.ui.IObjectActionDelegate#setActivePart(org.eclipse.jface.action.IAction,
     *      org.eclipse.ui.IWorkbenchPart)
     */
    public void setActivePart(IAction action, IWorkbenchPart targetPart)
    {
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.eclipse.ui.IActionDelegate#run(org.eclipse.jface.action.IAction)
     */
    public void run(IAction action)
    {
        IWorkbenchWindow window = PlatformUI.getWorkbench()
                .getActiveWorkbenchWindow();
        Cursor waitCursor = new Cursor(window.getShell().getDisplay(),
                SWT.CURSOR_WAIT);
        try
        {
            window.getShell().setCursor(waitCursor);
            ((ApplicationWindow) window).setStatus(Messages.ADDING_NATURE);

            //new way
            if (currentJavaProject == null)
            {
                // if the java nature is not present
                // it must be added, along with the Derby nature
                IProjectDescription description = currentProject
                        .getDescription();
                String[] natureIds = description.getNatureIds();
                String[] newNatures = new String[natureIds.length + 2];
                System.arraycopy(natureIds, 0, newNatures, 0, natureIds.length);
                newNatures[newNatures.length - 2] = JavaCore.NATURE_ID;
                newNatures[newNatures.length - 1] = CommonNames.DERBY_NATURE;
                description.setNatureIds(newNatures);
                currentProject.setDescription(description, null);

                currentJavaProject = (IJavaProject) JavaCore
                        .create((IProject) currentProject);
            }
            else
            {
                //add the derby nature, the java nature is already present
                IProjectDescription description = currentJavaProject
                        .getProject().getDescription();
                String[] natures = description.getNatureIds();
                String[] newNatures = new String[natures.length + 1];
                System.arraycopy(natures, 0, newNatures, 0, natures.length);
                // must prefix with plugin id
                newNatures[natures.length] = CommonNames.DERBY_NATURE;
                description.setNatureIds(newNatures);
                currentJavaProject.getProject().setDescription(description,
                        null);
            }

            IClasspathEntry[] rawClasspath = currentJavaProject
                    .getRawClasspath();

//IC see: https://issues.apache.org/jira/browse/DERBY-1931
            List<IClasspathEntry> newEntries = new ArrayList<IClasspathEntry>(rawClasspath.length+1);            
            for(IClasspathEntry e: rawClasspath) {
            	newEntries.add(e);
            }            
            newEntries.add(JavaCore.newContainerEntry(DerbyClasspathContainer.CONTAINER_ID));
            
            IClasspathEntry[] newEntriesArray = new IClasspathEntry[newEntries.size()];
            newEntriesArray = (IClasspathEntry[])newEntries.toArray(newEntriesArray);
            currentJavaProject.setRawClasspath(newEntriesArray, null);
                    
            // refresh project so user sees new files, libraries, etc
            currentJavaProject.getProject().refreshLocal(
                    IResource.DEPTH_INFINITE, null);
            ((ApplicationWindow) window).setStatus(Messages.DERBY_NATURE_ADDED);

        } catch ( Exception e)
        {
            Logger.log(Messages.ERROR_ADDING_NATURE + " '"
                    + currentJavaProject.getProject().getName() + "' : " + e,
                    IStatus.ERROR);
            Shell shell = new Shell();
            MessageDialog.openInformation(shell, CommonNames.PLUGIN_NAME,
                    Messages.ERROR_ADDING_NATURE + ":\n"
                            + SelectionUtil.getStatusMessages(e));
        } finally
        {
            window.getShell().setCursor(null);
            waitCursor.dispose();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.eclipse.ui.IActionDelegate#selectionChanged(org.eclipse.jface.action.IAction,
     *      org.eclipse.jface.viewers.ISelection)
     */
    public void selectionChanged(IAction action, ISelection selection)
    {
        currentJavaProject = SelectionUtil.findSelectedJavaProject(selection);

        if (currentJavaProject == null)
        {
            currentProject = org.apache.derby.ui.util.SelectionUtil
                    .findSelectedProject(selection);
        }

    }

}
