/*

	Derby - Class org.apache.derby.ui.decorator.DerbyIsRunningDecorator
	
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


package org.apache.derby.ui.decorate;

import org.apache.derby.ui.DerbyPlugin;
import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.util.DerbyServerUtils;
import org.apache.derby.ui.util.Logger;
import org.apache.derby.ui.util.SelectionUtil;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Platform;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jface.resource.ImageDescriptor;
import org.eclipse.jface.viewers.IDecoration;
import org.eclipse.jface.viewers.ILightweightLabelDecorator;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.LabelProviderChangedEvent;
import org.eclipse.swt.widgets.Display;
import org.eclipse.ui.IDecoratorManager;



public class DerbyIsRunningDecorator     extends LabelProvider
implements ILightweightLabelDecorator {


	private static final ImageDescriptor derbyRunningImageDesc = ImageDescriptor.
	createFromURL(Platform.getBundle(CommonNames.UI_PATH).getEntry("/icons/"+CommonNames.ISRUNNING+".gif"));
	
    public void decorate(Object element, IDecoration decoration) {
    	IProject proj=null;
    	if(element instanceof IJavaProject){
    		proj = ((IJavaProject)element).getProject();
    	}else{
    		proj=(IProject)element;
    	}
    	try {
			if (DerbyServerUtils.getDefault().getRunning(proj)) {
				decoration.addOverlay(derbyRunningImageDesc);
			}
    	}
    	catch (CoreException ce) {
    		Logger.log(SelectionUtil.getStatusMessages(ce),IStatus.ERROR);
    	}
    }


    	private void startUpdateDecor(IProject proj) {
    	final LabelProviderChangedEvent evnt = new LabelProviderChangedEvent(this, proj); 
		Display.getDefault().asyncExec(new Runnable() {
			public void run() {
				fireLabelProviderChanged(evnt);
			}
		});
    }

    	public static void performUpdateDecor(IProject proj) {
    		IDecoratorManager dm = DerbyPlugin.getDefault().getWorkbench().getDecoratorManager();
    		DerbyIsRunningDecorator decorator = (DerbyIsRunningDecorator)dm.getBaseLabelProvider(CommonNames.RUNDECORATOR);
    		decorator.startUpdateDecor(proj);
        }
        	
}
