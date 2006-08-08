/*

	Derby - Class org.apache.derby.ui.util.DerbyServerUtils
	
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

//import org.apache.ui.decorator.DerbyRunningDecorator;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.common.Messages;
import org.apache.derby.ui.decorate.DerbyIsRunningDecorator;
import org.apache.derby.ui.properties.DerbyProperties;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.IResourceChangeEvent;
import org.eclipse.core.resources.IResourceChangeListener;
import org.eclipse.core.resources.IWorkspace;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.QualifiedName;
import org.eclipse.debug.core.DebugEvent;
import org.eclipse.debug.core.DebugPlugin;
import org.eclipse.debug.core.IDebugEventSetListener;
import org.eclipse.debug.core.ILaunch;
import org.eclipse.debug.core.model.IProcess;
import org.eclipse.jdt.core.IJavaProject;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.widgets.Shell;



public class DerbyServerUtils {
	
	//Singleton Class
	private static DerbyServerUtils dsUtils = new DerbyServerUtils();
	private HashMap servers = new HashMap();
 
    private DerbyServerUtils() {
        super();
    }

	public static DerbyServerUtils getDefault() {
		if (dsUtils == null)
			dsUtils = new DerbyServerUtils();
		return dsUtils;
	}
	
	// listener for DebugEvents, to know if a server was stopped by the client
	// or died by itself
	 
	private IDebugEventSetListener listener = new IDebugEventSetListener() {
	    public void handleDebugEvents(DebugEvent[] events) {
	    	// type of event was a terminate...
	    	if(events.length>0){
				if (events[0].getKind() == DebugEvent.TERMINATE) {
					Object source = events[0].getSource();
					if (source instanceof IProcess) {
						// check for Derby Network Servre process.
						Object proj = servers.get(source);
						if (proj != null) {
							try {
								//remove it from the hashmap, update the ui
								servers.remove(source);
								if(proj instanceof IJavaProject){
									setRunning(((IJavaProject)proj).getProject(), null);
								}else if(proj instanceof IProject){
									setRunning((IProject)proj,null);
								}
							}
							catch (CoreException ce) {
								Logger.log("DerbyServerTracker.handleDebugEvents: "+ce, IStatus.ERROR);
							}catch(Exception e){
								Logger.log("DerbyServerTracker.handleDebugEvents: "+e, IStatus.ERROR);
							}
						}
					}
				}
	    	}
	    }
	};

	private IResourceChangeListener rlistener = new IResourceChangeListener() {
	      public void resourceChanged(IResourceChangeEvent event){
	         if(event.getType()==IResourceChangeEvent.PRE_CLOSE){
	         	try{
	         		if(event.getResource().getProject().isNatureEnabled(CommonNames.DERBY_NATURE)){
	         			if(getRunning(event.getResource().getProject())){
	         				stopDerbyServer(event.getResource().getProject());
	         			}
	         		}
	         	}catch(SWTException swe){
	         		//The SWTException is thrown during the Shell creation
	         		//Logger.log("Exception shutting down "+swe,IStatus.ERROR);
	         		//e.printStackTrace();
	         	}catch(Exception e){
	         		Logger.log("Exception shutting down "+e,IStatus.ERROR);
	         	}
	         }
	      }
	   };

	public boolean getRunning(IProject proj) throws CoreException {
		Object value = proj.getSessionProperty(new QualifiedName(CommonNames.UI_PATH, CommonNames.ISRUNNING));
		
		return value != null;
	}
	
	public void setRunning(IProject proj, Boolean value) throws CoreException {
		try{
			if (value != null && value.equals(Boolean.FALSE)){
				value = null;
			}
			if(proj.isOpen()){
				proj.setSessionProperty(new QualifiedName(CommonNames.UI_PATH,CommonNames.ISRUNNING ),value);
			}
		}catch(Exception e){
			Logger.log("DerbyServerUtils.setRunning() error: "+e, IStatus.ERROR);	
			
		}
		DerbyIsRunningDecorator.performUpdateDecor(proj);
	}

	public void startDerbyServer( IProject proj) throws CoreException {
		String args = CommonNames.START_DERBY_SERVER;
		String vmargs="";
		DerbyProperties dprop=new DerbyProperties(proj);
		//Starts the server as a Java app
		args+=" -h "+dprop.getHost()+ " -p "+dprop.getPort();
		
		//Set Derby System Home from the Derby Properties
		if((dprop.getSystemHome()!=null)&& !(dprop.getSystemHome().equals(""))){
			vmargs=CommonNames.D_SYSTEM_HOME+dprop.getSystemHome();
		}
		String procName="["+proj.getName()+"] - "+CommonNames.DERBY_SERVER+" "+CommonNames.START_DERBY_SERVER+" ("+dprop.getHost()+ ", "+dprop.getPort()+")";
		ILaunch launch = DerbyUtils.launch(proj, procName ,		
		CommonNames.DERBY_SERVER_CLASS, args, vmargs, CommonNames.START_DERBY_SERVER);
		IProcess ip=launch.getProcesses()[0];
		//set a name to be seen in the Console list
		ip.setAttribute(IProcess.ATTR_PROCESS_LABEL,procName);
		
		// saves the mapping between (server) process and project
		//servers.put(launch.getProcesses()[0], proj);
		servers.put(ip, proj);
		// register a listener to listen, when this process is finished
		DebugPlugin.getDefault().addDebugEventListener(listener);
		//Add resource listener
		IWorkspace workspace = ResourcesPlugin.getWorkspace();
		
		workspace.addResourceChangeListener(rlistener);
		setRunning(proj, Boolean.TRUE);
		Shell shell = new Shell();
		MessageDialog.openInformation(
			shell,
			CommonNames.PLUGIN_NAME,
			Messages.D_NS_ATTEMPT_STARTED+dprop.getPort()+".");

	}

	public void stopDerbyServer( IProject proj) throws CoreException, ClassNotFoundException, SQLException {
		String args = CommonNames.SHUTDOWN_DERBY_SERVER;
		String vmargs="";
		DerbyProperties dprop=new DerbyProperties(proj);
		args+=" -h "+dprop.getHost()+ " -p "+dprop.getPort();
		
		//	Set Derby System Home from the Derby Properties
		if((dprop.getSystemHome()!=null)&& !(dprop.getSystemHome().equals(""))){
			vmargs=CommonNames.D_SYSTEM_HOME+dprop.getSystemHome();
		}
		String procName="["+proj.getName()+"] - "+CommonNames.DERBY_SERVER+" "+CommonNames.SHUTDOWN_DERBY_SERVER+" ("+dprop.getHost()+ ", "+dprop.getPort()+")";
		
		// starts the server as a Java app
		ILaunch launch = DerbyUtils.launch(proj, procName,
		CommonNames.DERBY_SERVER_CLASS, args, vmargs,CommonNames.SHUTDOWN_DERBY_SERVER);
		IProcess ip=launch.getProcesses()[0];
		
		//set a name to be seen in the Console list
		ip.setAttribute(IProcess.ATTR_PROCESS_LABEL,procName);
		
		//update the objectState
		setRunning(proj, Boolean.FALSE);
		if(proj.isOpen()){
			Shell shell = new Shell();
			MessageDialog.openInformation(
			shell,
			CommonNames.PLUGIN_NAME,
			Messages.D_NS_ATTEMPT_STOPPED+dprop.getPort()+"." );
		}
	}
	public void shutdownAllServers() {
		Iterator it = servers.values().iterator();
		while (it.hasNext()) {
			try {
				stopDerbyServer((IProject)it.next());
			}
			catch (Exception e) {
				Logger.log("DerbyServerUtils.shutdownServers",IStatus.ERROR);
				Logger.log(SelectionUtil.getStatusMessages(e), IStatus.ERROR);
			}
		}
	}

}
