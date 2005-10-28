/*

	Derby - Class org.apache.derby.ui.util.DerbyUtils
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

package org.apache.derby.ui.util;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.derby.ui.DerbyPlugin;
import org.apache.derby.ui.common.CommonNames;
import org.apache.derby.ui.properties.DerbyProperties;
import org.eclipse.core.resources.IFile;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Path;
import org.eclipse.core.runtime.Platform;
import org.eclipse.debug.core.DebugPlugin;
import org.eclipse.debug.core.ILaunch;
import org.eclipse.debug.core.ILaunchConfiguration;
import org.eclipse.debug.core.ILaunchConfigurationType;
import org.eclipse.debug.core.ILaunchConfigurationWorkingCopy;
import org.eclipse.debug.core.ILaunchManager;
import org.eclipse.debug.core.model.IProcess;
import org.eclipse.jdt.core.IClasspathEntry;
import org.eclipse.jdt.core.JavaCore;
import org.eclipse.jdt.launching.IJavaLaunchConfigurationConstants;
import org.eclipse.jdt.launching.IRuntimeClasspathEntry;
import org.eclipse.jdt.launching.JavaRuntime;
import org.eclipse.osgi.util.ManifestElement;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;



public class DerbyUtils {
	
	private static ManifestElement[] getElements(String bundleName) throws BundleException {
		String requires = (String)Platform.getBundle(bundleName).getHeaders().get(Constants.BUNDLE_CLASSPATH);
		return ManifestElement.parseHeader(Constants.BUNDLE_CLASSPATH, requires);
	}
	public static IClasspathEntry[] addDerbyJars(IClasspathEntry[] rawCP) throws Exception{
		
		IClasspathEntry[] newRawCP= null;
		try{
			//New OSGI way
			ManifestElement[] elements_core, elements_ui;
			elements_core = getElements(CommonNames.CORE_PATH);
			elements_ui=getElements(CommonNames.UI_PATH);
			
			Bundle bundle=Platform.getBundle(CommonNames.CORE_PATH);
			URL pluginURL = bundle.getEntry("/");
			URL jarURL=null;
			URL localURL=null;

			newRawCP=new IClasspathEntry[rawCP.length + (elements_core.length) + (elements_ui.length-1)];
			System.arraycopy(rawCP, 0, newRawCP, 0, rawCP.length);
			
			//Add the CORE jars
			int oldLength=rawCP.length;
			 for(int i=0;i<elements_core.length;i++){
				jarURL= new URL(pluginURL,elements_core[i].getValue());
				localURL=Platform.asLocalURL(jarURL);
				newRawCP[oldLength+i]=JavaCore.newLibraryEntry(new Path(localURL.getPath()), null, null);
				
			}
			 // Add the UI jars
			bundle=Platform.getBundle(CommonNames.UI_PATH);
			pluginURL = bundle.getEntry("/");
			oldLength=oldLength+elements_core.length -1; 
			for(int i=0;i<elements_ui.length;i++){
				if(!(elements_ui[i].getValue().toLowerCase().equals("ui.jar"))){
					jarURL= new URL(pluginURL,elements_ui[i].getValue());
					localURL=Platform.asLocalURL(jarURL);
					newRawCP[oldLength+i]=JavaCore.newLibraryEntry(new Path(localURL.getPath()), null, null);
				}
			}					
			return newRawCP;
		}catch(Exception e){
			throw e;
		}
		
	}
	public static IClasspathEntry[] removeDerbyJars(IClasspathEntry[] rawCP) throws Exception{
		ArrayList arrL=new ArrayList();
		for (int i=0;i<rawCP.length;i++){
			arrL.add(rawCP[i]);
		}
		IClasspathEntry[] newRawCP= null;
		try{
			ManifestElement[] elements_core, elements_ui;
			elements_core = getElements(CommonNames.CORE_PATH);
			elements_ui=getElements(CommonNames.UI_PATH);
			
			Bundle bundle;
			URL pluginURL,jarURL,localURL;

			boolean add;
			IClasspathEntry icp=null;
			for (int j=0;j<arrL.size();j++){
				bundle=Platform.getBundle(CommonNames.CORE_PATH);
				pluginURL = bundle.getEntry("/");
				add=true;
				icp=(IClasspathEntry)arrL.get(j);
				//remove 'core' jars
				for (int i=0;i<elements_core.length;i++){
					jarURL= new URL(pluginURL,elements_core[i].getValue());
					localURL=Platform.asLocalURL(jarURL);
					if(((icp).equals(JavaCore.newLibraryEntry(new Path(localURL.getPath()), null, null)))||
							icp.getPath().toString().toLowerCase().endsWith("derby.jar")||
							icp.getPath().toString().toLowerCase().endsWith("derbynet.jar")||
							icp.getPath().toString().toLowerCase().endsWith("derbyclient.jar")||
							icp.getPath().toString().toLowerCase().endsWith("derbytools.jar")){
						add=false;
					}
				}
				if(!add){
					arrL.remove(j);
					j=j-1;
				}
				//REMOVE 'ui' jars
				bundle=Platform.getBundle(CommonNames.UI_PATH);
				pluginURL = bundle.getEntry("/");
				add=true;
				
				for (int i=0;i<elements_ui.length;i++){
					if(!(elements_ui[i].getValue().toLowerCase().equals("ui.jar"))){
						jarURL= new URL(pluginURL,elements_ui[i].getValue());
						localURL=Platform.asLocalURL(jarURL);					
						if((icp).equals(JavaCore.newLibraryEntry(new Path(localURL.getPath()), null, null))){
							add=false;
						}
					}
				}
				if(!add){
					arrL.remove(j);
					j=j-1;
				}
			}
			newRawCP=new IClasspathEntry[arrL.size()];
			for (int i=0;i<arrL.size();i++){
				newRawCP[i]=(IClasspathEntry)arrL.get(i);
			}
			return newRawCP;
		}catch(Exception e){
			e.printStackTrace();
			//return rawCP;
			throw e;
		}
		
	}
	protected static ILaunch launch(IProject proj, String name, String mainClass, String args, String vmargs, String app) throws CoreException {	
		ILaunchManager manager = DebugPlugin.getDefault().getLaunchManager();
	
		ILaunchConfigurationType type=null;
		if(app.equalsIgnoreCase(CommonNames.START_DERBY_SERVER)){
			//type= manager.getLaunchConfigurationType("org.apache.derby.ui.startDerbyServerLaunchConfigurationType");
			type= manager.getLaunchConfigurationType(CommonNames.START_SERVER_LAUNCH_CONFIG_TYPE);
		}else if(app.equalsIgnoreCase(CommonNames.SHUTDOWN_DERBY_SERVER)){
			//type= manager.getLaunchConfigurationType("org.apache.derby.ui.stopDerbyServerLaunchConfigurationType");
			type= manager.getLaunchConfigurationType(CommonNames.STOP_SERVER_LAUNCH_CONFIG_TYPE);
		}else if(app.equalsIgnoreCase(CommonNames.IJ)){
			//type= manager.getLaunchConfigurationType("org.apache.derby.ui.ijDerbyLaunchConfigurationType");
			type= manager.getLaunchConfigurationType(CommonNames.IJ_LAUNCH_CONFIG_TYPE);
		}else if(app.equalsIgnoreCase(CommonNames.SYSINFO)){
			//type= manager.getLaunchConfigurationType("org.apache.derby.ui.sysinfoDerbyLaunchConfigurationType");
			type= manager.getLaunchConfigurationType(CommonNames.SYSINFO_LAUNCH_CONFIG_TYPE);
		}else{
			type = manager.getLaunchConfigurationType(IJavaLaunchConfigurationConstants.ID_JAVA_APPLICATION);
		}
		ILaunchConfiguration config = null;
		// if the configuration already exists, delete it
		ILaunchConfiguration[] configurations = manager.getLaunchConfigurations(type);
		for (int i = 0; i < configurations.length; i++) {
			if (configurations[i].getName().equals(name))
				configurations[i].delete();
		}
		// else create a new one
		if (config == null) {
			ILaunchConfigurationWorkingCopy wc = type.newInstance(null, name);
			wc.setAttribute(IJavaLaunchConfigurationConstants.ATTR_PROJECT_NAME,
				proj.getProject().getName());
			// current directory should be the project root
			wc.setAttribute(IJavaLaunchConfigurationConstants.ATTR_WORKING_DIRECTORY,
				proj.getProject().getLocation().toString());
			// use the suplied args
			if((vmargs!=null)&&!(vmargs.equals(""))){
				wc.setAttribute(IJavaLaunchConfigurationConstants.ATTR_VM_ARGUMENTS, vmargs);
			}
			wc.setAttribute(IJavaLaunchConfigurationConstants.ATTR_MAIN_TYPE_NAME,
				mainClass);
			wc.setAttribute(IJavaLaunchConfigurationConstants.ATTR_PROGRAM_ARGUMENTS,
				args);
			// saves the new config
			config = wc.doSave();
		}
		ILaunch launch=config.launch(ILaunchManager.RUN_MODE, null);
		config.delete();
		return launch;
	}
	public static void runIJ(IFile currentScript, IProject currentProject) throws CoreException {	

		String launchType="";
		String args="";
		
		//the above some times throws wrong 'create=true|false' errors
		String vmargs="";
		DerbyProperties dprop=new DerbyProperties(currentProject);
		if((dprop.getSystemHome()!=null)&& !(dprop.getSystemHome().equals(""))){
			vmargs+=CommonNames.D_SYSTEM_HOME+dprop.getSystemHome();
		}
		
		if(currentScript!=null){
			launchType=CommonNames.SQL_SCRIPT;
			
			//Preferable to use the full String with quotes to take care of spaces 
			//in file names
			args="\""+currentScript.getLocation().toOSString()+"\"";
		}else{
			launchType=CommonNames.IJ;
			args="";	
		}
		
		ILaunch launch=launch(currentProject,launchType,CommonNames.IJ_CLASS,args, vmargs, CommonNames.IJ);
		IProcess ip=launch.getProcesses()[0];
		String procName="["+currentProject.getName()+"] - "+CommonNames.IJ+" "+args;
		ip.setAttribute(IProcess.ATTR_PROCESS_LABEL,procName);
	}
	public static void runSysInfo(IProject currentProject) throws CoreException {	
		String args="";
		ILaunch launch=launch(currentProject,CommonNames.SYSINFO,CommonNames.SYSINFO_CLASS,args, null, CommonNames.SYSINFO);
		IProcess ip=launch.getProcesses()[0];
		String procName="["+currentProject.getName()+"] - "+CommonNames.SYSINFO;
		ip.setAttribute(IProcess.ATTR_PROCESS_LABEL,procName);
	}
	//another launch mechanism 																	
	public void launch() throws CoreException{
		DerbyPlugin plugin = DerbyPlugin.getDefault();

		// constructs a classpath from the default JRE...
		IPath systemLibs = new Path(JavaRuntime.JRE_CONTAINER);
		IRuntimeClasspathEntry systemLibsEntry = JavaRuntime.newRuntimeContainerClasspathEntry(
			systemLibs, IRuntimeClasspathEntry.STANDARD_CLASSES);
		systemLibsEntry.setClasspathProperty(IRuntimeClasspathEntry.BOOTSTRAP_CLASSES);
		//include org.apache.derby.core plugin
		IRuntimeClasspathEntry derbyCPEntry = null;
		List classpath = new ArrayList();
		classpath.add(systemLibsEntry.getMemento());
		
		try {
			ManifestElement[] elements_core, elements_ui;
			elements_core = getElements(CommonNames.CORE_PATH);
			elements_ui=getElements(CommonNames.UI_PATH);
			
			Bundle bundle;
			URL pluginURL,jarURL,localURL;
			bundle=Platform.getBundle(CommonNames.CORE_PATH);
			pluginURL = bundle.getEntry("/");
			for(int i=0;i<elements_core.length;i++){
				if(!elements_core[i].getValue().toLowerCase().endsWith("derbynet.jar")){
					jarURL= new URL(pluginURL,elements_core[i].getValue());
					localURL=Platform.asLocalURL(jarURL);
					derbyCPEntry = JavaRuntime.newArchiveRuntimeClasspathEntry(new Path(localURL.getPath()));
					derbyCPEntry.setClasspathProperty(IRuntimeClasspathEntry.USER_CLASSES);
					classpath.add(derbyCPEntry.getMemento());
				}
			}
			bundle=Platform.getBundle(CommonNames.CORE_PATH);
			pluginURL = bundle.getEntry("/");
			for(int i=0;i<elements_ui.length;i++){
				if(!elements_ui[i].getValue().toLowerCase().equals("ui.jar")){
					jarURL= new URL(pluginURL,elements_ui[i].getValue());
					localURL=Platform.asLocalURL(jarURL);
					derbyCPEntry = JavaRuntime.newArchiveRuntimeClasspathEntry(new Path(localURL.getPath()));
					derbyCPEntry.setClasspathProperty(IRuntimeClasspathEntry.USER_CLASSES);
					classpath.add(derbyCPEntry.getMemento());
				}
			}
		}
		catch(Exception e) {
			e.printStackTrace();
			Logger.log("Error in launch() "+e,IStatus.ERROR);
		}
	
	}
}
