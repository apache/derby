/*

	Derby - Class org.apache.derby.ui.properties.DerbyProperties
	
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

package org.apache.derby.ui.properties;

import org.apache.derby.ui.common.CommonNames;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.QualifiedName;
import org.eclipse.jdt.core.IJavaProject;



public class DerbyProperties {

	public static final String DSPORT = "ds.port";
	//public static final String DS_RUNNING_PORT = "ds.running.port";
	public static final String DSHOST = "ds.host";
	public static final String DS_SYS_HOME = "derby.system.home";
	
	//Default Derby Properties
	private int port = 1527;
	//private int runningPort=0;
	private String host = "localhost";
	private String systemHome = ".";
	
	public DerbyProperties() {}
	
	public DerbyProperties(IJavaProject javaProject) throws CoreException {
		load(javaProject.getProject());
	}
	public DerbyProperties(IProject project) throws CoreException {
		load(project);
	}
	
	public void save(IProject project) throws CoreException {
		
		project.setPersistentProperty(new QualifiedName (
			CommonNames.UI_PATH, DSPORT), Integer.toString(port));
		project.setPersistentProperty(new QualifiedName (
			CommonNames.UI_PATH, DSHOST), host);
		project.setPersistentProperty(new QualifiedName (
			CommonNames.UI_PATH, DS_SYS_HOME), systemHome);
//		project.setPersistentProperty(new QualifiedName (
//				CommonNames.UI_PATH, DS_RUNNING_PORT), Integer.toString(runningPort));
	}
	
	public void load(IProject project) throws CoreException {
		
		String property = project.getPersistentProperty(new QualifiedName (
				CommonNames.UI_PATH, DSPORT));
		port = (property != null && property.length() > 0) ? Integer.parseInt(property) : port;
		property = project.getPersistentProperty(new QualifiedName (
				CommonNames.UI_PATH, DSHOST));
		host = (property != null && property.length() > 0) ? property : host;
		property = project.getPersistentProperty(new QualifiedName (
				CommonNames.UI_PATH, DS_SYS_HOME));
		systemHome = (property != null && property.length() > 0) ? property : systemHome;
//		property = project.getPersistentProperty(new QualifiedName (
//				CommonNames.UI_PATH, DS_RUNNING_PORT));
//		runningPort = (property != null && property.length() > 0) ? Integer.parseInt(property) : runningPort;
	}
	public String toString(){
		return "Derby Server Properties:\n Port = "+getPort()+" Host = "+getHost()+" System Home = "+getSystemHome();
	}
	
	/**
	 * @return Returns the host.
	 */
	public String getHost() {
		return host;
	}
	/**
	 * @param host The host to set.
	 */
	public void setHost(String host) {
		this.host = host;
	}
	/**
	 * @return Returns the port.
	 */
	public int getPort() {
		return port;
	}
	/**
	 * @param port The port to set.
	 */
	public void setPort(int port) {
		this.port = port;
	}
	/**
	 * @return Returns the systemHome.
	 */
	public String getSystemHome() {
		return systemHome;
	}
	/**
	 * @param systemHome The systemHome to set.
	 */
	public void setSystemHome(String systemHome) {
		this.systemHome = systemHome;
	}
	
}

