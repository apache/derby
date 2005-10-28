/*

	Derby - Class org.apache.derby.ui.properties.DerbyPropertiesPage
	
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
import org.apache.derby.ui.common.Messages;
import org.apache.derby.ui.util.DerbyServerUtils;
import org.apache.derby.ui.util.Logger;
import org.apache.derby.ui.util.SelectionUtil;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.jface.dialogs.MessageDialog;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.dialogs.PropertyPage;



public   class DerbyPropertiesPage extends PropertyPage {
	public DerbyPropertiesPage() {
		super();
	}
	protected DerbyProperties dsProps;
	protected Text hostText;
	protected Text portText;
	protected Text systemHomeText;
	private boolean isServerRunning;
	

	protected void addControls(Composite parent) {
		Composite composite = createDefaultComposite(parent);
		
		//Network Server Settings
		Label txt=new Label(composite,SWT.NONE);
		txt.setBackground(new Color(null,0,0,0));
		txt.setForeground(new Color(null,255,255,255));
		txt.setText("Network Server Settings:");
		
		//separator
		Label separatorLabel=new Label(composite, SWT.SEPARATOR | SWT.HORIZONTAL);
	    separatorLabel.setLayoutData(getSeperatorLabelGridData());
	
		org.eclipse.swt.widgets.
		Label portLabel = new Label(composite, SWT.NONE);
		portLabel.setText("&Network Server Port:");
		portText = new Text(composite, SWT.SINGLE | SWT.BORDER);
		GridData gd = new GridData();
		gd.widthHint = convertWidthInCharsToPixels(6);
		portText.setLayoutData(gd);
	
		Label hostLabel = new Label(composite, SWT.NONE);
		hostLabel.setText("&Network Server Host:");
		hostText = new Text(composite, SWT.SINGLE | SWT.BORDER);
		gd = new GridData();
		gd.widthHint = convertWidthInCharsToPixels(16);
		hostText.setLayoutData(gd);
	
		//Derby System Properties
		separatorLabel=new Label(composite, SWT.NONE);
		separatorLabel.setLayoutData(getSeperatorLabelGridData());
		separatorLabel.setText("");
	
		Label txt1=new Label(composite,SWT.NONE);
		txt1.setBackground(new Color(null,0,0,0));
		txt1.setForeground(new Color(null,255,255,255));
		txt1.setText("Derby System Properties:");
		
		//separator
		separatorLabel=new Label(composite, SWT.SEPARATOR | SWT.HORIZONTAL);
	    separatorLabel.setLayoutData(getSeperatorLabelGridData());
		
		Label sytemHomeLabel = new Label(composite, SWT.NONE);
		sytemHomeLabel.setText("&derby.system.home=");
		systemHomeText = new Text(composite, SWT.SINGLE | SWT.BORDER);
		gd = new GridData();
		gd.widthHint = convertWidthInCharsToPixels(16);
		systemHomeText.setLayoutData(gd);
	}

	protected Composite createDefaultComposite(Composite parent) {
		Composite composite = new Composite(parent, SWT.NULL);
		GridLayout layout = new GridLayout();
		layout.numColumns = 2;
		composite.setLayout(layout);
	
		GridData data = new GridData();
		data.verticalAlignment = GridData.FILL;
		data.horizontalAlignment = GridData.FILL;
		composite.setLayoutData(data);
	
		return composite;
	}

	protected void fillControls() {
		portText.setText(Integer.toString(dsProps.getPort()));
		hostText.setText(dsProps.getHost());
		systemHomeText.setText(dsProps.getSystemHome());
		isServerRunning = checkServer();
		// if the server is running do not allow
		// editing of the settings
		if (isServerRunning) {
		    portText.setEditable(false);
		    hostText.setEditable(false);
		    systemHomeText.setEditable(false);
		}
	}
	
	protected boolean checkServer() {
	    IProject proj = (IProject)getElement();
	    boolean serverRunning = false;
	    try {
	        serverRunning = DerbyServerUtils.getDefault().getRunning(proj);
	    }
	    catch (CoreException ce) {
	        Logger.log(SelectionUtil.getStatusMessages(ce),IStatus.ERROR);
	    }
	    return serverRunning;
	}

	protected void getParams() {
		dsProps = new DerbyProperties();		
		try {
			dsProps.setPort(Integer.parseInt(portText.getText()));
		}
		catch (NumberFormatException ne) {
			// do nothing; use the default port number
		}
		dsProps.setHost(hostText.getText());
		dsProps.setSystemHome(systemHomeText.getText());
		
		// if the server is running inform the user
		// to stop the server before changing the settings
		if (isServerRunning) {
		    Shell shell = new Shell();
			MessageDialog.openInformation(
			shell,
			CommonNames.PLUGIN_NAME,
			Messages.SERVER_RUNNING );
		}
	}

	protected GridData getSeperatorLabelGridData() {
	
	    GridData gridData = new GridData(GridData.BEGINNING |
	                            GridData.HORIZONTAL_ALIGN_FILL |
	                            GridData.GRAB_VERTICAL |
	                            GridData.BEGINNING |
	                            GridData.VERTICAL_ALIGN_BEGINNING |
	                            GridData.VERTICAL_ALIGN_FILL) ;
	    gridData.horizontalSpan = 2;
	    gridData.grabExcessVerticalSpace  = false;
	    gridData.grabExcessHorizontalSpace = true;
	    return gridData;
	
	}

	protected void performDefaults() {
		dsProps = new DerbyProperties();
		fillControls();
	}
	public boolean performOk() {
		IProject proj = (IProject)getElement();
		getParams();
		try {
		
			dsProps.save(proj.getProject());
		}
		catch (CoreException ce) {
			System.err.println(SelectionUtil.getStatusMessages(ce));
			return false;
		}
		return true;
	}

	protected Control createContents(Composite parent) {
		Composite composite = new Composite(parent, SWT.NONE);
		GridLayout layout = new GridLayout();
		composite.setLayout(layout);
		GridData data = new GridData(GridData.FILL);
		data.grabExcessHorizontalSpace = true;
		composite.setLayoutData(data);
		addControls(composite);
		IProject proj = (IProject)getElement();
		try {
			dsProps = new DerbyProperties(proj);
			fillControls();
		}
		catch (CoreException ce) {
			Logger.log(SelectionUtil.getStatusMessages(ce),IStatus.ERROR);
		}
		return composite;
	}	

}
