/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyBuild
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyBuild;

import java.io.*;
import java.util.*;

import org.apache.derby.iapi.services.i18n.MessageService;

public class splitmessages {
	/**
		arg[0] is the destination directory
		arg[1] is the source file.
	*/

	public static void main(String[] args) throws Exception {


		Properties p = new Properties();

		File dir = new File(args[0]);

		File source = new File(args[1]);
		String s = source.getName();
		// loose the suffix
		s = s.substring(0, s.lastIndexOf('.'));
		// now get the locale
		String locale = s.substring(s.indexOf('_'));

		boolean addBase = "_en".equals(locale);


		InputStream is = new BufferedInputStream(new FileInputStream(source), 64 * 1024);

		p.load(is);
		is.close();


		Properties[] c = new Properties[50];
		for (int i = 0; i < 50; i++) {
			c[i] = new Properties();
		}

		for (Enumeration e = p.keys(); e.hasMoreElements(); ) {
			String key = (String) e.nextElement();

			c[MessageService.hashString50(key)].put(key, p.getProperty(key));
		}

		for (int i = 0; i < 50; i++) {
			if (c[i].size() == 0)
				continue;
			OutputStream fos = new BufferedOutputStream(
				new FileOutputStream(new File(dir, "m"+i+locale+".properties")), 16 * 1024);

			c[i].save(fos, (String) null);
			fos.flush();
			fos.close();

			if (addBase) {
				// add duplicate english file as the base
				fos = new BufferedOutputStream(
					new FileOutputStream(new File(dir, "m"+i+".properties")), 16 * 1024);
				c[i].save(fos, (String) null);
				fos.flush();
				fos.close();
			}


		}

		System.out.println("split messages" + locale);
	}
}
