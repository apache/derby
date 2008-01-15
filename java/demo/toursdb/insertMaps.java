/*

   Derby - Class SimpleApp

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package toursdb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.SQLException;


public class insertMaps {

	public static final String CSdriver = new String("org.apache.derby.jdbc.EmbeddedDriver");
	public static final String dbURLCS = new String("jdbc:derby:toursDB");

	public static void main(String[] args) throws Exception {

		try {
			Connection connCS = null;

			System.out.println("Loading the Derby jdbc driver...");
			Class.forName(CSdriver).newInstance();
	
			System.out.println("Getting Derby database connection...");
			connCS = DriverManager.getConnection(dbURLCS);
			System.out.println("Successfully got the Derby database connection...");

			System.out.println("Inserted " + insertRows(null,connCS) + " rows into the ToursDB");

			connCS.close();

		} catch (SQLException e) {
			System.out.println ("FAIL -- unexpected exception: " + e.toString());
			e.printStackTrace();
		} catch (Exception e) {
			System.out.println ("FAIL -- unexpected exception: " + e.toString());
			e.printStackTrace();
		}

	}
	
	public static int insertRows(String path, Connection conn) 
	throws SQLException, FileNotFoundException, IOException {
		PreparedStatement ps = null;

		ps = conn.prepareStatement
		("insert into maps (map_name, region, area, photo_format, picture) values (?,?,?,?,?)");

		ps.setString(1,"BART");
		ps.setString(2,"Bay Area");
		ps.setBigDecimal(3, new BigDecimal("1776.11"));
		ps.setString(4,"gif");
		String fileName;
		if (path == null)
			fileName="BART.gif";
		else
			fileName=path + File.separator + "BART.gif";
		File file = new File (fileName);
		InputStream fileIn = new FileInputStream(file);
		ps.setBinaryStream(5, fileIn, (int)file.length());
		int numrows = ps.executeUpdate();
		fileIn.close();

		ps.setString(1,"Caltrain");
		ps.setString(2,"West Bay");
		ps.setBigDecimal(3, new BigDecimal("1166.77"));
		ps.setString(4,"gif");
		if (path == null)
			fileName="Caltrain.gif";
		else
			fileName=path + File.separator + "Caltrain.gif";
		file = new File (fileName);
		fileIn = new FileInputStream(file);
		ps.setBinaryStream(5, fileIn, (int)file.length());
		numrows = numrows + ps.executeUpdate();
		fileIn.close();

		ps.setString(1,"Light Rail");
		ps.setString(2,"Santa Clara Valley");
		ps.setBigDecimal(3, new BigDecimal("9117.90"));
		ps.setString(4,"gif");
		// To insert LightRail.gif would give an error because that BLOB
		// is larger than the size indicated for the column.
		// But we don't want to make toursDB bigger in the distribution
		if (path == null)
			fileName="BART.gif";
		else
			fileName=path + File.separator + "BART.gif";
		file = new File (fileName);
		fileIn = new FileInputStream(file);
		ps.setBinaryStream(5, fileIn, (int)file.length());
		numrows = numrows + ps.executeUpdate();

		fileIn.close();
		ps.close();
		
		return numrows;
	}

}
