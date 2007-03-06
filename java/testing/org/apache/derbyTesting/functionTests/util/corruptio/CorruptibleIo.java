/*

   Derby -  Derby - Class org.apache.derbyTesting.functionTests.util.corruptio.CorruptibleIo

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

package org.apache.derbyTesting.functionTests.util.corruptio;
import java.io.File;

/*
 * This is a helper class to instrument the CorruptDiskStorageFactory 
 * to modify the i/o opertions before the request are sent to 
 * a real storage factory. 
 * 
 * Tests can specify what type of corruption is required like log/data files
 * and the at what and offset and the length of the corruption to be 
 * done in the write requests. 
 * 
 * Only one instance of this class will exist in the system, Tests should hold
 * onto the instance of this class until they are done sending the i/o 
 * requests by executing statement that will actuall will trigger i/o , 
 * for example a commit will flush the log buffers. Otherwise class garbage 
 * collector can reinitialize the values. 
 * 
 * @version 1.0
 * @see WritableStorageFactory
 * @see StorageFactory
 */

public class CorruptibleIo {

	private static CorruptibleIo instance = new CorruptibleIo();
	private boolean corruptLog = false; //corrupt the log i/o to log*.dat files
	private boolean corruptData = false; //corrupt the files under seg0(data) 
	private int corruptLength; // no of bytes to corrupt
	private int corruptOffset; // offset inside the write request 


	private CorruptibleIo() {
    }

	public static CorruptibleIo getInstance() {
		return instance;
	}

	
	public void setLogCorruption(boolean corrupt) {
		corruptLog = corrupt;
	}

	public void setDataCorruption(boolean corrupt) {
		corruptData = corrupt;
	}
	
	public void setOffset(int off) {
		corruptOffset = off ;
	}

	public void setLength(int len) {
		corruptLength = len;
	}
		
	public int getOffset() {
		return corruptOffset;
	}

	public int getLength(){
		return corruptLength;
	}

	public boolean isCorruptibleFile(File file)
	{
		String name = file.getName();
		String parentName = file.getParent();
		if (parentName.endsWith("log") && name.endsWith("dat")) {
			return corruptLog;
		}
		else if (parentName.endsWith("seg0")) {
			return corruptData;
		}

		return false;
	}

    /**
	 * corrupt the byte array at the specified bytes, currenly this
	 * metods just complemetns the bits at the specified offsets.
     */
    public byte[] corrupt(byte b[], int off, int len)
	{
		if (corruptOffset >= off && (corruptOffset + corruptLength) < (off + len))
		{
			for(int i = corruptOffset ;  i < corruptOffset + corruptLength ; i++)
			{
				//System.out.println(b[i]);
				b[i] = (byte)~b[i];
				//System.out.println(b[i]);
			}
			// System.out.println("Corrupted the write request : Off = " + off + " Length = " + len);
		}else{
			System.out.println("Not valid corrupt request :" + 
							   "Write Request" + "Off=" + off + "size = " + len + 
							   "Corrupt Request" + "Off=" + corruptOffset + 
							   "size = " + corruptLength);
		}
		return b;
	}

}
