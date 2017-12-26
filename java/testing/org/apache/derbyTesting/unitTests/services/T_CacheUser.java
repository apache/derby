/*

   Derby - Class org.apache.derbyTesting.unitTests.services.T_CacheUser

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

package org.apache.derbyTesting.unitTests.services;

import org.apache.derby.iapi.services.cache.*;

import org.apache.derbyTesting.unitTests.harness.T_Fail;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

public class T_CacheUser implements Runnable {

	protected CacheManager	cm;
	protected int           iterations;
	protected HeaderPrintWriter out;
	protected T_CacheService parent;

	public T_CacheUser(CacheManager cm, int iterations, T_CacheService parent, HeaderPrintWriter out) {
		this.cm = cm;
		this.iterations = iterations;
		this.parent = parent;
		this.out = out;
	}


	public void run() {
		try {
			thrashCache();
		} catch (T_Fail tf) {
			parent.setChildException(tf);
		} catch (StandardException se) {
			parent.setChildException(T_Fail.exceptionFail(se));
		}
	}
	/**
		T_CachedInteger range - 0 - 100

		pick a key randomly
					48%/48%/4% chance of Int/String/invalid key
					90%/5%/5% chance of can find / can't find / raise exception
					50%/30%/20% find/findCached/create
					

		@exception StandardException  Standard Derby Error policy
		@exception T_Fail  Some error
				
		
	*/

	public void thrashCache() throws StandardException, T_Fail {

		// stats
		int f = 0, fs = 0, ff = 0, fe = 0;
		int fc = 0, fcs = 0, fcf = 0;
		int c = 0, cs = 0, cf = 0, ce = 0, cse = 0;
		int cleanAll = 0, ageOut = 0;
		int release = 0, remove = 0;


		for (int i = 0; i < iterations; i++) {

			if ((i % 100) == 0)
				out.printlnWithHeader("iteration " + i);

			T_Key tkey = T_Key.randomKey();

			double rand = Math.random();
			T_Cacheable e = null;
			if (rand < 0.5) {
				f++;

				try {

					e = (T_Cacheable) cm.find(tkey);
					if (e == null) {
						ff++;
						continue;
					}

					fs++;

				} catch (T_CacheException tc) {
					if (tc.getType() == T_CacheException.ERROR)
						throw tc;

					// acceptable error
					fe++;
					continue;
				}			
			} else if (rand < 0.8)  {

				fc++;

				e = (T_Cacheable) cm.findCached(tkey);
				if (e == null) {
					fcf++;
					continue;
				}
				fcs++;

			} else {
				c++;

				try {

					e = (T_Cacheable) cm.create(tkey, Thread.currentThread());
					if (e == null) {
						cf++;
						continue;
					}

					cs++;
 
				} catch (T_CacheException tc) {
					if (tc.getType() == T_CacheException.ERROR)
						throw tc;

					// acceptable error
					ce++;
					continue;
				} catch (StandardException se) {

					if (se.getMessageId().equals(SQLState.OBJECT_EXISTS_IN_CACHE)) {
						cse++;
						continue;
					}
					throw se;
				}			
			}

			// ensure we can find it cached and that the key matches
			cm.release(parent.t_findCachedSucceed(cm, tkey));

			if (Math.random() < 0.25)
				e.setDirty();

			if (Math.random() < 0.75)
				Thread.yield();

			if ((Math.random() < 0.10) && (e.canRemove())) {
				remove++;
				cm.remove(e);
			} else {
				release++;
				cm.release(e);
			}
			e = null;

			double rand2 = Math.random();
			
			if (rand2 < 0.02) {
				cleanAll++;
				cm.cleanAll();
			}
			else if (rand2 < 0.04) {
				ageOut++;
				cm.ageOut();
			}
		}

		// ensure all our output in grouped.
		synchronized (parent) {
			out.printlnWithHeader("find()       calls " + f  + " : found/not found/exception : " + fs + "/" + ff + "/" + fe);
			out.printlnWithHeader("findCached() calls " + fc + " : found/not found           : " + fcs + "/" + fcf);
			out.printlnWithHeader("create()     calls " + c  + " : found/not found/exception/standard exception : " + cs + "/" + cf + "/" + ce + "/" + cse);
			out.printlnWithHeader("release()    calls " + release);
			out.printlnWithHeader("remove()     calls " + remove);
			out.printlnWithHeader("cleanAll()   calls " + cleanAll);
			out.printlnWithHeader("ageOut()     calls " + ageOut);
		}

	}
}
