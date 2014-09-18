/*

   Derby - Class org.apache.derbyTesting.unitTests.harness.T_Bomb

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

package org.apache.derbyTesting.unitTests.harness;

import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.context.Context;

import java.security.PrivilegedAction;
import java.security.AccessController;
import java.util.Vector;

public class T_Bomb implements Runnable { 
	public static String BOMB_DELAY_PN="derby.testing.BombDelay";
	private static int DEFAULT_BOMB_DELAY=3600000; //1 hour

	private static T_Bomb me;
	
	private Thread t;
	private Vector<T_Bombable> v;
	private long delay;
	private boolean armed = false;

	private T_Bomb()
	{
		delay =
			PropertyUtil.getSystemInt(BOMB_DELAY_PN,0,
									  Integer.MAX_VALUE,
									  DEFAULT_BOMB_DELAY);
		v = new Vector<T_Bombable>();
		t = new Thread(this);
		t.setDaemon(true);
		t.start();
	}

	/**
	  Make an armed bomb set to go off in 1 hour.
	  */
	public synchronized static void makeBomb() {
		if (me==null) me = new T_Bomb();
		me.armBomb();
	}

	/**
	  Arm a bomb to go off. If the bomb does not exist
	  make it.
	  */
	public synchronized void armBomb() {
		if (me == null) me = new T_Bomb();
		me.armed = true;
	}

	/**
	  Cause a bomb to explode. If the bomb does not exist
	  make it.
	  */
	public synchronized static void explodeBomb() {
		if (me == null) me = new T_Bomb();
		me.armed = true;
		me.blowUp();
	}

	public synchronized static void registerBombable(T_Bombable b)
	{
		if (me == null) me = new T_Bomb();
		me.v.addElement(b);
	}

	public synchronized static void unRegisterBombable(T_Bombable b)
	{
		if (null == me || null == b )
            return;
        me.v.removeElement(b);
        if( me.v.isEmpty())
        {
            me.armed = false;
            me.t.interrupt();
            me = null;
        }
	}

	public void run() {

		try {
			Thread.sleep(delay);
		}

		catch (InterruptedException e) {
		}

		if (armed)
		{
			me.blowUp();
		}
	}

	private void blowUp()
	{
			performLastGasp();
			ContextService csf = getContextService();
			if (csf != null)
			{
				System.out.println("ran out of time");
				csf.notifyAllActiveThreads
					((Context) null);
			}

			try {
				Thread.currentThread().sleep(30*1000); //Give threads 30 sec to shut down.
			}
			catch (InterruptedException ie) {}
			System.out.println("Exit due to time bomb");
			Runtime.getRuntime().exit(1234);
	}

	private void performLastGasp()
	{
        for (T_Bombable b : v) {
			try{
			 b.lastChance();
			}


			catch (Exception exc) {
				System.out.println("Last Gasp exception");
				exc.printStackTrace();
			}
		} //end for

	}
    
    /**
     * Privileged lookup of the ContextService. Must be private so that user code
     * can't call this entry point.
     */
    private  static  ContextService    getContextService()
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getFactory();
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<ContextService>()
                 {
                     public ContextService run()
                     {
                         return ContextService.getFactory();
                     }
                 }
                 );
        }
    }

}
