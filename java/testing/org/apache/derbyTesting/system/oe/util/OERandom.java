/*
 *
 * Derby - Class OERandom
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.system.oe.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Random;

import org.apache.derbyTesting.system.oe.client.Load;

/**
 * Methods to implement the random database population types
 * for the Order Entry Benchmark. The rules for generating 
 * the random data is per the TPC-C specification.
 */
public class OERandom {

    final Random rand;

    protected final int Clast;

    protected final int Cid;

    protected final int Citem;
    
    /**
     * Create a matching OERandom, for use in multi-threaded
     * runs where all the submitters need to share the same
     * Clast, Cid and Citem values.
     * @param oer
     */
    public OERandom(OERandom oer) {
        rand = new Random(System.currentTimeMillis());
        Clast = oer.Clast;
        Cid = oer.Cid;
        Citem = oer.Citem;
    }

    public OERandom(int last, long seed) {

        rand = new Random(seed);
        Clast = last;
        Cid = this.randomInt(0, 255);
        Citem = this.randomInt(0, 255);
        
        initAStrings();
    }

    public OERandom(int last) {
        this(last, System.currentTimeMillis());
    }

    private static int[] RESCALE = { 0, 10, 100, 1000, 10000, 100000, 1000000 };

    private StringBuffer decimalString = new StringBuffer(12);

    public String randomDecimalString(int start, int end, int scale) {

        int val = randomInt(start, end);

        int whole = val / RESCALE[scale];

        int part = val % RESCALE[scale];

        decimalString.setLength(0);
        decimalString.append(whole);
        decimalString.append('.');

        int pos = decimalString.length();

        decimalString.append(part);

        int tempScale = decimalString.length() - pos;
        if (tempScale < scale) {
            for (int i = 0; i < (scale - tempScale); i++)
                decimalString.insert(pos, '0');
        }

        return decimalString.toString();
    }
    
    /**
     * Payment amount between 1.00 and 5,000.00
     * @return Payment amount between 1.00 and 5,000.00
     */
    public BigDecimal payment()
    {
        return randomDecimal(1, 500000, 2);
    }

    public BigDecimal randomDecimal(int start, int end, int scale) {
        BigInteger bi = BigInteger.valueOf(randomInt(start, end));
        return new BigDecimal(bi, scale);
    }

    /**
     * tpcc 4.3.2.5 Implements random within [x .. y ] for int
     */
    public int randomInt(int start, int end) {
        double drand = rand.nextDouble();

        double rrand = (drand * (end - start)) + 0.5;

        return ((int) rrand) + start;
    }
    
    /**
     * Return a random district [1..10]
     */
    public short district()
    {
        return (short) randomInt(1, 10);
    }
    
    /**
     * Return a random carrier [1..10]
     */
    public short carrier()
    {
        return (short) randomInt(1, 10);
    }

    /**
     * Return a random threshold for the stock level [10..20]
     */
    public int threshold()
    {
        return randomInt(10, 20);
    }
    
    /**
     * tpcc 4.3.2.2 (random a string)
     */
    public String randomAString(int min, int max) {
        int len = randomInt(min, max);
        char[] c = new char[len];
        for (int i = 0; i < len; i++) {

            double drand = rand.nextDouble();

            if (i == 0) {
                if (drand < 2.0)
                    c[0] = (char) randomInt((int) 'A', (int) 'Z');
                else {
                    switch (randomInt(1, 10)) {
                    case 1:
                        c[0] = '\u00c0';
                        break;
                    case 2:
                        c[0] = '\u00c1';
                        break;
                    case 3:
                        c[0] = '\u00c2';
                        break;
                    case 4:
                        c[0] = '\u00ca';
                        break;
                    case 5:
                        c[0] = '\u00cb';
                        break;
                    case 6:
                        c[0] = '\u00d4';
                        break;
                    case 7:
                        c[0] = '\u00d8';
                        break;
                    case 8:
                        c[0] = '\u00d1';
                        break;
                    case 9:
                        c[0] = '\u00cd';
                        break;
                    default:
                        c[0] = '\u00dc';
                        break;
                    }
                }

                continue;
            }

            if (drand < 2.0)
                c[i] = (char) randomInt((int) 'a', (int) 'z');
            else {
                switch (randomInt(1, 10)) {
                case 1:
                    c[i] = '\u00e2';
                    break;
                case 2:
                    c[i] = '\u00e4';
                    break;
                case 3:
                    c[i] = '\u00e7';
                    break;
                case 4:
                    c[i] = '\u00e8';
                    break;
                case 5:
                    c[i] = '\u00ec';
                    break;
                case 6:
                    c[i] = '\u00ef';
                    break;
                case 7:
                    c[i] = '\u00f6';
                    break;
                case 8:
                    c[i] = '\u00f9';
                    break;
                case 9:
                    c[i] = '\u00fc';
                    break;
                default:
                    c[i] = '\u00e5';
                    break;
                }
            }
        }

        return new String(c);
    }

    /**
     * tpcc 4.3.2.2 (random n string)
     */
    public String randomNString(int min, int max) {
        int len = randomInt(min, max);
        char[] c = new char[len];
        for (int i = 0; i < len; i++) {
            c[i] = (char) randomInt((int) '0', (int) '9');
        }

        return new String(c);
    }

    /**
     * tpcc 4.3.2.3
     */
    private final String[] SYLLABLES = { "BAR", "OUGHT", "ABLE", "PRI", "PRES",
            "ESE", "ANTI", "CALLY", "ATION", "EING" };

    protected String randomCLast(int n) {

        return SYLLABLES[n / 100] + SYLLABLES[(n / 10) % 10]
                + SYLLABLES[n % 10];
    }

    /**
     * Generate the zipcode value
     * return zipcode value according to the requirements specified in 
     * Clause 4.3.2.7 of TPC-C spec
     */
    public String randomZIP() {
        return randomNString(4, 4) + "11111";
    }

    /**
     * Section 2.1.6 of TPC-C specification, for OL_I_ID NURand(A, x, y) =
     * (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x C is a run-time
     * constant randomly chosen within [0 .. A] NURand(8191, 1,100000)
     * 
     * @return nonuniform random number
     */
    public int NURand8191() {
        int l = randomInt(0, 8191);
        int r = randomInt(1, Load.ITEM_COUNT);
        int C = randomInt(0, 8191);
        return ((l | r) + C) % (Load.ITEM_COUNT - 1 + 1) + 1;
    }

    /**
     * Section 2.1.6 of TPC-C specification for CID NURand(A, x, y) =
     * (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x NURand(1023,
     * 1,3000)
     * 
     * @return nonuniform random number
     */
    public int NURand1023() {
        int l = randomInt(0, 1023);
        int r = randomInt(1, (Load.CUSTOMER_COUNT_W / Load.DISTRICT_COUNT_W));
        int C = randomInt(0, 1023);
        return ((l | r) + C)
                % ((Load.CUSTOMER_COUNT_W / Load.DISTRICT_COUNT_W) - 1 + 1) + 1;
    }

    /**
     * Section 2.1.6 of TPC-C specification, for C_LAST NURand(A, x, y) =
     * (((random(0, A) | random(x, y)) + C) % (y - x + 1)) + x NURand(255,0,999)
     * 
     * @return nonuniform random number
     */
    public int NURand255() {
        int l = randomInt(0, 255);
        int r = randomInt(0, 999);
        int C = randomInt(0, 255);
        return ((l | r) + C) % (999 - 0 + 1) + 0;
    }

    public String randomState() {
        StringBuffer s = new StringBuffer(2);
        for (int i = 0; i < 2; i++) {
            s.append((char) randomInt((int) 'A', (int) 'Z'));
        }

        return s.toString();
    }

    /**
     * Clause 4.3.2.3 of the TPC-C specification
     * @param cid - customer id.
     * @return the generated Customer's last name per the requirements
     * in the TPC-C spec. 
     */
    public String randomCLastPopulate(int cid) {
        
        // First thousand customers (C_ID is one based)
        // have a fixed last name based upon the contiguous
        // values from 0-999, section 4.3.3.1
        if (cid <= 1000)
            return randomCLast(cid-1); // range 0 - 999

        return randomCLast(NURand255());
    }

    public String randomCLast() {
        return randomCLast(NURand255());
    }

    /**
     * Clause 4.3.3.1 of TPC-C spec. random a-string [26 .. 50]. For 10%
     * of the rows, selected at random, the string "ORIGINAL" must be held by 8
     * consecutive characters starting at a random position within the string
     * 
     * @return string data per the TPC-C requirements
     */
    public String randomData() {
        String s = randomAString26_50();
        if (rand.nextDouble() < 0.9)
            return s;

        int pos = randomInt(0, s.length() - 9);

        if (pos == 0)
            return "ORIGINAL" + s.substring(8);

        if (pos == (s.length() - 9))
            return s.substring(0, s.length() - 9) + "ORIGINAL";

        return s.substring(0, pos) + "ORIGINAL"
                + s.substring(pos + 8, s.length());
    }

    public int[] randomIntPerm(int count) {
        int[] data = new int[count];

        for (int i = 0; i < count; i++) {
            data[i] = i + 1;
        }

        for (int j = 0; j < (count * 4); j++) {

            int a = randomInt(0, count - 1);
            int b = randomInt(0, count - 1);

            int val = data[a];
            data[a] = data[b];
            data[b] = val;
        }
        return data;
    }

    private final static String[] left24 = new String[10];

    private final static String[] left300 = new String[10];

    private final static String[] right200 = new String[10];

    private final static String[] left10 = new String[10];

    private final static String[] right10 = new String[10];

    private final static String[] left14 = new String[10];

    private final static String[] left26 = new String[10];

    private final static String[] right24 = new String[10];

    private final static String[] left8 = new String[10];

    private final static String[] right8 = new String[10];

    private static boolean doneInit;

    private void initAStrings() {

        synchronized (left24) {
            if (doneInit)
                return;

            for (int i = 0; i < 10; i++) {
                // 24..24
                left24[i] = randomAString(24, 24);

                // 300...500
                left300[i] = randomAString(300, 300);
                right200[i] = randomAString(0, 200);

                // 10...20
                left10[i] = randomAString(10, 10);
                right10[i] = randomAString(0, 10);

                // 14 .. 24
                left14[i] = randomAString(10, 10);

                // 26 .. 50
                left26[i] = randomAString(26, 26);
                right24[i] = randomAString(0, 24);

                // 8 .. 16
                left8[i] = randomAString(8, 8);
                right8[i] = randomAString(0, 8);

            }
            doneInit = true;
        }
    }

    public String randomAString24() {
        return left24[randomInt(0, 9)];
    }

    /**
     * Section 4.3.2.2(and comments 1 and 2).
     * The notation random a-string [x .. y] (respectively,
     * n-string [x ..y]) represents a string of random alphanumeric (respectively, 
     * numeric)characters of a random length of minimum x, maximum y, and mean (y+x)/2.
     * 
     * @return string value.
     */
    public String randomAString300_500() {
        String l = left300[randomInt(0, 9)];
        String r = right200[randomInt(0, 9)];

        return l.concat(r);
    }

    public String randomAString10_20() {
        String l = left10[randomInt(0, 9)];
        String r = right10[randomInt(0, 9)];

        return l.concat(r);
    }

    public String randomAString14_24() {
        String l = left14[randomInt(0, 9)];
        String r = right10[randomInt(0, 9)];

        return l.concat(r);
    }

    public String randomAString26_50() {
        String l = left26[randomInt(0, 9)];
        String r = right24[randomInt(0, 9)];

        return l.concat(r);
    }

    public String randomAString8_16() {
        String l = left8[randomInt(0, 9)];
        String r = right8[randomInt(0, 9)];

        return l.concat(r);
    }

}
