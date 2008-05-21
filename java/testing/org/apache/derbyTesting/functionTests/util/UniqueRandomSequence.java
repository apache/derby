/*

Derby - Class org.apache.derbyTesting.functionTests.util.UniqueRandomSequence

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

package org.apache.derbyTesting.functionTests.util;

import java.util.Random;

/**
 * Utility class that generates a sequence of unique numbers in random order.
 * Example of how to use the generator to print all the numbers from 0 to 9
 * in random order:
 * <pre>
 * UniqueRandomSequence sequence = new UniqueRandomSequence(10);
 * while (sequence.hasMore()) {
 *     System.out.println(sequence.nextValue());
 * }
 * </pre>
 */
public class UniqueRandomSequence {

    /** Array of the numbers to be used in the sequence. */
    private final int[] numbers;

    /** Random number generator. */
    private final Random random = new Random();

    /** How many numbers are left in the sequence. */
    private int numbersLeft;

    /**
     * Generate a random sequence with all the numbers from 0 up to
     * {@code length-1}.
     * @param length the length of the sequence
     */
    public UniqueRandomSequence(int length) {
        this(0, length, 1);
    }

    /**
     * Generate a random sequence in the specified range.
     * @param start the smallest number in the sequence
     * @param length the size of the sequence
     * @param step the difference between adjacent numbers if the sequence is
     * sorted
     */
    public UniqueRandomSequence(int start, int length, int step) {
        if (step <= 0) {
            throw new IllegalArgumentException("step must be greater than 0");
        }
        numbers = new int[length];
        for (int i = 0, val = start; i < length; i++, val += step) {
            numbers[i] = val;
        }
        numbersLeft = length;
    }

    /**
     * Check whether there are more numbers in the sequence.
     * @return {@code true} if there are more numbers in the sequence,
     * {@code false} otherwise
     */
    public boolean hasMore() {
        return numbersLeft > 0;
    }

    /**
     * Fetch the next number from the sequence.
     * @return a unique value in this generator's range
     */
    public int nextValue() {
        int pos = random.nextInt(numbersLeft);
        int value = numbers[pos];
        numbers[pos] = numbers[numbersLeft - 1];
        numbersLeft--;
        return value;
    }
}
