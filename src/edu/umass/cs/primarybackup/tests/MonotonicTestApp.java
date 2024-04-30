package edu.umass.cs.primarybackup.tests;

import edu.umass.cs.primarybackup.examples.MonotonicApp;

/**
 * MonotonicTestApp extends MonotonicApp with addition of test_{*} methods so our unit tests can
 * access the Application internal state: {@link MonotonicApp#sequence}.
 * This class enable us to assert that the internal state satisfies monotonically increasing
 * stateDiff.
 */
public class MonotonicTestApp extends MonotonicApp {

    protected synchronized void test_AssertMonotonicallyIncreasingNumbers() {
        boolean isMonotonic = true;

        if (this.sequence.isEmpty()) {
            return;
        }

        MonotonicApp.Number prev = sequence.getFirst();
        for (int i = 1; i < sequence.size(); i++) {
            MonotonicApp.Number cur = sequence.get(i);
            if (cur.number() <= prev.number()) {
                isMonotonic = false;
                break;
            }
        }

        assert isMonotonic;
    }

    public synchronized String test_GetSequenceAsString() {
        StringBuilder seqStr = new StringBuilder();
        int size = sequence.size();
        for (MonotonicApp.Number n : sequence) {
            seqStr.append(n.timestamp());
            seqStr.append(":");
            seqStr.append(n.number());
            size = size - 1;
            if (size > 0) {
                seqStr.append(", ");
            }
        }
        return seqStr.toString();
    }

    protected synchronized int test_GetSequenceSize() {
        return this.sequence.size();
    }

}
