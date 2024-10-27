package com.gbft.framework.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;
import java.util.List;
import java.util.ArrayList;

import com.gbft.framework.data.RequestData;
import com.gbft.framework.data.RequestData.Operation;
import com.gbft.framework.utils.AdvanceConfig;
import com.gbft.framework.utils.Config;
import com.gbft.framework.utils.DataUtils;

public class ClientDataset extends Dataset {

    private int clientId;
    private Random random;
    private Map<Integer, LongAdder> lookahead;

    public ClientDataset(int clientId) {
        super();

        this.clientId = clientId;

        random = new Random();
        lookahead = new HashMap<>();
        IntStream.range(0, RECORD_COUNT)
                 .forEach(record -> lookahead.computeIfAbsent(record, x -> new LongAdder()).add(DEFAULT_VALUE));
    }

    @Override
    public void update(RequestData request, int value) {
        super.update(request, value);

        var record = request.getRecord();
        var op = request.getOperation();

        switch (op) {
        case INC:
            lookahead.get(record).increment();
            break;
        case ADD:
            lookahead.get(record).add(request.getValue());
            break;
        default:
            break;
        }
    }

    /*
     * Randomly Generates n transactions of the form (x,y,num) -> which means x transfers y num amount of money
     */
    public List<int[]> generateRandomTransactions(int n) {
        // Create a Random object
        Random random = new Random();

        // List to store arrays
        List<int[]> arrayList = new ArrayList<>();

        // Generate 'n' arrays
        for (int i = 0; i < n; i++) {
            // Create an array of size 3
            int[] array = new int[3];

            // Assign random values
            array[0] = random.nextInt(99) + 1;    // sender's key - 0th element: 1-99
            array[1] = random.nextInt(99) + 1;    // receiver's key - 1st element: 1-99
            array[2] = random.nextInt(100) + 1;  // Amount - 2nd element: 1-10000

            // Add the array to the list
            arrayList.add(array);
        }

        // Return the list of arrays
        return arrayList;
    }

    public RequestData createRequestWithKeyAndVal(long reqnum, long record, long value) {
        var operation = Operation.ADD;
        if (value < 0) {
            operation = Operation.SUB;
        }
        // generate read only optimization
        if (Config.stringList("plugins.message").contains("read-only")) {
            if (random.nextDouble() < AdvanceConfig.doubleNumber("workload.read-only-ratio")) {
                operation = Operation.READ_ONLY;
            }
        }
        return DataUtils.createRequest(reqnum, (int) record, operation, (int) value, clientId);
    }

    public RequestData createRequest(long reqnum) {

        var record = random.nextInt(AdvanceConfig.integer("workload.contention-level"));
        var operation = Operation.values()[random.nextInt(5)];
        int value = 0;

        switch (operation) {
        case ADD:
            value = random.nextInt(DEFAULT_VALUE);
            break;
        case SUB:
            var max = Math.min(DEFAULT_VALUE, lookahead.get(record).intValue());
            if (max <= 0) { // < 0 to fix #47
                operation = Operation.NOP;
            } else {
                value = random.nextInt(max);
                lookahead.get(record).add(-value);
            }
            break;
        case DEC:
            if (lookahead.get(record).intValue() < 1) {
                operation = Operation.NOP;
            } else {
                lookahead.get(record).decrement();
            }
            break;
        default:
            break;
        }

        // generate read only optimization
        if (Config.stringList("plugins.message").contains("read-only")) {
            if (random.nextDouble() < AdvanceConfig.doubleNumber("workload.read-only-ratio")) {
                operation = Operation.READ_ONLY;
            }
        }

        return DataUtils.createRequest(reqnum, record, operation, value, clientId);
    }

}
