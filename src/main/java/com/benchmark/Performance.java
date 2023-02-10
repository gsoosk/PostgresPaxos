package com.benchmark;

import static net.sourceforge.argparse4j.impl.Arguments.append;
import static net.sourceforge.argparse4j.impl.Arguments.store;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import io.prometheus.client.*;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;



public class Performance {

    private static final Integer INFINITY = -1;

    private Namespace res;

    public static void main(String[] args) throws Exception{
        Performance perf = new Performance();
        try {
            perf.parseArguments(args);
            DefaultExports.initialize(); // export jvm
            HTTPServer server = new HTTPServer.Builder()
                    .withPort(7000)
                    .build();

            perf.start();
            server.close();
        } catch (ArgumentParserException ignored) {
        }

    }

    private void parseArguments(String[] args) throws ArgumentParserException {
        ArgumentParser parser = argParser();
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
            } else {
                parser.handleError(e);
            }
           throw e;
        }
    }


    private String address;
    private int port;
    private PaxosServerGrpc.PaxosServerBlockingStub datastore;
    private PaxosServerGrpc.PaxosServerStub asyncDatastore;
    private ManagedChannel channel;

    private static Logger logger = getLogger("logs/performance.log", true, false);

    //takes in the log-file path and builds a logger object
    private static Logger getLogger(String logFile, Boolean verbose, Boolean fileVerbose) {
        Logger logger = Logger.getLogger("performance_log");
        FileHandler fh;

        try {
            // This stops logs from getting displayed on console
            if (!verbose)
                logger.setUseParentHandlers(false);
            // if file does not exist we create a new file
            if (fileVerbose) {
                File log = new File(logFile);
                if (!log.exists()) {
                    log.createNewFile();
                }
                fh = new FileHandler(logFile, true);
                logger.addHandler(fh);
                SimpleFormatter formatter = new SimpleFormatter();
                fh.setFormatter(formatter);
            }

        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return logger;
    }

    void start() throws Exception {
        /* parse args */
        address = res.getString("address");
        port = res.getInt("port");

        Integer batchSize = res.getInt("batchSize");

        List<Integer> dynamicBatchSize = res.getList("dynamicBatchSize");
        List<Integer> dynamicBatchTimes = res.getList("dynamicBatchTime");
        int currentBatchIndex = 0;
        boolean dynamicBatching = false;
        if (dynamicBatchSize != null && dynamicBatchTimes != null ) {
            if (dynamicBatchSize.size() != dynamicBatchTimes.size())
                throw new Exception("number of dynamic batch times and batch sizes should be equal");
            batchSize = dynamicBatchSize.get(currentBatchIndex);
            dynamicBatching = true;
        }
        Integer recordSize = res.getInt("recordSize");
        long warmup = batchSize != null ? (batchSize / recordSize) * 4L : 100;
        long numRecords = res.getLong("numRecords") == null ? Integer.MAX_VALUE - warmup - 2 : res.getLong("numRecords");
        numRecords += warmup;
        int throughput = res.getInt("throughput");
        String payloadFilePath = res.getString("payloadFile");
        String resultFilePath = res.getString("resultFile") == null ? "result.csv" : res.getString("resultFile") ;
        String metricsFilePath = res.getString("metricsFile") == null ? "metrics.csv" : res.getString("metricsFile") ;
        String partitionId = res.getString("partitionId");
        // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
        String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");
        Long benchmarkTime = res.getLong("benchmarkTime");
        Integer interval = res.getInt("interval");
        Integer timeout = res.getInt("timeout") != null ? res.getInt("timeout") : interval * 2;
        Integer maxRetry = res.getInt("maxRetry");

        List<byte[]> payloadByteList = readPayloadFile(payloadFilePath, payloadDelimiter);


        /* setup perf test */
        byte[] payload = null;
        if (recordSize != null) {
            payload = new byte[recordSize];
        }
        Random random = new Random(0);
        Stats stats = new Stats(numRecords, 1000, resultFilePath, metricsFilePath, recordSize, batchSize, interval, timeout);
        long startMs = System.currentTimeMillis();

        ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

        connectToDataStore();
        logger.info("Running benchmark for partition: " + partitionId);
        logger.info("Cleaning previous database...");
        datastore.clear(Partition.newBuilder().setPartitionId(partitionId).build());
        Thread.sleep(10000);

        int batched = 0;
        int data = 0;
        Map<String, String> values = new HashMap<>();
        List<Long> starts = new ArrayList<>();

        List<Values> retries = Collections.synchronizedList(new ArrayList<>());
        List<List<Long>> retryStarts = Collections.synchronizedList(new ArrayList<>());
        List<Integer> sent = Collections.synchronizedList(new ArrayList<>());
//            HashMap<Integer, Integer>  numOfRetries = new HashMap<>();
        sent.add(0);
        sent.add(0);

        long requestsId = 0;

        boolean batching = batchSize != null;
        long sendingStart = System.currentTimeMillis();
        long lastDynamicBatchCheckpoint = System.currentTimeMillis();
//            TODO: Refactor this function
        for (long i = 0; i < numRecords; i++) {
            payload = generateRandomPayload(recordSize, payloadByteList, payload, random);

            String record =  new String(payload, StandardCharsets.UTF_8);
            long sendStartMs = System.currentTimeMillis();

            if (!batching) {
                stats.nextAdded(payload.length);
                datastore.put(Data.newBuilder()
                        .setKey("test_" + i)
                        .setValue(record)
                        .setPartitionId(partitionId)
                        .build());
                if (warmup < i)
                    stats.nextCompletion(sendStartMs, payload.length);
            }
            else {
                stats.report(System.currentTimeMillis());
                if (batched < batchSize) {
                    values.put("test_" + partitionId + "_" + data, record);
                    data++;
                    batched += record.length();
                    starts.add(System.currentTimeMillis());
                }
                if (batched >= batchSize) {
                    Values request = Values.newBuilder()
                            .putAllValues(values)
                            .setPartitionId(partitionId)
                            .setId(++requestsId)
                            .build();
                    long c = i;
                    List<Long> copyStarts = new ArrayList<>(starts);
                    if (interval == -1) {
                        stats.nextAdded((recordSize + 5) * request.getValuesMap().size());
                        datastore.batch(request);
                        long end = System.currentTimeMillis();
                        for (int j = 0 ; j < starts.size() ; j++) {
                            if (i + j > warmup)
                                stats.nextCompletion(starts.get(j), end, payload.length + 5);
                        }
                    }
                    else {

                        StreamObserver<Result> observer = new StreamObserver<>() {
                            @Override
                            public void onNext(Result result) {
                            }

                            @Override
                            public void onError(Throwable throwable) {
                                retries.add(request);
                                retryStarts.add(copyStarts);
                            }

                            @Override
                            public void onCompleted() {
                                sent.set(0, sent.get(0)-1);
                                long end = System.currentTimeMillis();
                                for (int j = 0 ; j < copyStarts.size() ; j++) {
                                    if (c + j > warmup)
                                        stats.nextCompletion(copyStarts.get(j), end, recordSize + 5);
                                }
                            }
                        };
                        stats.nextAdded((recordSize + 5) * request.getValuesMap().size());
                        asyncDatastore.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).batch(request, observer);
                        sent.set(0, sent.get(0)+1);
                        sent.set(1, sent.get(1)+1);
                        long startToWaitTime = System.currentTimeMillis();
                        while (System.currentTimeMillis() < startToWaitTime + interval) {
                            try {
                                Thread.sleep(1);
                                 while (retries.size() != 0) {
//                                         logger.info("retry size is " + retries.size());
                                     if (stats.getNumberOfRetries(retries.get(0).getId()) < maxRetry || maxRetry.equals(INFINITY))
                                        retry(warmup, timeout, stats, retries, retryStarts, sent, c, recordSize);
                                    retries.remove(0);
                                    retryStarts.remove(0);
                                    if (System.currentTimeMillis() < startToWaitTime + interval)
                                        break;
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    values.clear();
                    starts.clear();
                    batched = 0;
                }
            }

            if (benchmarkTime != null) {
                long timeElapsed = (sendStartMs - sendingStart) / 1000;
                if (timeElapsed >= benchmarkTime)
                    break;
            }

            if (dynamicBatching && currentBatchIndex < dynamicBatchSize.size()) {
                long timeElapsed = (sendStartMs - lastDynamicBatchCheckpoint) / 1000;
                if (timeElapsed >= dynamicBatchTimes.get(currentBatchIndex)) {
                    lastDynamicBatchCheckpoint = System.currentTimeMillis();
                    currentBatchIndex++;
                    if (currentBatchIndex < dynamicBatchSize.size()) {
                        batchSize = dynamicBatchSize.get(currentBatchIndex);
                        logger.info("Changed batch size to " + batchSize);
                        stats.updateBatchSize(batchSize);
                    }
                }
            }

            if (throttler.shouldThrottle(i, sendStartMs)) {
                throttler.throttle();
            }
        }

        long now = System.currentTimeMillis();
        while (sent.get(0) != 0) {
            try {
                Thread.sleep(10);
                if (System.currentTimeMillis() - now > timeout)
                    break;
//                    while (retries.size() != 0) {
////                        logger.info("retry size is " + retries.size());
//                        retry(warmup, timeout, stats, retries, retryStarts, sent, warmup + 1, recordSize);
//                        retries.remove(0);
//                        retryStarts.remove(0);
//                    }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        System.out.println("sent : " + sent.get(1));
        System.out.println("Should retry is " + retries.size());

        // TODO: closing open things?
        /* print final results */
        stats.printTotal();


    }

    private void retry(long warmup, Integer timeout, Stats stats, List<Values> retries, List<List<Long>> retryStarts, List<Integer> sent, long c, int length) {
        List<Long> toRetryStart = retryStarts.get(0);
        Values retryRequest = retries.get(0);
        stats.addRetry(retryRequest.getId());
        StreamObserver<Result> retryObserver = new StreamObserver<>() {
            @Override
            public void onNext(Result result) {
            }

            @Override
            public void onError(Throwable throwable) {
                retries.add(retryRequest);
                retryStarts.add(toRetryStart);
            }

            @Override
            public void onCompleted() {
                logger.info("retry complete");
                stats.completeRetry();
                sent.set(0, sent.get(0)-1);
                long end = System.currentTimeMillis();
                for (int i = 0; i < toRetryStart.size() ; i++) {
                    if (c + i > warmup)
                        stats.nextCompletion(toRetryStart.get(i), end, length + 5);
                }
            }
        };
        stats.nextAdded((length + 5) * retryRequest.getValuesMap().size());
        asyncDatastore.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).batch(retryRequest, retryObserver);
    }

    private void connectToDataStore() throws MalformedURLException, RemoteException {
        this.channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
        ManagedChannel asyncChannel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
        this.datastore = PaxosServerGrpc.newBlockingStub(channel);
        this.asyncDatastore = PaxosServerGrpc.newStub(asyncChannel);

        while (true) {
            try {
                if (this.channel.getState(true) == ConnectivityState.READY &&
                    asyncChannel.getState(true) == ConnectivityState.READY)
                    break;
            } catch (Exception e) {
                System.out.println("Remote connection failed, trying again in 5 seconds");
                logger.log(Level.SEVERE, "Remote connection failed", e);
                // wait for 5 seconds before trying to re-establish connection
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
    }


    static byte[] generateRandomPayload(Integer recordSize, List<byte[]> payloadByteList, byte[] payload,
                                        Random random) {
        if (!payloadByteList.isEmpty()) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        } else if (recordSize != null) {
            for (int j = 0; j < payload.length; ++j)
                payload[j] = (byte) (random.nextInt(26) + 65);
        } else {
            throw new IllegalArgumentException("no payload File Path or record Size provided");
        }
        return payload;
    }


    static List<byte[]> readPayloadFile(String payloadFilePath, String payloadDelimiter) throws IOException {
        List<byte[]> payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            logger.info("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0)  {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            String[] payloadList = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split(payloadDelimiter);

            logger.info("Number of messages read: " + payloadList.length);

            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }
        }
        return payloadByteList;
    }

    /** Get the command-line argument parser. */
    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        MutuallyExclusiveGroup payloadOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --record-size or --payload-file must be specified but not both.");

        MutuallyExclusiveGroup numberOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --num-records or --benchmark-time must be specified but not both.");

        parser.addArgument("--address")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ADDRESS")
                .dest("address")
                .help("leader's address");

        parser.addArgument("--port")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("PORT")
                .dest("port")
                .help("leader's port");

        numberOptions.addArgument("--num-records")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        numberOptions.addArgument("--benchmark-time")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("BENCHMARK-TIME")
                .dest("benchmarkTime")
                .help("benchmark time in seconds");

        payloadOptions.addArgument("--record-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("RECORD-SIZE")
                .dest("recordSize")
                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

        payloadOptions.addArgument("--payload-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-FILE")
                .dest("payloadFile")
                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
                        "Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--result-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("RESULT-FILE")
                .dest("resultFile")
                .help("a csv file containing the total result of benchmark");

        parser.addArgument("--metric-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("METRIC-FILE")
                .dest("metricsFile")
                .help("a csv file containing the timeline result of benchmark");

        parser.addArgument("--partition-id")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("PARTITION-ID")
                .dest("partitionId")
                .help("Id of the partition that you want to put load on");

        parser.addArgument("--batch-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("BATCH-SIZE")
                .dest("batchSize")
                .help("batch size in bytes. This producer batches records in this size and send them to kv store");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--interval")
                .action(store())
                .required(true)
                .type(Integer.class)
                .dest("interval")
                .metavar("INTERVAL")
                .help("interval between each packet.  Set this -1 to send packets blocking");

        parser.addArgument("--timeout")
                .action(store())
                .required(false)
                .type(Integer.class)
                .dest("timeout")
                .metavar("TIMEOUT")
                .help("timeout of each batch request. It is two times of interval by default");

        parser.addArgument("--max-retry")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .dest("maxRetry")
                .metavar("MAXRETRY")
                .help("Maximum number of times a request can be retried");

        parser.addArgument("--dynamic-batch-size")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicBatchSize")
                .metavar("DYNAMICBATCHSIZE")
                .help("dynamic batch size until a specific time");

        parser.addArgument("--dynamic-batch-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicBatchTime")
                .metavar("DYNAMICBATCHTIME")
                .help("deadline for a dynamic batch size");





        return parser;
    }

    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        private long numRecords;
        private int recordSize;
        private int batchSize;
        private String resultFilePath;
        private int interval;
        private int timeout;
        private Map<Long, Integer> retries;
        private int previousWindowRequestRetried;
        private int previousWindowRetries;
        private int completedRetries;
        private int previousWindowCompletedRetries;

        private String metricsFilePath;
        private int started;
        private int startedBytes;
        private int startedWindowBytes;

        // Metrics
        private static final Summary finishedRequestsBytes = Summary.build()
                .name("paxos_requests_finished_bytes")
                .help("size of requests that finished")
                .register();
        private static final Summary addedRequestsBytes = Summary.build()
                .name("paxos_requests_sent_bytes")
                .help("size of requests sent to the leader")
                .register();
        private static final Gauge batchSizeGauge = Gauge.build()
                .name("paxos_batch_size")
                .help("Batch size")
                .register();
        private static final Gauge intervalGauge = Gauge.build()
                .name("paxos_interval_total")
                .help("Interval between requests")
                .register();
        private static final Gauge timeoutGauge = Gauge.build()
                .name("paxos_timeout_total")
                .help("timeout of a request")
                .register();
        private static final Summary finishedRequestsLatency = Summary.build()
                .name("paxos_requests_finished_latency")
                .help("Latency of requests responded")
                .quantile(0.5, 0.001)    // 0.5 quantile (median) with 0.01 allowed error
                .quantile(0.95, 0.005)  // 0.95 quantile with 0.005 allowed error
                .register();
        private static final Counter requestRetried = Counter.build()
                .name("paxos_requests_retired_total")
                .help("Number of requests that retried")
                .register();
        private static final Counter allRetries = Counter.build()
                .name("paxos_request_retries_total")
                .help("Number of request retries")
                .register();
        private static final Counter retriesCompleted = Counter.build()
                .name("paxos_request_retry_completed_total")
                .help("Number of retries that has been compelted")
                .register();

        public Stats(long numRecords, int reportingInterval, String resultFilePath, String metricsFilePath, int recordSize, int batchSize, int interval, int timeout) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.started = 0;
            this.startedBytes = 0;
            this.startedWindowBytes = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.previousWindowRequestRetried = 0;
            this.previousWindowRetries = 0;
            this.completedRetries = 0;
            this.previousWindowCompletedRetries = 0;
            this.reportingInterval = reportingInterval;
            this.resultFilePath = resultFilePath;
            this.metricsFilePath = metricsFilePath;
            this.numRecords = numRecords;
            this.recordSize = recordSize;
            this.batchSize = batchSize;
            batchSizeGauge.set(batchSize);
            this.interval = interval;
            intervalGauge.set(interval);
            this.timeout = timeout;
            timeoutGauge.set(timeout);
            this.retries = new HashMap<>();
            createResultCSVFiles(resultFilePath, metricsFilePath);
        }

        private void createResultCSVFiles(String resultFilePath, String metricFilePath) {
            if (!Files.exists(Paths.get(resultFilePath))){
                String CSVHeader = "num of records, record size, interval, timeout, batch size, throughput, goodput, average latency, max latency, 50th latency, 95th latency, requests retried, retries, completed retries\n";
                try {
                    BufferedWriter out = new BufferedWriter(
                            new FileWriter(resultFilePath, true));

                    // Writing on output stream
                    out.write(CSVHeader);
                    // Closing the connection
                    out.close();
                }
                catch (IOException ex) {
                    logger.warning("Invalid path");
                }
            }

            String CSVHeader = "num of records, record size, interval, timeout, batch size, throughput, goodput, average latency, max latency, requests retried, retries, completed retries\n";
            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(metricFilePath, false));

                // Writing on output stream
                out.write(CSVHeader);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                logger.warning("Invalid path");
            }
        }

        public synchronized void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            finishedRequestsBytes.observe(bytes);
            finishedRequestsLatency.observe((double) latency/1000);
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            report(time);
        }

        private void report(long time) {
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public void nextCompletion(long start, int bytes) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            record(iteration, latency, bytes, now);
            this.iteration++;
        }

        public synchronized void nextAdded(int bytes) {
            long now = System.currentTimeMillis();
            this.started++;
            addedRequestsBytes.observe(bytes);
            this.startedBytes += bytes;
            this.startedWindowBytes += bytes;

            report(now);
        }

        public void nextCompletion(long start, long end, int bytes) {
            int latency = (int) (end - start);
            record(iteration, latency, bytes, end);
            this.iteration++;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0);
            double throughputMbPerSec = 1000.0 * this.startedWindowBytes / (double) elapsed / (1024.0);
            logger.info(String.format("%d records sent, %.1f records/sec (%.3f KB/sec) of (%.3f KB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    throughputMbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency));



            String resultCSV = String.format("%d,%d,%d,%d,%d,%.3f,%.3f,%.2f,%.2f,%d,%d,%d\n",
                    count,
                    recordSize,
                    interval,
                    timeout,
                    batchSize,
                    throughputMbPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) maxLatency,
                    retries.size() - previousWindowRetries,
                    retries.values().stream().mapToInt(Integer::intValue).sum() - previousWindowRequestRetried,
                    completedRetries - previousWindowCompletedRetries);

            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(metricsFilePath, true));

                // Writing on output stream
                out.write(resultCSV);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                logger.warning("Invalid path");
            }
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.startedWindowBytes = 0;
            previousWindowRetries = retries.size();
            previousWindowRequestRetried = retries.values().stream().mapToInt(Integer::intValue).sum();
            previousWindowCompletedRetries = completedRetries;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0);
            double throughputMbPerSec = 1000.0 * this.startedBytes / (double) elapsed / (1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            logger.info(String.format("%d records sent, %f records/sec (%.3f KB/sec) of (%.3f KB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n --  requests retried: %d, retries: %d\n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    throughputMbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3],
                    retries.size(),
                    retries.values().stream().mapToInt(Integer::intValue).sum()));

            String resultCSV = String.format("%d,%d,%d,%d,%d,%.3f,%.3f,%.2f,%.2f,%d,%d,%d,%d,%d\n",
                    count,
                    recordSize,
                    interval,
                    timeout,
                    batchSize,
                    throughputMbPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    retries.size(),
                    retries.values().stream().mapToInt(Integer::intValue).sum(),
                    completedRetries);
            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(resultFilePath, true));

                // Writing on output stream
                out.write(resultCSV);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                logger.warning("Invalid path");
            }
            logger.info(resultCSV);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }

        public void addRetry(long c) {
            allRetries.inc();
            if (retries.containsKey(c))
                retries.put(c, retries.get(c) + 1);
            else {
                retries.put(c, 1);
                requestRetried.inc();
            }
        }

        public Integer getNumberOfRetries(long c) {
            return retries.getOrDefault(c, 0);
        }

        public void updateBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            batchSizeGauge.set(batchSize);
        }

        public void completeRetry() {
            retriesCompleted.inc();
            completedRetries ++;
        }
    }



}