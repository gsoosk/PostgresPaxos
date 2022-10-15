package com.benchmark;

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
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.*;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;



public class Performance {

    public static void main(String[] args) throws Exception {
        Performance perf = new Performance();
        perf.start(args);
    }

    private String address;
    private int port;
    private PaxosServerGrpc.PaxosServerBlockingStub datastore;
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

    void start(String[] args) throws IOException {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            address = res.getString("address");
            port = res.getInt("port");

            Integer batchSize = res.getInt("batchSize");
            Integer recordSize = res.getInt("recordSize");
            long warmup = (batchSize / recordSize) * 4L;
            long numRecords = res.getLong("numRecords") == null ? Integer.MAX_VALUE - warmup - 2 : res.getLong("numRecords");
            numRecords += warmup;
            int throughput = res.getInt("throughput");
            String payloadFilePath = res.getString("payloadFile");
            String resultFilePath = res.getString("resultFile") == null ? "result.csv" : res.getString("resultFile") ;
            String partitionId = res.getString("partitionId");
            // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
            String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");
            Long benchmarkTime = res.getLong("benchmarkTime");


            List<byte[]> payloadByteList = readPayloadFile(payloadFilePath, payloadDelimiter);


            /* setup perf test */
            byte[] payload = null;
            if (recordSize != null) {
                payload = new byte[recordSize];
            }
            Random random = new Random(0);
            Stats stats = new Stats(numRecords, 5000, resultFilePath, recordSize, batchSize);
            long startMs = System.currentTimeMillis();

            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

            connectToDataStore();
            logger.info("Running benchmark for partition: " + partitionId);
            datastore.clear(Partition.newBuilder().setPartitionId(partitionId).build());

            int batched = 0;
            int data = 0;
            Map<String, String> values = new HashMap<>();
            List<Long> starts = new ArrayList<>();
            boolean batching = batchSize != null;
            for (long i = 0; i < numRecords; i++) {
                payload = generateRandomPayload(recordSize, payloadByteList, payload, random);

                String record =  new String(payload, StandardCharsets.UTF_8);
                long sendStartMs = System.currentTimeMillis();

                if (!batching) {
                    datastore.put(Data.newBuilder()
                            .setKey("test_" + i)
                            .setValue(record)
                            .setPartitionId(partitionId)
                            .build());
                    if (warmup < i)
                        stats.nextCompletion(sendStartMs, payload.length);
                }
                else {
                    if (batched < batchSize) {
                        values.put("test_" + partitionId + "_" + data, record);
                        data++;
                        batched += record.length();
                        starts.add(sendStartMs);
                    }
                    if (batched >= batchSize) {
                        datastore.batch(Values.newBuilder()
                                .putAllValues(values)
                                .setPartitionId(partitionId)
                                .build());
                        values.clear();
                        for (int j = 0 ; j < starts.size() ; j++) {
                            if (i + j > warmup)
                                stats.nextCompletion(starts.get(j), payload.length + 5);
                        }
                        starts.clear();
                        batched = 0;
                    }
                }

                if (benchmarkTime != null) {
                    long timeElapsed = (sendStartMs - startMs) / 1000;
                    if (timeElapsed >= benchmarkTime)
                        break;
                }

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }

            // TODO: closing open things?
            /* print final results */
            stats.printTotal();

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
            } else {
                parser.handleError(e);
            }
        }


    }

    private void connectToDataStore() throws MalformedURLException, RemoteException {
        this.channel = ManagedChannelBuilder.forAddress(address, port).usePlaintext().build();
        this.datastore = PaxosServerGrpc.newBlockingStub(channel);

        while (true) {
            try {
                if (this.channel.getState(true) == ConnectivityState.READY)
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

        public Stats(long numRecords, int reportingInterval, String resultFilePath, int recordSize, int batchSize) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
            this.resultFilePath = resultFilePath;
            this.numRecords = numRecords;
            this.recordSize = recordSize;
            this.batchSize = batchSize;
            if (!Files.exists(Paths.get(resultFilePath))){
                String CSVHeader = "num of records, record size, batch size, throughput, average latency, max latency, 50th latency, 95th latency\n";
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
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
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
            /* maybe report the recent perf */
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

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0);
            logger.info(String.format("%d records sent, %.1f records/sec (%.3f KB/sec), %.1f ms avg latency, %.1f ms max latency.%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency));
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            logger.info(String.format("%d records sent, %f records/sec (%.3f KB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3]));

            String resultCSV = String.format("%d,%d,%d,%.3f,%.2f,%.2f,%d,%d\n",
                    count,
                    recordSize,
                    batchSize,
                    mbPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1]);
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
    }



}