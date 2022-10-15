package com;

import io.grpc.*;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class PaxosServer {
    public Logger logger;
    private final Server server;

    private final int port;

    public PaxosServer(String serverID, int port, String postgresPort, ServerBuilder<?> serverBuilder) {
        this.logger = getLogger("logs/"+serverID+"_server.log", true, false);
        this.server = serverBuilder
                .addService(new PaxosServerService(serverID, port, postgresPort, logger))
                .build();
        this.port = port;
    }

    public void start() throws IOException {
        server.start();
        logger.info("Server started, listening on " + port);
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static class PaxosServerService extends PaxosServerGrpc.PaxosServerImplBase {
        private final ServerImplementation serverImplementation;
        private final Logger logger;

        public PaxosServerService(String serverId, int port, String postgresPort, Logger logger) {
            serverImplementation = new ServerImplementation(serverId, port, postgresPort, logger);
            this.logger = logger;
            this.logger.info("New paxos service has been constructed");
        }

        @Override
        public void registerNewServer(ServerDetails request, StreamObserver<Result> responseObserver) {
            serverImplementation.registerNewServer(request.getServerId());
            responseObserver.onNext(Result.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }


		@Override
		public void put(Data request, StreamObserver<Result> responseObserver) {
		    Response response = serverImplementation.put(request.getKey(), request.getValue(), request.getPartitionId());
            responseObserver.onNext(
                    Result.newBuilder()
                            .setSuccess(true)
                            .setMessage(response.getMessage())
                            .build()
            );
            responseObserver.onCompleted();
		}

        @Override
        public void prepare(Proposal request, StreamObserver<PromiseMessage> responseObserver) {
            try {
                Promise promise = serverImplementation.prepare(request.getProposalNumber(), request.getPartitionId());
                PromiseMessage message = PromiseMessage.newBuilder()
                        .setProposalNumber(promise.getProposalNumber())
                        .setPreviousProposalNumber(promise.getPreviousProposalNumber())
                        .build();
                responseObserver.onNext(message);
            } catch (RemoteException e) {
                responseObserver.onError(Status.ABORTED.asRuntimeException());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void accept(Proposal request, StreamObserver<AcceptedMessage> responseObserver) {
            try {
                Accepted accepted = serverImplementation.accept(request.getProposalNumber(), request.getPartitionId());
                responseObserver.onNext(AcceptedMessage.newBuilder()
                        .setProposalNumber(accepted.getProposalNumber())
                        .build());
            } catch (RemoteException e) {
                e.printStackTrace();
            }
            responseObserver.onCompleted();
        }

        @Override
        public void learn(TransactionMessage request, StreamObserver<Result> responseObserver) {
            serverImplementation.invokeLearner(request);
            responseObserver.onNext(Result.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
        }

        @Override
        public void get(Key request, StreamObserver<Result> responseObserver) {
            Response response = serverImplementation.get(request.getKey(), request.getPartitionId());
            responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage(response.getReturnValue()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void delete(Key request, StreamObserver<Result> responseObserver) {
            Response response = serverImplementation.delete(request.getKey(), request.getPartitionId());
            responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage(response.getMessage()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void clear(Partition request, StreamObserver<Result> responseObserver) {
            Response response = serverImplementation.clear(request.getPartitionId());
            responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage(response.getMessage()).build());
            responseObserver.onCompleted();
        }

        @Override
        public void batch(Values request, StreamObserver<Result> responseObserver) {
            Response response = serverImplementation.batch(request.getValuesMap(), request.getPartitionId());
            responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage(response.getMessage()).build());
            responseObserver.onCompleted();
        }
    }

    public static Logger getLogger(String logFile, Boolean verbose, Boolean fileVerbose) {
        Logger logger = Logger.getLogger("server_log");
        logger.setLevel(Level.INFO);
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

    public static String createServerID(int port) {
        String id = null;
        try {
            InetAddress IP = InetAddress.getLocalHost();
            id = IP.getHostAddress()+":"+String.valueOf(port);

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return id;
    }


    public static void main(String args[]) {
        try {
            System.setProperty("java.util.logging.SimpleFormatter.format", "[%1$tF %1$tT : %1$tL] [%4$-7s] %5$s %n");
            int port = Integer.parseInt(args[0]);
            String postgresPort = args[1];
            PaxosServer paxosServer = new PaxosServer(createServerID(port), port, postgresPort, ServerBuilder.forPort(port));

            paxosServer.start();

            String currentServerID = createServerID(port);
            InputStream input = new FileInputStream("resources/config.properties");
            Properties prop = new Properties();
            prop.load(input);
            String[] discoveryNodes = prop.getProperty("discovery.nodes").split(",");

            ManagedChannel channelToSelf = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
            PaxosServerGrpc.PaxosServerBlockingStub selfStub = PaxosServerGrpc.newBlockingStub(channelToSelf);
            selfStub.registerNewServer(ServerDetails.newBuilder().setServerId(currentServerID).build());



            // get discovery nodes to connect to cluster
            paxosServer.logger.info("Trying to connect to a cluster");


            for(String discoveryNode : discoveryNodes)
            {
                String[] data = discoveryNode.split(":");
                String discoveryNodeIPAddress = data[0];
                int discoveryNodePort = Integer.parseInt(data[1]);

                if (isSelf(discoveryNodeIPAddress, discoveryNodePort, currentServerID))
                    continue;

                paxosServer.logger.info("Trying to connect to node " + discoveryNode);

                ManagedChannel channel = ManagedChannelBuilder.forAddress(discoveryNodeIPAddress, discoveryNodePort).usePlaintext().build();
                PaxosServerGrpc.PaxosServerBlockingStub blockingStub = PaxosServerGrpc.newBlockingStub(channel);
                try {
                    blockingStub.registerNewServer(ServerDetails.newBuilder().setServerId(currentServerID).build());
                    paxosServer.logger.info("Registered current server(" + currentServerID +")  in server " + discoveryNode);
                    selfStub.registerNewServer(ServerDetails.newBuilder().setServerId(discoveryNode).build());
                } catch (StatusRuntimeException e) {
                    paxosServer.logger.log(Level.WARNING, "RPC failed to register in remote node }");
                }
                channel.shutdown();
                // TODO: if storage initialization needed

            }

            channelToSelf.shutdown();
            paxosServer.blockUntilShutdown();


		}
		catch(ArrayIndexOutOfBoundsException e) {
			System.out.println("Please provide port as command line argument");
			System.exit(0);
		}
		catch(NumberFormatException e) {
			System.out.println("Please provide port as command line argument");
			System.exit(0);
		}
		catch(java.rmi.ConnectException e) {
			System.err.println("Could not connect to Master com.Server: " + e);
			System.exit(0);
		}
		catch (Exception e) {
			e.printStackTrace();
			System.err.println("com.Server exception: " + e);
			System.exit(0);
		}


    }

    private static boolean isSelf(String discoveryNodeIPAddress, int discoveryNodePort, String currentServerID) {
        String[] data = currentServerID.split(":");
        String ip = data[0];
        int port = Integer.parseInt(data[1]);
        return (discoveryNodeIPAddress.equals("localhost") || discoveryNodeIPAddress.equals("127.0.0.1") || discoveryNodeIPAddress.equals(ip))
                && discoveryNodePort == port;
    }


}
