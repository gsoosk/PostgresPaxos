package com;

import java.rmi.Naming;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.io.*;

public class Client 
{ 

	private static DatastoreInterface datastore;

	private String address;
	private int port;

	private BufferedReader reader =  new BufferedReader(new InputStreamReader(System.in));

	// com.Client log is stored in logs folder
	private Logger logger = getLogger("logs/client.log");

	public Client(String address, int port) 
	{ 
		this.address = address;
		this.port = port;
	} 

	public void start() {
		while(true) {
			try
			{
				datastore = (DatastoreInterface) Naming.lookup("//"+address+":"+port+"/com.Server");

				System.out.println("Remote connection established  [ host:"+address+", port:"+port+" ]");
				logger.info("Remote connection established  [ host:"+address+", port:"+port+" ]");


				String command ="";

				while (!command.equals("exit")) 
				{ 
					try
					{ 
						// Reads the request command from the user
						System.out.print("Enter request type (put/get/delete/exit): ");
						reader = new BufferedReader(new InputStreamReader(System.in));
						command = reader.readLine().toLowerCase().trim();


						if(command.equals("put")) {
							System.out.print("Enter key: ");
							String key = reader.readLine();
							System.out.print("Enter value: ");
							String value = reader.readLine();
							logger.info("Request Query [ipaddress=" + this.address + ", type=" + command + ", key=" + key + ", value=" + value + "]");

							// calls a remote procedure 'put'
							Response response = datastore.put(key, value);
							logger.info(response.toString());
							System.out.println("com.Response Message: "+response.getMessage());

						}
						else if(command.equals("get")) {
							System.out.print("Enter key: ");
							String key = reader.readLine();
							logger.info("Request Query [ipaddress=" + this.address + ", type=" + command + ", key=" + key + "]");

							// calls a remote procedure 'get'
							Response response = datastore.get(key);
							logger.info(response.toString());
							System.out.println("com.Response Message: "+response.getMessage());
							System.out.println("com.Response result: "+response.getReturnValue());
						}
						else if(command.equals("delete")) {
							System.out.print("Enter key: ");
							String key = reader.readLine();
							logger.info("Request Query [ipaddress=" + this.address + ", type=" + command + ", key=" + key + "]");

							// calls a remote procedure 'delete'
							Response response = datastore.delete(key);
							logger.info(response.toString());
							System.out.println("com.Response Message: "+response.getMessage());
						}
					}
					catch(Exception e) {
						System.out.println("Request cannot be completed, trying to re-establish connection");
						logger.log(Level.SEVERE, "Request cannot be completed", e);
						break;
					}

				}
				
				if(command.equals("exit")) {
					break;
				}
			}
			catch (Exception e) {
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

	//takes in the log-file path and builds a logger object
	private Logger getLogger(String logFile) {
		Logger logger = Logger.getLogger("client_log");  
		FileHandler fh;  

		try {  
			// This stops logs from getting displayed on console
			logger.setUseParentHandlers(false);
			// if file does not exist we create a new file
			File log = new File(logFile);
			if(!log.exists()) {
				log.createNewFile();
			}
			fh = new FileHandler(logFile,true);  
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter);  

		} catch (SecurityException e) {  
			e.printStackTrace();  
		} catch (IOException e) {  
			e.printStackTrace();  
		} 

		return logger;
	}

	public static void main(String args[]) 
	{ 
		try {
			Client client = new Client(args[0], Integer.parseInt(args[1]));
			client.start();
		} 
		catch(ArrayIndexOutOfBoundsException e) {
			System.out.println("Please provide host and port as command line arguments");
		}
		catch(NumberFormatException e) {
			System.out.println("Please provide host and port as command line arguments");
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	} 
} 

