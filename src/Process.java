import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Time;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Scanner;
import java.io.*;
import java.util.Random;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;



public class Process implements Runnable, Serializable{
	private transient ServerSocket serverSocket = null;
	private ArrayList<ConnectedSocket> connectedClientSockets;
	private int myID;
	private String myIP;
	private int myPort;
	private static int minSeconds;
	private static int maxSeconds;
	private static int diff;
	private static String[][] config = new String[4][3];
	transient Scanner scan = new Scanner(System.in);
	private boolean connectBack=true;
	private boolean run = true;
	private int count;
	Random randomGenerator = new Random();
	transient ArrayList<Metadata> arr = new ArrayList<Metadata>();
	 private final Lock lock = new ReentrantLock();
	private int[] vt = new int[4];


	
	public Process(String id, String ip, String port){//constructor
		diff = maxSeconds - minSeconds;
		vt[0]=0;
		vt[1]=0;
		vt[2]=0;
		vt[3]=0;
		myID = Integer.parseInt(id);
		myIP = ip;
		myPort = Integer.parseInt(port);
		connectedClientSockets = new ArrayList<ConnectedSocket>();
		openServerSocket(myIP,myPort);
		Thread t1 = new Thread(new Runnable() {//new thread to keep accepting connections				
			public void run() {
			   waitForConnections();
			}
		});  
		t1.start();
		if(myID!=1)
			connectToServers();//connect to all the servers(only for processes 2,3 and 4)
		sendMessage();
		
	}
	
	public void connectToServers(){
		for(int i=0;i<myID-1;i++){
			String IP = config[i][1];
			int port = Integer.parseInt(config[i][2]);
			String id = config[i][0];
			try{
				Socket client = new Socket(IP, port);//send message to connected server about which process u are
				OutputStream outToServer = (OutputStream) client.getOutputStream();
				OutputStreamWriter out = new OutputStreamWriter(outToServer);
				Integer mess=myID;
				String message = mess.toString();
				out.write(message + "\n");
				out.flush();
				ConnectedSocket c = new ConnectedSocket(client,i+1);
				connectedClientSockets.add(c);
			}catch(IOException e){
				throw new RuntimeException("Error accepting client connection", e);
			}
		}
	}
	
	public void sendMessage(){//send message 
		while(scan.hasNext()){
			vt[myID-1]=vt[myID-1]+1;
			String message = scan.nextLine();
			String[] splitted = message.split(" ");
			if(splitted[0].equals("msend")){//multicast
				for(int i=0;i<connectedClientSockets.size();i++){
					if(connectedClientSockets.get(i).processID!=myID){
						ConnectedSocket c=connectedClientSockets.get(i);
						final int a =i;
						Thread t1 = new Thread(new Runnable() {//new thread to keep accepting connections, this is a way of showing buffering through causal ordering			
							public void run() {
							   if(connectedClientSockets.get(a).processID==2){	
								   write(message,c,2000);//turn message into metadata, if statements for msend
							   }else if(myID==1 && connectedClientSockets.get(a).processID==3){
								   write(message,c,15000);
							   }else if(myID==2 && connectedClientSockets.get(a).processID==3){
								   write(message,c,1000);
							   }else{
								   write(message,c,1000); 
							   }
							}
						});
						t1.start();
					}
				}
				
			}else{
				int process = Integer.parseInt(splitted[1]);//unicsast
				for(int i=0;i<connectedClientSockets.size();i++){
					if(connectedClientSockets.get(i).processID==process){
						ConnectedSocket c=connectedClientSockets.get(i);
						Thread t1 = new Thread(new Runnable() {//new thread to keep accepting connections				
							public void run() {
								write(message,c, 5000);//turn message into metadata, if statements for msend
							}
						});
						t1.start();
					}
				}
			}
		}
	}
	
	
	public void write(String message, ConnectedSocket c, int ms){
		try{
			String[] splitted = message.split(" ");
			long currentTime = System.currentTimeMillis();
			Time t = new Time(currentTime);
			int id = c.processID;
			Metadata d;
			if(splitted[0].equals("msend")){
				System.out.println("Sent \""+ splitted[1] + "\" to process" + id + ", system time is " + t.toString());
				int[] arr = new int[4];
				arr[0]=vt[0];
				arr[1]=vt[1];
				arr[2]=vt[2];
				arr[3]=vt[3];
				d = new Metadata(splitted[1],myID,arr);
			}else{
				System.out.println("Sent \""+ splitted[2] + "\" to process" + id + ", system time is " + t.toString());
				int[] arr = new int[4];
				arr[0]=vt[0];
				arr[1]=vt[1];
				arr[2]=vt[2];
				arr[3]=vt[3];
				d = new Metadata(splitted[2],myID,arr);
			}
			try {//either randomly delay or delay the sepcified amount
				if(ms!=0){
					Thread.sleep(ms);
				}else{
					int a = randomGenerator.nextInt(diff) + minSeconds;
					Thread.sleep(a);
				}
    		} catch(InterruptedException ex) {
    			Thread.currentThread().interrupt();
    		}
			Socket client = c.sock;
			OutputStream outToServer = client.getOutputStream();
			DataOutputStream out = new DataOutputStream(outToServer);
			out.writeUTF(d.toString());
			
		}catch(IOException e){
			throw new RuntimeException("Error accepting client connection", e);
		}
	}
	
	public void runnableWorker(int processID, ConnectedSocket clientSocket) throws ClassNotFoundException{//gets message
		try {
        	if(processID>myID){
        		connectBackServer(processID);
        	}
        	
        	while(run){
        		
        		checkArray();//checks array twice for preceding messages
        		checkArray();
        		InputStream inFromServer = clientSocket.sock.getInputStream();
                DataInputStream in = new DataInputStream(inFromServer);
                String g = in.readUTF();
                Metadata o = new Metadata(g);
                arr.add(o);
                checkArray();//gets this message and check array agin
               
                int count =0;
                
        	}
        	
        } catch (IOException e) {
            e.printStackTrace();
        }
	}
	
	public void checkArray(){//check array for timestamps and the ability deliver
		
		for(int i=0;i<arr.size();i++){
        	Metadata z = arr.get(i);
        	int id=z.ProcessId-1;//1, then 0
        	//printV(vt);
        	if(z.vector[id]==vt[id]+1){
        		if((0!=id && z.vector[0]!=vt[0]) || (1!=id && z.vector[1]!=vt[1]) || (2!=id && z.vector[2]!=vt[2]) || (3!=id && z.vector[3]!=vt[3])){
        		}else{
        			arr.remove(i);
        			long currentTime = System.currentTimeMillis();
        			Time t = new Time(currentTime);
        			vt=z.vector;
        			System.out.print("Received \""+ z.message + "\" from process" + z.ProcessId + ", system time is " + t.toString() +"\n" );
        		}
        	}
        }
	}


	public void printVector(){//print you vector timestamp
		System.out.println("<"+vt[0]+","+vt[1]+","+vt[2]+","+vt[3]+">");
	
	}
	public void printV(int[] arr){//print any vector timestamp
		System.out.println("<"+arr[0]+","+arr[1]+","+arr[2]+","+arr[3]+">");

	}
	
	public void connectBackServer(int processID){//if anyone connects to yu, connect back to them
		Socket client;
		
		try{
			 
			 client = new Socket(config[processID-1][1], Integer.parseInt(config[processID-1][2]));
			 OutputStream outToServer = (OutputStream) client.getOutputStream();
			 OutputStreamWriter out = new OutputStreamWriter(outToServer);
			 Integer mess=myID;
			 String message = mess.toString();
			 out.write(message + "\n");
			 out.flush();
			 ConnectedSocket d = new ConnectedSocket(client,processID);
			connectedClientSockets.add(d);
			
			
		/*	 out.close();
			outToServer.close();*/
			 
		}catch(IOException e){
			throw new RuntimeException("Error accepting client connection", e);
		}
		
		
	}
	
	
	public void waitForConnections(){//wait for connections from other processes
		while(true){
			Socket clientSocket = null;
			try {
			  clientSocket = this.serverSocket.accept();
			  int processNum=-1;
			  BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			  String temp = reader.readLine();
			  processNum = Integer.parseInt(temp);	
			  
			  final int num=processNum;
		
	          ConnectedSocket c = new ConnectedSocket(clientSocket,processNum);
	          Thread t1 = new Thread(new Runnable() {//new thread to keep accepting connections
	        	  public void run() {
	        		  //synchronized(arr){
	        		  	try {
	        		  		runnableWorker(num,c);
	        		  	} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
	        		  	}
	        		 // }
	        	  }
			  }); 
			  t1.start();
			  
			} catch (IOException e) {
				throw new RuntimeException("Error accepting client connection", e);
			}
			
		}
	}
	
	public int[] getNewVector(int[] arr1, int[] arr2){//get new vector for self
		int[] v = new int[4];
		for(int i=0;i<4;i++){
			if(i==myID-1){
				v[i]=Math.max(arr1[myID-1], arr2[myID-1])+1;
			}else{
				v[i]=Math.max(arr1[i], arr2[i]);
			}
		}
		vt=v;
		return v;
	}
	
	
		
	public void run() {
		// TODO Auto-generated method stub
		
	}
	
	public void openServerSocket(String ip, int port){//open ur server socket
		try {
            serverSocket = new ServerSocket(myPort);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open port", e);
        }
	}
	
	private class ConnectedSocket implements Serializable{//class to hold socket and id
		public transient Socket sock;
		public int processID; 
		public ConnectedSocket(Socket s, int process){
			sock=s;
			processID=process;
		}
	}
	
	
	private class Metadata implements java.io.Serializable{//metadat to send over connection
		public String message;
		public int ProcessId;
		public int [] vector = new int[4];
		public Metadata(String m, int id,int[] v){
			message = m;
			ProcessId=id;
			vector = v;
			
		}
		public Metadata(String input){//turn string into data
			String[] splitted = input.split(Pattern.quote("$"));
			message = splitted[0];
			ProcessId = Integer.parseInt(splitted[1]);
			vector[0]=Integer.parseInt(splitted[2]);
			vector[1]=Integer.parseInt(splitted[3]);
			vector[2]=Integer.parseInt(splitted[4]);
			vector[3]=Integer.parseInt(splitted[5]);
		}
		
		@Override
		public String toString(){//to string 
			String output = "";
			output = output + message;
			output = output + "$" + String.valueOf(ProcessId);
			output = output + "$" + String.valueOf(vector[0]);
			output = output + "$" + String.valueOf(vector[1]);
			output = output + "$" + String.valueOf(vector[2]);
			output = output + "$" + String.valueOf(vector[3]);
		//	System.out.println("output "+output);
			return output;
		}
	}
	
	public static void main(String[] args){//main method
		File file = new File(args[1]);//"/Users/kushmaheshwari/Documents/workspace/425MP1/src/Config"
		String[] split = null;
		int count =0;
		
		try{
			Scanner sc = new Scanner(file);//parse config file get specifications
			String delayLine = sc.nextLine();
			String[] splitted = delayLine.split(" ");
			minSeconds = Integer.parseInt(splitted[0]);
			maxSeconds = Integer.parseInt(splitted[1]);
			while (sc.hasNextLine()) {
				String line = sc.nextLine();
				split = line.split(" ");
				config[count][0] = split[0];
				config[count][1] = split[1];
				config[count][2] = split[2];
				count++;
	        }
		}catch(Exception ex){
			ex.printStackTrace();
		}
		
		int i = Integer.parseInt(args[0]);
		i=i-1;
		
		new Process(config[i][0],config[i][1],config[i][2]);//create new process
	}

}
