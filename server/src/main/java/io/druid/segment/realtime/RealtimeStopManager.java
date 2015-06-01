package io.druid.segment.realtime;

import io.druid.segment.realtime.plumber.RealtimePlumberLogger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.metamx.common.logger.Logger;

public class RealtimeStopManager {

	private static final Logger log = new Logger(RealtimeStopManager.class);
	private static final int PORT = 9991;
	private volatile static boolean stopFlag = false;

	private volatile static boolean isStopFinished = false;
	private static PrintWriter socketWriter = null;

	private static final Map<String, RealtimeManager.FireChief> fireChiefMap = new ConcurrentHashMap<>();
	private static final Map<String, Boolean> fireChiefAlreadyStopMap = new ConcurrentHashMap<>();
	
	public static Map<String, Boolean> getFireChiefAlreadyStopMap() {
		return fireChiefAlreadyStopMap;
	}

	public static Map<String, RealtimeManager.FireChief> getFireChiefMap() {
		return fireChiefMap;
	}

	public static boolean isSetStopFlag() {
		return stopFlag;
	}

	public static void setStopFlag(boolean isStop) {
		RealtimeStopManager.stopFlag = isStop;
	}

	public static void startListener() {
		try {
			final ServerSocket server = new ServerSocket(PORT);
			new Thread("real time stop listener thread") {
				public void run() {
					while (true) {
						try {
							Socket sk = server.accept();
							socketWriter = new PrintWriter(sk.getOutputStream());

							BufferedReader rdr = new BufferedReader(
									new InputStreamReader(sk.getInputStream()));
							String inMsg = rdr.readLine();
							
							if("stop real time".equals(inMsg)){
								
								if(stopFlag == true){
									sendToClient("already set the stop flag, please wait");
									closeClient();
									return;
								}
								
								setStopFlag(true);
								
								sendToClient("begin stop the real time");
								sendToClient("wait for 1 second");
								
								// stop 3 秒
								try {
									Thread.sleep(1000);
								} catch (Exception e) {
									log.error(e,
											"real time stop manager thread error occured");
								}
								
								for(Map.Entry<String, RealtimeManager.FireChief> entry : fireChiefMap.entrySet()){
									sendToClient(String.format("data source %s try to stop it", entry.getKey()));
									fireChiefMap.get(entry.getKey()).persistentForSafeClose();
								}

								setStopFinished();
								while(!isStopFinished){
									try {
										sendToClient("not finished, wait 3 seconds");
										Thread.sleep(3000);
									} catch (Exception e) {
										String msg = "real time stop manager thread error occured: " + e;
										log.error(msg);
										sendToClient(msg);
									}
									setStopFinished();
								}
							}else{
								sendToClient("wrong message, muste be 'stop real time'");
							}
							
							RealtimePlumberLogger.closeAll();

							sendToClient("will invoke system.exit, please use ps -ef to check the java process later");
							sendToClient("bye");
							closeClient();
							System.exit(0);
						} catch (Exception e) {
							log.error(e,
									"real time stop manager thread error occured");
							sendToClient("error occured " + e + " please see error log for detail");
							closeClient();
						}
					}
				};
			}.start();
		} catch (Exception e) {
			log.error(e, "start real time stop manager socket failed");
		}
	}
	
	private static void setStopFinished(){
		boolean hasFalse = false;
		for(Map.Entry<String, Boolean> entry : fireChiefAlreadyStopMap.entrySet()){
			if(!entry.getValue()){
				sendToClient("data source " + entry.getKey() + " is still not finished");
				hasFalse = true;
				break;
			}
		}
		isStopFinished = !hasFalse;
	}
	
	public static void sendToClient(String msg){
		if(null != socketWriter){
			socketWriter.print(msg);
			socketWriter.print("\r\n");
			socketWriter.flush();
		}
	}
	
	public static void closeClient(){
		if(null != socketWriter){
			try{
				socketWriter.close();
			}catch(Exception e){
			}
			socketWriter = null;
		}
	}
	
	public static void main(String[] args) {
		startListener();
	}
}
