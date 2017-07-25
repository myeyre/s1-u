package com.asiainfo.sdtp;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {
	private ServerSocket serverSocket = null;
	private String interface_type = "";
	public Server() {

	}

	public void service() throws IOException{
		this.serverSocket = new ServerSocket(Integer.parseInt(Utils.getProperties("server_port")));
		ThreadGroup tg = new ThreadGroup("sokect-lte");
		// 锟斤拷锟斤拷锟斤拷锟斤拷锟竭程ｏ拷锟斤拷要锟斤拷锟斤拷写锟斤拷志
		WatchThread thread = new WatchThread(tg);
		new Thread(thread).start();
		
		//锟斤拷锟斤拷转锟斤拷锟竭程ｏ拷准锟斤拷锟斤拷锟酵达拷锟斤拷锟竭筹拷转锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷莸锟絢afka
		int send_num=Integer.parseInt(Utils.getProperties("sendtoKafka_thread_num"));
		SendDataToKafka[] sendThreads=new SendDataToKafka[send_num];
		for(int i=0;i<send_num;i++){
			sendThreads[i]=new SendDataToKafka(tg);
			sendThreads[i].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler());
			sendThreads[i].start();
		}
		
		//锟斤拷锟斤拷锟斤拷锟斤拷锟竭程ｏ拷准锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷叱锟阶拷锟斤拷锟斤拷锟斤拷锟斤拷锟斤拷
		int process_num=Integer.parseInt(Utils.getProperties("process_thread_num"));
		ProcessData[] process=new ProcessData[process_num];
		for(int i=0;i<process_num;i++){
			process[i]=new ProcessData(tg,sendThreads);
			process[i].setUncaughtExceptionHandler(new MyUncaughtExceptionHandler());
			process[i].start();
		}

		// 锟斤拷锟斤拷丝冢锟斤拷锟斤拷锟斤拷泳锟斤拷驴锟揭伙拷锟斤拷锟斤拷锟斤拷叱檀锟斤拷?然锟斤拷锟斤拷锟斤拷锟斤拷
		while (true) {
			try {
				System.out.println("listening...");
				Socket sk = serverSocket.accept();
				ReceiveHandlerV4 handler = new ReceiveHandlerV4(tg, sk,process);
				handler.start();
				System.out.println("start a new Thread to procces client's request!");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private String checkConfig() {
		// 锟斤拷锟接匡拷锟斤拷锟斤拷
		interface_type = Utils.getProperties("interface_type");
		if (!"s1_u".equals(interface_type) && !"s1_mme".equals(interface_type)) {
			return "config error! interface_type must s1_u or s1_mme!";
		}
		// 锟斤拷锟斤拷锟斤拷丝锟�
		try {
			Integer.parseInt(Utils.getProperties("server_port"));
		} catch (Exception e) {
			return "config error! server_port mast be a integer!";
		}

		// 锟斤拷锟矫匡拷畏锟斤拷偷锟絢afka锟斤拷锟斤拷息锟斤拷锟斤拷
		try {
			Integer.parseInt(Utils.getProperties("send_size"));
		} catch (Exception e) {
			return "config error! send_size mast be a integer!";
		}

		try {
			Integer.parseInt(Utils.getProperties("max_wait_time"));
		} catch (Exception e) {
			return "config error! max_wait_time mast be a integer!";
		}

		try {
			Integer.parseInt(Utils.getProperties("process_queue_size"));
		} catch (Exception e) {
			return "config error! process_queue_size mast be a integer!";
		}

		try {
			Integer.parseInt(Utils.getProperties("send_queue_size"));
		} catch (Exception e) {
			return "config error! send_queue_size mast be a integer!";
		}

		try {
			Integer.parseInt(Utils.getProperties("process_thread_num"));
		} catch (Exception e) {
			return "config error! process_thread_num mast be a integer!";
		}
		try {
			Integer.parseInt(Utils.getProperties("sendtoKafka_thread_num"));
		} catch (Exception e) {
			return "config error! sendtoKafka_thread_num mast be a integer!";
		}
		
		try {
			int tmp=Integer.parseInt(Utils.getProperties("log_print_type"));
			if(tmp!=1&& tmp!=2)return "config error! log_print_type mast be 1 or 2";
		} catch (Exception e) {
			return "config error! log_print_type mast be 1 or 2";
		}
		
		try {
			Integer.parseInt(Utils.getProperties("log_print_interval"));
		} catch (Exception e) {
			return "config error! log_print_interval mast be a integer!";
		}
		
		String log_print_time = Utils.getProperties("log_print_time");
		if (!"minute".equals(log_print_time) && !"hour".equals(log_print_time)&& !"day".equals(log_print_time)) {
			return "config error! log_print_time must minute, hour or day!";
		}

		return "";
	}

	public static void main(String[] args) throws IOException {

		Server server = new Server();
		String errorMsg = server.checkConfig();
		if (!"".equals(errorMsg)) {
			System.out.println(errorMsg);
			System.exit(-1);
		}
		server.service();
	}
}
