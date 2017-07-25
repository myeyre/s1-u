package com.asiainfo.sdtp;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SendDataToKafka extends Thread {
	private Producer<String, String> producer;
	public ArrayBlockingQueue<KeyedMessage<String, String>> msg_queue;
	private List<KeyedMessage<String, String>> msglist = new ArrayList<KeyedMessage<String, String>>();
	private long sendNum = 0L;
	private int send_size = 0;
	private int max_wait_time = 0;

	public SendDataToKafka(ThreadGroup tg) {
		super(tg,"ServerHandlerV4");
		try {
			this.msg_queue = new ArrayBlockingQueue<KeyedMessage<String, String>>(Integer.parseInt(Utils
			        .getProperties("send_queue_size")));
			this.send_size = Integer.parseInt(Utils.getProperties("send_size"));
			this.max_wait_time = Integer.parseInt(Utils.getProperties("max_wait_time"));
			InputStream is = this.getClass().getClassLoader().getResourceAsStream("producer.properties");
			Properties props = new Properties();
			// props.put("serializer.class", "kafka.serializer.DefaultEncoder");
			// props.put("key.serializer.class",
			// "kafka.serializer.StringEncoder");
			// props.put("metadata.broker.list",
			// "cloud1:9092, cloud22:9092, cloud3:9092");
			props.load(is);
			ProducerConfig config = new ProducerConfig(props);
			this.producer = new Producer<String, String>(config);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void run() {
		long lastSendTime = System.currentTimeMillis();
		while (true) {
			try {
				KeyedMessage<String, String> msg = this.msg_queue.poll( this.max_wait_time, TimeUnit.MILLISECONDS);
				if (msg == null)
					continue;
				msglist.add(msg);
				
				if (msglist.size() >= this.send_size || ((System.currentTimeMillis() - lastSendTime) >= this.max_wait_time && msglist.size() > 0)) {
					lastSendTime = System.currentTimeMillis();
					producer.send(msglist);
					this.sendNum += msglist.size();
					msglist.clear();
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	
	}

	public long getSendNum() {
    	return sendNum;
    }
	
	
	
}
