package com.asiainfo.sdtp;

import java.util.Calendar;
import java.util.HashMap;

public class WatchThread implements Runnable {
	private ThreadGroup tg = null;

	public WatchThread(ThreadGroup handler) {
		this.tg = handler;
	}

	public void run() {
		int log_print_type = Integer.parseInt(Utils.getProperties("log_print_type"));
		long last_time = System.currentTimeMillis();
		int log_print_interval = Integer.parseInt(Utils.getProperties("log_print_interval"));
		HashMap<String, Integer> timeunit = null;
		if (log_print_type == 2) {
			timeunit = new HashMap<String, Integer>();
			timeunit.put("minute", Integer.valueOf(Calendar.SECOND));
			timeunit.put("hour", Integer.valueOf(Calendar.MINUTE));
			timeunit.put("day", Integer.valueOf(Calendar.HOUR_OF_DAY));
		}

		while (true) {
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			// �ж��Ƿ񵽴�ָ��ʱ����
			if (log_print_type == 1) {
				if ((System.currentTimeMillis() - last_time) / 1000 < log_print_interval) {
					continue;
				}
			}
			// �ж��Ƿ������
			else if (log_print_type == 2) {
				Calendar datetime = Calendar.getInstance();
				if (datetime.get(timeunit.get(Utils.getProperties("log_print_time")).intValue()) == 0) {
					continue;
				}
			} else {
				continue;
			}
			Thread[] list = new Thread[tg.activeCount() * 2];
			int count = tg.enumerate(list);
			for (int i = 0; i < count; i++) {
				Thread thread = list[i];
				if (thread instanceof ReceiveHandlerV4) {
					ReceiveHandlerV4 td = (ReceiveHandlerV4) thread;
					CountWriter.addCount( td.getId() + "", new CountEntity(td.getReceiveTotalNum(), td.getReceiveXdrNum(), 0, null));
				}
				if (thread instanceof ProcessData) {
					ProcessData td = (ProcessData) thread;
					CountWriter.addCount( td.getId() + "", new CountEntity(0, 0, 0, td.getSendTopicNum()));
				}
				if (thread instanceof SendDataToKafka) {
					SendDataToKafka td = (SendDataToKafka) thread;
					CountWriter.addCount( td.getId() + "", new CountEntity(0, 0, td.getSendNum(),null));
				}
				CountWriter.write(thread.getName()+":"+thread.getId());
			}
			
			CountWriter.writeCount();
			last_time = System.currentTimeMillis();
		}
	}
}
