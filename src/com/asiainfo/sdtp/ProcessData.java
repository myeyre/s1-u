package com.asiainfo.sdtp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;

import kafka.producer.KeyedMessage;

import org.apache.commons.lang.StringUtils;

public class ProcessData extends Thread {
	private String interface_type = "";
	private HashMap<String, Long> sendTopicNum = new HashMap<String, Long>();
	private SendDataToKafka[] nextstep;
	private String kafka_date_symbol ;
	public ArrayBlockingQueue<byte[]> msg_queue;
	StringBuilder sb ;
	public ProcessData(ThreadGroup tg, SendDataToKafka[] nextstep)
			throws IOException {
		super(tg, "ServerHandlerV4");
		this.nextstep = nextstep;
		this.msg_queue = new ArrayBlockingQueue<byte[]>(Integer.parseInt(Utils
				.getProperties("process_queue_size")));
		this.interface_type = Utils.getProperties("interface_type");
		this.kafka_date_symbol = Utils.getProperties("kafka_date_symbol");
	}

	public void run() {
		short messageType = 0;
		int sequenceId = 0;
		byte totalContents = 0;
		int totalLength = 0;
		HashMap<String, String> topicMap = new HashMap<String, String>();
		Random random = new Random(this.getId());
		// topic map
		topicMap.put("100", "topic_lte_common");
		topicMap.put("101", "topic_lte_dns");
		topicMap.put("102", "topic_lte_mms");
		topicMap.put("103", "topic_lte_http");
		topicMap.put("104", "topic_lte_ftp");
		topicMap.put("105", "topic_lte_email");
		topicMap.put("106", "topic_lte_voip");
		topicMap.put("107", "topic_lte_rtsp");
		topicMap.put("108", "topic_lte_p2p");
		topicMap.put("109", "topic_lte_video");
		topicMap.put("110", "topic_lte_im");
		topicMap.put("0", "topic_lte_p2p_gn_s11");
		topicMap.put("1", "topic_lte_p2p_gn_s11");

		try {
			while (true) {
				int off = 0;
				byte[] buffer = this.msg_queue.take();
				totalLength = buffer.length + 2;

				messageType = ConvToByte.byteToShort(buffer, off);
				off += 2;

				sequenceId = ConvToByte.byteToInt(buffer, off);
				off += 4;
				totalContents = buffer[off++];

				String topicName = "";

				// s1-u
				if ("s1_u".equals(this.interface_type)) {
					
					for (int i = 0; i < (totalContents & 0xff); i++) {
						sb = new StringBuilder();
						// Length unsigned int 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// City byte 2
						sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2,true)).append(kafka_date_symbol);
						off += 2;
						// Interface unsigned int 1
						sb.append(buffer[off]).append(kafka_date_symbol);
						off += 1;
						// xDR ID unsigned int 16
						sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append(kafka_date_symbol);
						off += 16;
						// RAT unsigned int 1
						sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append(kafka_date_symbol);
						off += 1;
						// IMSI byte 8
						String imsi = ConvToByte.decodeTBCD(buffer, off,off + 8, true);
						sb.append(imsi).append(kafka_date_symbol);
						off += 8;
						// IMEI byte 8
						sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8,true)).append(kafka_date_symbol);
						off += 8;
						// MSISDN byte 16
						sb.append(StringUtils.replaceChars(ConvToByte.decodeTBCD(buffer, off, off + 16,false),"f", "")).append(kafka_date_symbol);
						off += 16;
						// Machine IP Add type Unsigned int 1
						int ipType = (buffer[off] & 0xff);
						sb.append(ipType).append(kafka_date_symbol);
						off += 1;
						// SGW/GGSN IP Add byte 4月16日
						if (ipType == 1) {
							sb.append(ConvToByte.getIpv4(buffer, off)).append(kafka_date_symbol);// IPv4
							off += 4;
						} else if (ipType == 2) {
							sb.append(ConvToByte.getIpv6(buffer, off)).append(kafka_date_symbol);// IPv6
							off += 16;
						}
						// eNB/SGSN IP Add byte 4月16日
						if (ipType == 1) {
							sb.append(ConvToByte.getIpv4(buffer, off)).append(kafka_date_symbol);// IPv4
							off += 4;
						} else if (ipType == 2) {
							sb.append(ConvToByte.getIpv6(buffer, off)).append(kafka_date_symbol);// IPv6
							off += 16;
						}
						// SGW/GGSN Port byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// eNB/SGSN Port byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// eNB/SGSN GTP-TEID byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// SGW/GGSN GTP-TEID unsigned byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// TAC byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// Cell ID Byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// APN String 32
						sb.append(new String(buffer, off, 32).trim()).append(kafka_date_symbol);
						off += 32;
						// App Type Code byte 1
						int appTypeCode = (buffer[off] & 0xff);
						sb.append(appTypeCode).append(kafka_date_symbol);
						off += 1;
						// Procedure Start Time dateTime 8
						sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
						off += 8;
						// Procedure End Time dateTime 8
						sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
						off += 8;
						// Protocol Type byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// App Type byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// App Sub-type byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// App Content byte 1
						sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
						off += 1;
						// App Status byte 1
						sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
						off += 1;
						// USER_IPv4 byte 4
						sb.append(ConvToByte.getIpv4(buffer, off)).append(kafka_date_symbol);// IPv4
						off += 4;
						// USER_IPv6 byte 16
						sb.append(ConvToByte.getIpv6(buffer, off)).append(kafka_date_symbol);// IPv6
						off += 16;
						// User Port byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// L4 protocal byte 1
						sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
						off += 1;
						// App Server IP_IPv4 byte 4
						sb.append(ConvToByte.getIpv4(buffer, off)).append(kafka_date_symbol);// IPv4
						off += 4;
						// App Server IP_IPv6 byte 16
						sb.append(ConvToByte.getIpv6(buffer, off)).append(kafka_date_symbol);// IPv6
						off += 16;
						// App Server Port byte 2
						sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
						off += 2;
						// UL Data byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// DL Data byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// UL IP Packet byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// DL IP Packet byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// 上行TCP乱序报文数 byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// 下行TCP乱序报文数 byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// 上行TCP重传报文数 byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// 下行TCP重传报文数 byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// TCP建链响应时延（ms） byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// TCP建链确认时延（ms） byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// UL_IP_FRAG_PACKETS byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// DL_IP_FRAG_PACKETS byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// TCP建链成功到第一条事务请求的时延（ms） byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// 第一条事务请求到其第一个响应包时延（ms） byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// 窗口大小 byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// MSS大小 byte 4
						sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
						off += 4;
						// TCP建链尝试次数 byte 1
						sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
						off += 1;
						// TCP连接状态指示 byte 1
						sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
						off += 1;
						// 会话是否结束标志 byte 1
						sb.append((buffer[off] & 0xff));
						off += 1;
						topicName = topicMap.get(String.valueOf(appTypeCode));
						KeyedMessage<String, String> km = null;

						switch (appTypeCode) {
						case 100:
							km = CommonDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 101:
							km = DnsDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 102:
							km = MMSDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 103:
							km = HTTPDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 104:
							km = FTPDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 105:
							km = EmailDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 106:
							km = VoipDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 107:
							km = RtspDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 108:
							km = P2pDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 109:
							km = VideoDataHandler(buffer, off, topicName, sb,imsi);
							break;
						case 110:
							km = ImDataHandler(buffer, off, topicName, sb, imsi);
							break;
//						default:
//							sb.delete(0, sb.length());
//							sb.append("total_length=" + totalLength);
//							sb.append("sequenceId=" + sequenceId);
//							sb.append("totalContents=" + totalContents);
//							sb.append("body=");
//							sb.append(getOtherMessageCommon(buffer, off));
//							sb.append("\n");
//							CountWriter.writeerror(sb.toString());
//							break;
						}

						if (topicName != null) {
							if(null != km){
								nextstep[random.nextInt(nextstep.length)].msg_queue.put(km);
								long num = sendTopicNum.get(topicName) == null ? 0: sendTopicNum.get(topicName);
								sendTopicNum.put(topicName, num + 1);
							}
						}

					}
					// s1-mme
				} else if ("s1_mme".equals(this.interface_type)) {

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

		}
	}

	private KeyedMessage<String, String> CommonDataHandler(byte[] buffer,int off, String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
//		sb.append(new Date());
		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());
		return km;
	}

	private KeyedMessage<String, String> DnsDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// 请求查询的DNS域名 V 64
		sb.append(ConvToByte.charToString(buffer, off, off + 64)).append(kafka_date_symbol);
		off += 64;
		// 查询结果IP char 15
		sb.append(ConvToByte.charToString(buffer, off, off + 15)).append(kafka_date_symbol);
		off += 15;
		// DNS响应码 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// DNS的请求次数 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 响应数目 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 授权内容数目 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 附加内容数目 byte 1
		sb.append((buffer[off] & 0xff));
		off += 1;
//		 sb.append(new Date());
		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> MMSDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// TRANS_TYPE byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// SUCCESS_FLAG byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// HTTP_OR_WAP1.X byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// HTTP_WAP_CODE byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// MMSE_RSP_STATUS byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// MMS_SEND_ADDR char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// MMS_MSG_ID char 64
		sb.append(ConvToByte.charToString(buffer, off, off + 64)).append(kafka_date_symbol);
		off += 64;
		// MMS_TRANSACTION_ID char 32
		sb.append(ConvToByte.charToString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// MMS_RETRIVE_ADDR char 256
		sb.append(ConvToByte.charToString(buffer, off, off + 256)).append(kafka_date_symbol);
		off += 256;
		// MMS_RETRIVE_ADDR_NUM byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// MMS_CC_BCC_ADDR char 256
		sb.append(ConvToByte.charToString(buffer, off, off + 256)).append(kafka_date_symbol);
		off += 256;
		// MMS_CC_BCC_ADDR_NUM byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// MMS_SUBJECT char 256
		sb.append(ConvToByte.charToString(buffer, off, off + 256)).append(kafka_date_symbol);
		off += 256;
		// MMS_DATA_SIZE byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// MMSC地址 char 64
		sb.append(ConvToByte.charToString(buffer, off, off + 64)).append(kafka_date_symbol);
		off += 64;
		// HOST char 64
		sb.append(ConvToByte.charToString(buffer, off, off + 64)).append(kafka_date_symbol);
		off += 64;
		// URI char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// X-Online-Host char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128));
		off += 128;
//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> HTTPDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// HTTP版本 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 事务类型 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// HTTP/WAP事务状态 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// 第一个HTTP响应包时延(MS) byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// 最后一个HTTP内容包的时延(MS) byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// 最后一个ACK确认包的时延（ms） byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// HOST char 64
		sb.append(ConvToByte.charToString(buffer, off, off + 64).trim()).append(kafka_date_symbol);
		off += 64;
		// URI char 512
		sb.append(StringUtils.replaceChars(ConvToByte.charToString(buffer, off, off + 512),"|","u007C").trim()).append(kafka_date_symbol);
		off += 512;
		// X-Online-Host char 128
		sb.append(StringUtils.replaceChars(ConvToByte.charToString(buffer, off, off + 128),"ffffffff","").trim()).append(kafka_date_symbol);
		off += 128;
		// User-Agent char 256
		sb.append(ConvToByte.charToString(buffer, off, off + 256).trim()).append(kafka_date_symbol);
		off += 256;
		// HTTP_content_type char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128).trim()).append(kafka_date_symbol);
		off += 128;
		// refer_URI char 128
		sb.append(StringUtils.replaceChars(ConvToByte.charToString(buffer, off, off + 128),"ffffffff","")).append(kafka_date_symbol);
		off += 128;
		// Cookie char 256
		sb.append(StringUtils.replaceChars(ConvToByte.charToString(buffer, off, off + 256),"ffffffff","")).append(kafka_date_symbol);
		off += 256;
		// Content-Length byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// 目标行为 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// Wtp中断类型 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// wtp中断原因 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// title char 256
		sb.append(ConvToByte.charToString(buffer, off, off + 256).trim()).append(kafka_date_symbol);
		off += 256;
		// key word char 256
		sb.append(StringUtils.replaceChars(ConvToByte.charToString(buffer, off, off + 256),"ffffffff","")).append(kafka_date_symbol);
		off += 256;
		// 业务行为标识 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 业务完成标识 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 业务时延(ms) byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// 浏览工具 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 门户应用集合 byte 1
		sb.append((buffer[off] & 0xff));
		off += 1;
//		sb.append(new Date());
		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> FTPDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// FTP状态 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 登录用户名 char 32
		sb.append(ConvToByte.charToString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 当前目录 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// 文件传输模式 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 传输方向标志 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 文件名称 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// FTP会话中本地数据端口 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// FTP会话中远端数据端口 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// 文件总大小 byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// 响应时延（ms） byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// 传输时长（ms） byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off));
		off += 4;
//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> EmailDataHandler(byte[] buffer,int off, String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// 事务类型 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// 返回状态码 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// 用户名 char 32
		sb.append(ConvToByte.charToString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 发送方信息 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// Email长度 byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// SMTP域名 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// 邮件接收方账号 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// 要获取的邮件头信息 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// 接入方式 char 1
		sb.append(buffer[off]);
		off += 1;
//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> VoipDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// 呼叫方向 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 主叫号码 char 32
		sb.append(ConvToByte.charToString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 被叫号码 char 32
		sb.append(ConvToByte.charToString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 呼叫类型 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// VOIP中的数据流数量 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// 挂机原因 byte 1
		sb.append((buffer[off] & 0xff)).append(kafka_date_symbol);
		off += 1;
		// 信令协议类型 byte 1
		sb.append((buffer[off] & 0xff));
		off += 1;
//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> RtspDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// RTSP请求中的URL信息 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// User-Agent字段 char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// 用户访问的目标RTP服务器IP char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128)).append(kafka_date_symbol);
		off += 128;
		// RTP会话中客户端的起始端口 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// RTP会话中客户端的结束端口 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// RTP会话中服务器的起始端口 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// RTP会话中服务器的结束端口 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// RTSP session中的Video流数 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// RTSP session中的Audio流数 byte 2
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);
		off += 2;
		// 响应时延 byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off));
		off += 4;
//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	public KeyedMessage<String, String> ImDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		sb.append(kafka_date_symbol);
		// 登录账号 char 32
		sb.append(ConvToByte.getHexString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 软件版本 char 32
		sb.append(ConvToByte.getHexString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 客户端类型 char 32
		sb.append(ConvToByte.getHexString(buffer, off, off + 32)).append(kafka_date_symbol);
		off += 32;
		// 操作类型 char 1
		sb.append(buffer[off]);
		off += 1;

//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}
	
	public KeyedMessage<String, String> P2pDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		
		sb.append(kafka_date_symbol);
		// 文件大小 byte 4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 4;
		// P2P标示 byte 16
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append(kafka_date_symbol);
		off += 16;
		// Tracker char 128
		sb.append(ConvToByte.charToString(buffer, off, off + 128));
		off += 128;

//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}
	
	public KeyedMessage<String, String> VideoDataHandler(byte[] buffer, int off,String topicName, StringBuilder sb, String imsi) {
		KeyedMessage<String, String> km = null;
		
		sb.append(kafka_date_symbol);
		//		Request-Time		8
		sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
		off += 8;
		//		Resource-Name	Char	256
		sb.append(ConvToByte.charToString(buffer, off, off + 256)).append(kafka_date_symbol);
		off += 256;
		//		Streaming-Rate	byte	4
		sb.append(ConvToByte.byteToUnsignedInt(buffer, off)).append(kafka_date_symbol);
		off += 256;
		//		Play-Success	Char	1
		sb.append(buffer[off]).append(kafka_date_symbol);
		off += 1;
		//		First-Play-Time	dateTime	8
		sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
		off += 8;
		//		Pause-Num	Char	1
		sb.append(buffer[off]).append(kafka_date_symbol);
		off += 1;
		//		InitbufferDuration	uint64	8
		sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
		off += 8;
		//		VideoDownOctets	uint64	8
		sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
		off += 8;
		//		VideoDownTime	uint64	8
		sb.append(ConvToByte.byteToLong(buffer, off)).append(kafka_date_symbol);
		off += 8;
		//		FileType	Char	5
		sb.append(ConvToByte.charToString(buffer, off, off + 5));
		off += 5;
//		sb.append(new Date());

		km = new KeyedMessage<String, String>(topicName, imsi, sb.toString());

		return km;
	}

	@SuppressWarnings("unused")
	private List<KeyedMessage<String, String>> normallDataHandler(String topic,byte[] buffer, int off, String date, String time_stamp,
			SimpleDateFormat sdf) {
		String[] messages = new String(buffer, off, buffer.length - off)
				.split("\\r\\n");
		List<KeyedMessage<String, String>> list = new ArrayList<KeyedMessage<String, String>>();
		for (String message : messages) {
			if ("0".equals(time_stamp)) {
				message += "|" + date;
			}
			KeyedMessage<String, String> km = new KeyedMessage<String, String>(
					topic, message);
			list.add(km);
			long num = sendTopicNum.get(topic) == null ? 0 : sendTopicNum
					.get(topic);
			sendTopicNum.put(topic, num + 1);
		}
		return list;
	}

	private String getOtherMessageCommon(byte[] buffer, int off) {
		StringBuilder sb = new StringBuilder();
		sb.append(ConvToByte.byteToUnsignedShort(buffer, off)).append(kafka_date_symbol);// Length
																			// unsigned
																			// int
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true))
				.append(kafka_date_symbol);// 2 Local Province
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true))
				.append(kafka_date_symbol);// 3 Local City
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true))
				.append(kafka_date_symbol);// 4 Owner Province
		off += 2;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 2, true))
				.append(kafka_date_symbol);// 5 Owner City
		off += 2;
		sb.append(buffer[off]);// 6 Roaming Type
		off += 1;
		sb.append(buffer[off]).append(kafka_date_symbol);// Interface unsigned int
		off += 1;
		sb.append(ConvToByte.getHexString(buffer, off, off + 16)).append(kafka_date_symbol);// XDR
																				// ID
		off += 16;
		sb.append(ConvToByte.byteToUnsignedByte(buffer, off)).append(kafka_date_symbol);// RAT
																			// unsigned
																			// int
		off += 1;
		String imsi = ConvToByte.decodeTBCD(buffer, off, off + 8, true);
		sb.append(imsi).append(kafka_date_symbol);// IMSI byte
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 8, true))
				.append(kafka_date_symbol);// IMEI byte
		off += 8;
		sb.append(ConvToByte.decodeTBCD(buffer, off, off + 16, false)).append(
				"|");// MSISDN byte
		off += 16;
		return sb.toString();
	}

	public HashMap<String, Long> getSendTopicNum() {
		return sendTopicNum;
	}
}
