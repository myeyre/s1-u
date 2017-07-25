package com.asiainfo.sdtp;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Random;


public class ReceiveHandlerV4 extends Thread {
	private Socket socket = null;
	@SuppressWarnings("unused")
	private long receiveNum = 0L;
	private long receiveTotalNum = 0L;
	private long receiveXdrNum = 0L;
	private ProcessData[] nextstep;
	private ObjectInputStream dis =null;
	public ReceiveHandlerV4(ThreadGroup tg,Socket socket,ProcessData[] nextstep)throws IOException {
		super(tg,"ServerHandlerV4");
		this.socket = socket;
		this.nextstep=nextstep;
	}

	public void run() {
		byte totalContents=0;

		
		try {
			//this.socket.setSoLinger(true, 3);
			//融合程序
			ObjectInputStream dis = new ObjectInputStream(new BufferedInputStream(socket.getInputStream()));
			//DataInputStream dis = new DataInputStream(this.socket.getInputStream());
			//DataOutputStream out = new DataOutputStream(this.socket.getOutputStream());
			
			
			
			Random random = new Random(this.getId());
			while (true){
				nextstep[random.nextInt(nextstep.length)].msg_queue.offer((byte[]) dis.readObject());
				this.receiveNum++;
				this.receiveXdrNum++;
				this.receiveTotalNum +=totalContents & 0xff;
//				while(true){
//					if ()
//						break;
//				}
			}
//			do {
//				int readnum = 0;
//			    totalLength=dis.readUnsignedShort();
//
//				int length = totalLength - 2;
//				byte[] buffer = new byte[length];
//				readnum = 0;
//				while (readnum < length) {
//					int num = dis.read(buffer, readnum, length - readnum);
//					if (num > 0) {
//						readnum += num;
//					}
//				}
//				int off = 0;
//
//				messageType = ConvToByte.byteToShort(buffer, off);
//				off += 2;
//
//				sequenceId = ConvToByte.byteToInt(buffer, off);
//				off += 4;
//				totalContents = buffer[off++];
//				if (this.is_login && messageType == 5) {
//					//
//
//					this.receiveNum++;// sdtp���У����
//					this.receiveXdrNum++;// ��¼���յ�xdr��
//					this.receiveTotalNum +=totalContents & 0xff;
//					// ���Ӧ��
//					byte[] requestArray = responseNotifyEventData(sequenceId, totalContents);
//					out.write(requestArray);
//					//����ݷ������ѡ��Ĵ����̵߳Ķ����У������̶߳��������������ѡ���̣߳�ֱ���ųɹ�Ϊֹ
//					while(true){
//						if (nextstep[random.nextInt(nextstep.length)].msg_queue.offer(buffer))
//							break;
//					}
////					out.flush();
//					// ��¼��ݰ���Ϣ����
////					CountWriter.writepackage("sequenceId=" + sequenceId + ";totalContents=" + totalContents + ";\n");
//				} else if (messageType == 1) {// verNego �汾Э��
//					byte[] requestArray = responseVerNego(sequenceId, totalContents);
//					out.write(requestArray);
//					out.flush();
//				} else if (messageType == 2) {// linkAuth Ȩ����֤
//					byte[] loginId = new byte[12];
//					byte[] digestArray = new byte[16];
//					byte[] timestamp = new byte[4];
//					byte[] rand = new byte[2];
//					System.arraycopy(buffer, off, loginId, 0, 12);
//					off += 12;
//					System.arraycopy(buffer, off , digestArray, 0, 16);
//					off += 16;
//					System.arraycopy(buffer, off , timestamp, 0, 4);
//					off += 4;
//					System.arraycopy(buffer, off , rand, 0, 2);
//
//					byte[] requestArray = responseLinkAuth(sequenceId, totalContents, loginId, digestArray, timestamp,
//					        rand);
//					out.write(requestArray);
//					out.flush();
//
//					this.is_login = true;
//				} else if (this.is_login && messageType == 3) {// linkCheck ��·���
//					byte[] requestArray = responseLinkCheck(sequenceId, totalContents);
//					out.write(requestArray);
//					out.flush();
//				} else if (this.is_login && messageType == 7) {// linkDataCheck ��·��ݷ���У��
//					int  sendflag=ConvToByte.byteToInt(buffer, off);
//					off += 4;
//					int  sendDataInfo=ConvToByte.byteToInt(buffer, off);
//
//					byte[] requestArray = responseLinkDataCheck(sequenceId, totalContents,sendflag,sendDataInfo,(int)receiveNum);
//					this.receiveNum=0;
//					out.write(requestArray);
//					out.flush();
//				} else {
//					break;// �ͷ����ӣ�δ��֤Ȩ�޻���messageType����1��2��3, 7
//				}
//			} while (messageType != 4);// linkRel �����ͷ�
//			System.out.println("messageType=" + messageType);
//			byte[] requestArray = responseLinkRel(sequenceId, totalContents);
//			out.write(requestArray);
//			out.flush();
		} catch (Exception e) {
			//System.out.println("totalLength="+totalLength+";messageType="+messageType+";sequenceId="+sequenceId+";totalContents="+totalContents);
			e.printStackTrace();
			
		} finally {
			//logger.warn("socket client 已经断开！");
			try {
				if (dis != null){
					dis.close();
				}
				if (socket != null){
					socket.close();
				}
			} catch (IOException e) {
				e.getStackTrace();
			}
			//close();
		}
	}

	public byte[] responseVerNego(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8001;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
        System.out.println("VerNego :"+reslut);
		return requestArray;
	}

	public byte[] responseLinkAuth(int sequenceId, byte totalContents, byte[] loginId, byte[] digestArray,
	        byte[] timestamp, byte[] rand) {
		short totalLength = 74;
		int messageType = 0x8002;
		byte reslut = 0;
		String passwd = "asiainfo123";


		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		int pos = 0;
		try {
			// ��ȡMD5������
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			// ���������Ӧ��SHA256����ֵ
			byte[] sha_passwd = md.digest(passwd.getBytes());
			StringBuilder sb=new StringBuilder();
			for (byte b : sha_passwd)
				sb.append(String.format("%02x", b));
			// ƴ����ʱ�ֶΣ��Լ���digest���Ϳͻ��˷��͵�ֵ�ȶ�

			String str_tmp=(new String(loginId))+sb.toString()+ConvToByte.byteToInt(timestamp, 0)+"rand="+ConvToByte.byteToShort(rand, 0);

			// ����digest
			byte[] digest = md.digest(str_tmp.getBytes());
			// �ȶԼ������digest�Ϳͻ��˷��͹�����digest�Ƿ���ȣ���ȷ���1�����򷵻�0
			reslut = Arrays.equals(digest, digestArray) ? (byte) 1 : (byte) 0;
			
			sb.delete(0, sb.length());
			for (byte b : digestArray)
				sb.append(String.format("%02x", b));
			String receive=sb.toString();
			sb.delete(0, sb.length());
			for (byte b : digest)
				sb.append(String.format("%02x", b));
			System.out.println("LinkAuth :received digest="+receive+";our digest="+sb.toString());
			
			reslut=1;//������֤��������ݽ���
			System.out.println("LinkAuth :"+reslut);
		} catch (Exception e) {
			e.printStackTrace();
		}

		byte[] requestArray = new byte[totalLength];
		pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		pos++;
		System.arraycopy(digestArray, 0, requestArray, pos, digestArray.length);

		return requestArray;
	}

	public byte[] responseLinkCheck(int sequenceId, byte totalContents) {
		short totalLength = 9;
		int messageType = 0x8003;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		
		System.out.println("LinkCheck ");
		return requestArray;
	}

	public byte[] responseLinkRel(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8004;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;
		System.out.println("LinkRel:"+reslut);
		return requestArray;
	}

	public byte[] responseNotifyEventData(int sequenceId, byte totalContents) {
		short totalLength = 10;
		int messageType = 0x8005;
		byte reslut = 1;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);

		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		requestArray[pos] = reslut;

		return requestArray;
	}
	
	public byte[] responseLinkDataCheck(int sequenceId, byte totalContents,int sendflag,int sendDataInfo,int receiveDatainfo) {
		short totalLength = 22;
		int messageType = 0x8007;
		byte reslut=0 ;

		byte[] totalLengthArray = ConvToByte.shortToByte(totalLength);
		byte[] messageTypeArray = ConvToByte.intToByte(messageType);
		byte[] sequenceIdArray = ConvToByte.intToByte(sequenceId);
		byte[] sendflagArray = ConvToByte.intToByte(sendflag);
		byte[] sendDataInfoArray = ConvToByte.intToByte(sendDataInfo);
		byte[] recciveDatainfoArray = ConvToByte.intToByte(receiveDatainfo);
		
		if(sendDataInfo==receiveDatainfo){
			reslut=0;
		}else if(sendDataInfo>receiveDatainfo){
			reslut=1;
		}else if(sendDataInfo<receiveDatainfo){
			reslut=2;
		}
		
		byte[] requestArray = new byte[totalLength];
		int pos = 0;
		System.arraycopy(totalLengthArray, 0, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(messageTypeArray, 2, requestArray, pos, 2);
		pos += 2;
		System.arraycopy(sequenceIdArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = totalContents;
		pos++;
		System.arraycopy(sendflagArray, 0, requestArray, pos, 4);
		pos += 4;
		requestArray[pos] = reslut;
		pos++;
		System.arraycopy(sendDataInfoArray, 0, requestArray, pos, 4);
		pos += 4;
		System.arraycopy(recciveDatainfoArray, 0, requestArray, pos, 4);
		System.out.println("threadid:"+this.getId()+"; responseLinkDataCheck:reslut="+reslut+";sendDataInfo="+sendDataInfo+";recciveDatainfo="+receiveDatainfo);
		return requestArray;
	}


	public long getReceiveTotalNum() {
    	return receiveTotalNum;
    }

	public long getReceiveXdrNum() {
    	return receiveXdrNum;
    }

	@SuppressWarnings("unused")
	private void close() {
		try {
			//System.out.println("�ر����ӣ��ͻ��ˣ�" + this.socket.getInetAddress().getHostAddress());
			this.socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			//��ӡ��־
			CountWriter.addCount(Thread.currentThread().getId()+"", new CountEntity(this.receiveTotalNum,this.receiveXdrNum,0,null));
//			CountWriter.write(Thread.currentThread().getId()+"");
			if(Thread.activeCount()==1){
				CountWriter.writeCount();
//				CountWriter.close();
			}
		}
	}
}
