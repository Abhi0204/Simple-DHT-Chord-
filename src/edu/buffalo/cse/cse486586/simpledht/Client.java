package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import edu.buffalo.cse.cse486586.simpledht.Message;

import android.content.ContentValues;
import android.provider.ContactsContract.Contacts.Data;
import android.util.Log;

public class Client extends Thread {

	String message;
	String port;
	String function;
	ObjectOutputStream obj;
	public Client(String current_port,String port,String function)
	{
		this.message=current_port;
		this.port=port;
		this.function=function;
	


	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Message m=new Message();

		Socket socket=null;
		try {
			socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),Integer.parseInt(port)*2);
		Log.v("Port",port+ "Message"+message +"Funtion"+function);
			obj=new ObjectOutputStream(socket.getOutputStream());

		if(socket!=null){
			
			if(function.contains("$JOIN$"))
			{

				Log.v("Inside Join",message);
				m.setKey("$Join$");
				m.setValue(message);
				
			
			}
			else if(function.contains("$Update$"))
			{

				m.setKey("$Update$");
				m.setValue(message);
			
			}
			else if(function.contains("$SetSucc$"))
			{

				m.setKey("$SetSucc$");
				m.setValue(message);
			
			}
			else if(function.contains("$SetPred$"))
			{

				m.setKey("$SetPred$");
				m.setValue(message);
			
			}
			else if(function.contains("$InsertKeySucc$"))
			{
				Log.v("message sent",message);
			    m.setKey("$InsertKeySucc$"+"|"+port);
				m.setValue(message);
	
			
			}
			else if(function.contains("$QuerySucc$"))
			{
				Log.v("message sent",message);
			    m.setKey("$QuerySucc$");
			    m.setValue(message);
			
			}
			else if(function.contains("$QueryResponse$"))
			{
				Log.v("message sent",message);
			    m.setKey("$QueryResponse$");
			    m.setValue(message);
			
			}
			else if(function.contains("$Gdump$"))
			{
				Log.v("message sent",message);
			    m.setKey("$Gdump$");
			    m.setValue(message);
			
			}
			else if(function.contains("Delete"))
			{
				Log.v("message sent",message);
			    m.setKey("Delete");
			    m.setValue(message);
			
			}
		
			obj.reset();		
			obj.writeObject(m);
			obj.close();
			socket.close();
		
		}

	
			
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		/*
		 * TODO: Fill in your client code that sends out a message.
		 */

	}



}
