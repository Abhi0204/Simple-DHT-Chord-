package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MatrixCursor.RowBuilder;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.text.GetChars;
import android.text.Selection;
import android.util.Log;
import android.widget.SlidingDrawer;

public class SimpleDhtProvider extends ContentProvider {

	static SQLiteDatabase sqLite_db;
	public static MyDbHelper dbHelper;
	public static String port_Str=null;
	public String base_port="5554";
	public static String ip_addr="10.0.2.2";
	public static String node_ID=null;
	public static int myPort=0;
	public static MyDbHelper Db;
	public static Uri mUri;
	ContentResolver myContentResolver;
	public static String startAvd="5554";
	public static String predecessor="";
	public static String successor="";

	Socket sock=null;
	public  static String db_key;
	public static String db_value;
	public static boolean largest;
	public static boolean smallest;
	public static String sflag="";
	public static String fflag="";
    static final int SERVER_PORT = 10000;
    public static boolean flag=false;
    public static boolean flag1=false;




	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		if(selection.equals("*"))
		{
			sqLite_db.delete(MyDbHelper.tableName, null, null);

			

			new Thread(new Client("*", successor, "Delete")).start();	

		}
		else if(selection.equalsIgnoreCase("@"))
		{
             sqLite_db.delete(MyDbHelper.tableName,null,null );

             
			
			}
		else
		{

			int no=sqLite_db.delete(MyDbHelper.tableName,"key=?",new String[]{ selection});
            if(no==0)
            {
    			new Thread(new Client(selection, successor, "Delete")).start();	

            }
        
		
		}
	
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub


		db_key = (String) values.get(MyDbHelper.col_key);
		db_value = (String) values.get(MyDbHelper.col_value);
		Message m=new Message();
		m.setKey(db_key);
		m.setValue(db_value);


		try {

			if(successor.equals("")){
				sqLite_db=dbHelper.getWritableDatabase();
			
				sqLite_db.insertWithOnConflict(MyDbHelper.tableName, null, values,SQLiteDatabase.CONFLICT_REPLACE);
				Log.v("insert without succ", values.toString());
				return uri;
			}
			else{
				if(genHash(port_Str).compareTo(genHash(predecessor))<0)
				{
					if((genHash(db_key).compareTo(genHash(port_Str))<=0)||(genHash(db_key).compareTo(genHash(predecessor))>0))
					{

						sqLite_db=dbHelper.getWritableDatabase();
						sqLite_db.insertWithOnConflict(MyDbHelper.tableName, null, values,SQLiteDatabase.CONFLICT_REPLACE);

						return uri;

					}

					else
					{
                         Log.v("Sending message to",successor);
                         Log.v("Sending from ", port_Str);
						new Thread(new Client(m.getKey()+"|"+m.getValue(),successor, "$InsertKeySucc$")).start();

					}
				}
				else
				{
					if((genHash(db_key).compareTo(genHash(port_Str)) <= 0)&&(genHash(db_key).compareTo(genHash(predecessor))>0))
					{

						sqLite_db=dbHelper.getWritableDatabase();
						sqLite_db.insertWithOnConflict(MyDbHelper.tableName, null, values,SQLiteDatabase.CONFLICT_REPLACE);
						Log.v("insert", values.toString());
						return uri;		
					}
					else 
					{
						Log.v("Sending message to",successor);
                        Log.v("Sending from ", port_Str);
						new Thread(new Client(m.getKey()+"|"+m.getValue(), successor, "$InsertKeySucc$")).start();	
					}
				}
			}

		}
		catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//getContext().getContentResolver().notifyChange(uri, null);
		return uri;
	}

	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}
	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub


		dbHelper = new MyDbHelper(getContext());
		dbHelper.getWritableDatabase();
		mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

		TelephonyManager tel =(TelephonyManager)getContext().getSystemService(Context.TELEPHONY_SERVICE);
		port_Str = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		Log.v("On Create AVD", port_Str);
		try
		{
		ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
        new Server().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             * 
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e("Abhinav", "Can't create a ServerSocket");
		}
		return true;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) {

         
		String temp=null;
		Message m=new Message();
		m.setKey(selection);
		SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
		queryBuilder.setTables(MyDbHelper.tableName);

		SQLiteDatabase query_db = dbHelper.getReadableDatabase();

		if(successor.equals("")){
			if(selection.equals("*"))
			{

				Cursor cursor = queryBuilder.query(query_db,null,null, null, null, null, null);

				//cursor.setNotificationUri(getContext().getContentResolver(), uri);
				Log.v("query", selection);

				return cursor;
			}
			else if(selection.equalsIgnoreCase("@"))
			{
				Cursor cursor = queryBuilder.query(query_db,null,null, null, null, null, null);

				Log.v("query", selection);

				return cursor;

			}
			else{
				Cursor cursor = queryBuilder.query(query_db,null,MyDbHelper.tableName +"."+MyDbHelper.col_key + "='"+selection+"'", null, null, null, null);

				Log.v("query", selection);

				return cursor;	
			}
		}
		else{
			if(selection.equals("*"))
			{
				Cursor cursor = queryBuilder.query(query_db,null,null, null, null, null, null);
				cursor.moveToFirst();
				String message=port_Str+"|";
				while (cursor.isAfterLast() == false) 
				{
					String key = cursor.getString(0);
					String value=cursor.getString(1);
					message=message+key+":"+value+"|";

					cursor.moveToNext();

				}

				cursor.close();
				new Thread(new Client(message, successor, "$Gdump$")).start();	

				while(flag);
				
				Log.v("query", selection);
				MatrixCursor max=new MatrixCursor(new String[]{"key","value"});
				String[] strArray=fflag.split("\\|");
				for(String str:strArray)
				{			

					if(str.contains(":"))
					{	
					RowBuilder bg = max.newRow();
					bg.add("key",str.split(":")[0]);
					bg.add("value",str.split(":")[1]);
					
					Log.v("key",str.split(":")[0]);
					Log.v("value",str.split(":")[1]);
					}
				}




				fflag="";
				return max;	

			}
			else if(selection.equalsIgnoreCase("@"))
			{
				Cursor cursor = queryBuilder.query(query_db,null,null, null, null, null, null);

				Log.v("query", selection);

				return cursor;
			}
			else
			{
				Cursor cursor = queryBuilder.query(query_db,null,MyDbHelper.tableName +"."+MyDbHelper.col_key + "='"+selection+"'", null, null, null, null);

				if(!(cursor.getCount()>0))
				{
					new Thread(new Client(selection+"|"+port_Str, successor, "$QuerySucc$")).start();	

				}
				else{
					return cursor;
				    }
				while(flag1);
			
				
				Log.v("query", selection);
				MatrixCursor max=new MatrixCursor(new String[]{"key","value"});
				max.addRow(new String[]{selection,sflag});



				sflag="";
				flag1=false;
				return max;	
			}
		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	public static String getPortNo(String port)
	{
		switch (Integer.parseInt(port)) {
		case 5554:return "11108";
		case 5556:return "11112";
		case 5558:return "11116";
		case 5560:return "11120";
		case 5562:return "11124";

		default:return "No Matching port Found for the AVD";

		}
	}
	public static String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public void insertSuc(ContentValues cv)
	{
		getContext().getContentResolver().insert(mUri, cv);
	}

public class Server extends AsyncTask<ServerSocket, String , Void>{

	ServerSocket serverSocket;
	int server_port=10000;
	Socket sock;
	String var;
	public  String base_avd="5554";
	String myavd;
	protected Void doInBackground(ServerSocket... arg0) {
		// TODO Auto-generated method stub
	
	ObjectInputStream obj;
	ContentValues cv;



		// TODO Auto-generated method stub


		myavd=SimpleDhtProvider.port_Str;
		if(myavd.equalsIgnoreCase(base_avd))
		{
			//SimpleDhtProvider.predecessor="";
			//SimpleDhtProvider.successor="";
			System.out.println("Myavd:"+myavd);
		}

		else
		{
			System.out.println("otherAvd"+myavd);
			new Thread(new Client(myavd,"5554","$Join$")).start();
		}

		try {
			serverSocket = arg0[0];

			while(true)
			{	

				Socket socket=serverSocket.accept();
				obj=new ObjectInputStream(socket.getInputStream());
				Message message = null;
				try {
					message = (Message)obj.readObject();
					obj.close();
					socket.close();
				} catch (ClassNotFoundException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}

				
				//if(myavd.equalsIgnoreCase("5554"))
				//{
				if(message.getKey().contains("$Join$"))
				{
					System.out.println("Inside Server Join"+message.getKey()+message.getValue());

					try {
						joinNewNode(message.getValue().toString());

					} 
					catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				else if(message.getKey().contains("$SetSucc$"))
				{
					successor=message.getValue();
					System.out.println("My Port - >" + port_Str + "succe"+successor);
				}
				else if(message.getKey().contains("$SetPred$"))
				{
					predecessor=message.getValue();
					System.out.println("My Port - >" + port_Str + "Pred"+predecessor);
				}
				else if(message.getKey().contains("$InsertKeySucc$"))
				{
					try {
						if(genHash(port_Str).compareTo(SimpleDhtProvider.genHash(SimpleDhtProvider.predecessor))<0)
						{
							try {
								String[] str=message.getValue().split("\\|");
								
								if((genHash(str[0]).compareTo(genHash(port_Str))<0)||(genHash(str[0]).compareTo(genHash(predecessor))>0))
								{
									
									cv =new ContentValues();
									cv.put("key",str[0]);
									cv.put("value", str[1]);
									getContext().getContentResolver().insert(mUri, cv);

								}

								else
								{

									new Thread(new Client(message.getValue(),SimpleDhtProvider.successor, "$InsertKeySucc$")).start();

								}
							} catch (NoSuchAlgorithmException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
						else
						{
							String[] str=message.getValue().split("\\|");
						
							try {
								if((genHash(str[0]).compareTo(genHash(port_Str)) < 0)&&(genHash(str[0]).compareTo(genHash(predecessor))>0))
								{

									//String[] str=message.getValue().split("\\|");
									cv =new ContentValues();
									cv.put("key",str[0]);
									cv.put("value", str[1]);
									getContext().getContentResolver().insert(mUri, cv);

								}
								else 
								{
									new Thread(new Client(message.getValue(),SimpleDhtProvider.successor, "$InsertKeySucc$")).start();
								}
							} catch (NoSuchAlgorithmException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} catch (NoSuchAlgorithmException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				
				else if(message.getKey().contains("$QuerySucc$"))
				{
					String temp=null;
					SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
					queryBuilder.setTables(MyDbHelper.tableName);

					SQLiteDatabase query_db = SimpleDhtProvider.dbHelper.getReadableDatabase();
					Cursor cursor = queryBuilder.query(query_db,null,MyDbHelper.tableName +"."+MyDbHelper.col_key + "='"+message.getValue().split("\\|")[0].trim()+"'", null, null, null, null);
					
					cursor.moveToFirst();
					while (cursor.isAfterLast() == false) 
					{
					   temp = cursor.getString(1);
						cursor.moveToNext();
						Log.v("Local dump", temp);
					}
					
					cursor.close();
					if(!(cursor.getCount()>0))
					{
						new Thread(new Client(message.getValue(),successor, "$QuerySucc$")).start();	
	
					}
					else
					{
					Log.v("In server message",message.getValue().split("\\|")[1]);
					new Thread(new Client(temp,message.getValue().split("\\|")[1].trim(), "$QueryResponse$")).start();	
					}

				}
				
				else if(message.getKey().contains("$QueryResponse$"))
				{
					sflag=message.getValue();
					flag1=true;
				}
				
				else if(message.getKey().contains("$Gdump$"))
				{
					if(message.getValue().contains(SimpleDhtProvider.port_Str))
					{
						fflag=message.getValue();
						flag=true;

					}
					else
					{
					
					
					SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
					queryBuilder.setTables(MyDbHelper.tableName);

					SQLiteDatabase query_db = SimpleDhtProvider.dbHelper.getReadableDatabase();
					Cursor cursor = queryBuilder.query(query_db,null,null, null, null, null, null);
					cursor.moveToFirst();
					String newMessage=message.getValue()+"|";
					while (cursor.isAfterLast() == false) 
					{
					    String key = cursor.getString(0);
						String value=cursor.getString(1);
						newMessage=newMessage+key+":"+value+"|";
						
					    cursor.moveToNext();
					}
					cursor.close();
					new Thread(new Client(newMessage,successor, "$Gdump$")).start();	
					}

				}
				else if(message.getKey().contains("Delete"))
				{
					if(message.getValue().contains("*"))
					{
					sqLite_db.delete(MyDbHelper.tableName,null,null);
					new Thread(new Client("*", successor, "Delete")).start();	

					}
					else
					{
						int no=sqLite_db.delete(MyDbHelper.tableName,"key=?",new String[]{ message.getValue()});
		           
					if(no==0)
		            {
		    			new Thread(new Client(message.getValue(), successor, "Delete")).start();	

		            }
						
					}
					


					

				}

			}
		

	


} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}

	return null;
	}

	public void joinNewNode(String newChordNode) throws NoSuchAlgorithmException{

		Log.v("JOIN in Join new Node",newChordNode);
		String curr_port = port_Str;
		
		
		
		if (successor.equals("")){
			successor = newChordNode;
			predecessor = newChordNode;
			new Thread(new Client(curr_port,newChordNode,"$SetSucc$" )).start();
			new Thread(new Client(curr_port,newChordNode,"$SetPred$" )).start();
		}
		else if(genHash(predecessor).compareTo(genHash(curr_port))>0){
			
			if(genHash(newChordNode).compareTo(genHash(predecessor)) > 0 || genHash(newChordNode).compareTo(genHash(curr_port)) <= 0 ){
				//new Thread(new Client(node to be added,node to which msg is sent ,"$SetSucc$" )).start();
				
				new Thread(new Client(predecessor,newChordNode,"$SetPred$" )).start();
				new Thread(new Client(curr_port,newChordNode,"$SetSucc$" )).start();
				new Thread(new Client(newChordNode,predecessor,"$SetSucc$" )).start();
				new Thread(new Client(newChordNode,curr_port,"$SetPred$" )).start();
				
			}
			else
			{
				new Thread(new Client(newChordNode,successor,"$Join$" )).start();
	
			}
		}
		else if(genHash(predecessor).compareTo(genHash(newChordNode))<0 && genHash(newChordNode).compareTo(genHash(curr_port))<=0){
			
			new Thread(new Client(predecessor,newChordNode,"$SetPred$" )).start();
			new Thread(new Client(curr_port,newChordNode,"$SetSucc$" )).start();
			new Thread(new Client(newChordNode,predecessor,"$SetSucc$" )).start();
			new Thread(new Client(newChordNode,curr_port,"$SetPred$" )).start();
		}
		else
		{
			new Thread(new Client(newChordNode,successor,"$Join$" )).start();

		}

	}
}




}







