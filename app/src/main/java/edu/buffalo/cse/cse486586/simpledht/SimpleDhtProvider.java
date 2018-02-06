package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.ArrayList;
import java.util.HashMap;

import android.database.MatrixCursor;
import android.database.MergeCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.AsyncTask;
import android.util.Log;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.telephony.TelephonyManager;

public class SimpleDhtProvider extends ContentProvider {

    static final String TAG = SimpleDhtProvider.class.getSimpleName();

    private final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");

    //Referred from PA2A
    class Database extends SQLiteOpenHelper {

        //Defining the table name and column names

        public static final String TABLE_NAME = "CHORD";
        public static final String COLUMN_KEY = "key";
        public static final String COLUMN_VALUE = "value";

        // create table table_name (key text primary, value text)
        private static final String SQL_CREATE_ENTRIES = "CREATE TABLE "+ TABLE_NAME+ "("+ COLUMN_KEY + " TEXT PRIMARY KEY, "+ COLUMN_VALUE + " TEXT NOT NULL)";

        private static final String SQL_DELETE_ENTRIES = "DROP TABLE IF EXISTS " + TABLE_NAME;

        public static final int DATABASE_VERSION = 1;
        public static final String DATABASE_NAME = "Chord.db";


        public Database(Context context) {

            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }
        public void onCreate(SQLiteDatabase db) {

            db.execSQL(SQL_CREATE_ENTRIES);
        }
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            db.execSQL(SQL_DELETE_ENTRIES);
            onCreate(db);
        }
        public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            onUpgrade(db, oldVersion, newVersion);
        }

    }

    Database database;
    SQLiteDatabase db;

    String myPort=null;

    String node_id; //Each content provider instance should have a node id derived from its emulator port.


    int SERVER_PORT = 10000;
    String REQUEST = "11108";

    ArrayList node_list;
    ArrayList port_list;

    String predecessor;
    String successor;
    String min; //port with the least node_id
    String max; //port with the greatest node_id

    boolean all = false;
    boolean delete_all = false;
    MatrixCursor all_cursor;
    MatrixCursor single_cursor;
    boolean single=false;

    private HashMap<String,String> key_value;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        System.out.println("Delete ");
        database = new Database(this.getContext());
        db = database.getWritableDatabase();

        String[] args = new String[]{selection};
        Cursor cursor = null;

        String succ_port = successor;

        if(args[0] . equals("*"))
        {
            if(node_list.size() == 1) //only one node
            {
                db.rawQuery("DROP table CHORD",null);
            }
            else
            {
                db.rawQuery("DROP table CHORD",null);
                //Forward delete to successor;
                String message = "delete" + ":#:" + myPort;
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, succ_port);

                while(!delete_all)
                {

                }
            }
        }
        if(args[0].equals("@"))
        {
            cursor = db.rawQuery("DROP table CHORD",null);
        }
        db.delete(Database.TABLE_NAME,"key = ?", args);
        db.close();

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

        //Referred from https://developer.android.com/reference/android/database/sqlite/SQLiteOpenHelper.html#getWritableDatabase()
        System.out.println("Here ");
        database = new Database(this.getContext());
        db = database.getWritableDatabase();


        /* Referred from https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#insertWithOnConflict(java.lang.String,%20java.lang.String,%20android.content.ContentValues,%20int)
        For inserting a row into database */
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        //System.out.println(key+" = "+value);
        String hash_key=null,hash_value=null;
        try {
            hash_key = genHash(key);
            hash_value = genHash(value);
        }catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG,"NoSuchAlgorithmException: Error in insert method");
        }
        /*ContentValues hash_values = new ContentValues();
        hash_values.put("key",hash_key);
        hash_values.put("value",hash_value);

        System.out.println(hash_key+" = "+hash_value);

        long row_id = db.insertWithOnConflict(database.TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
        Log.v("insert", hash_values.toString());
        db.close();*/

        System.out.println("hash_key = "+hash_key);
        boolean decide = inPartition(hash_key);
        System.out.println("In partition: "+decide);
        if(decide)
        {
            System.out.println("Inserted "+key);
            long row_id = db.insertWithOnConflict(database.TABLE_NAME,null,values,SQLiteDatabase.CONFLICT_REPLACE);
        }

        else
        {
            //check with the successor
            String message = "insert"+":#:"+hash_key+":#:"+key+":#:"+value;
            String succ_port = successor;
            System.out.println("checking succ for insertion "+succ_port+ "for "+key);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,succ_port);
        }

        db.close();

        return uri;

        //return null;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        database = new Database(this.getContext());

        //Initializing the values
        // Referred from PA1
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));

        try{
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }catch(IOException e){
            Log.e(TAG, "Can't create a ServerSocket");
            return false;
        }

        try{
            node_id = genHash(portStr);
        }catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG,"NoSuchAlgorithmException in onCreate()");
        }

        node_list =  new ArrayList();
        port_list = new ArrayList();

        key_value = new HashMap<String, String>();

        String message = "join" + ":#:" + node_id + ":#:" + myPort;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, REQUEST);


        //Cheking the hash values
        /*try{
            System.out.println("5554: "+ genHash("5554"));
            System.out.println("5556: "+ genHash("5556"));
            System.out.println("5558: "+ genHash("5558"));
            System.out.println("5560: "+ genHash("5560"));
            System.out.println("5562: "+ genHash("5562"));

            for(int i=0; i< 50;i++)
            {
                String var = "key"+i;
                System.out.println(var+": "+genHash(var));
            }
        }catch(NoSuchAlgorithmException e)
        {
            Log.e(TAG,"Generating hash values");
        }*/

        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        database = new Database(this.getContext());
        db = database.getReadableDatabase();
        Cursor cursor = null;

        System.out.println("Query");
        String[] args = new String[]{selection};
        System.out.println(args[0]);
        int size = args.length;
        System.out.println("query size " + size);

        if (args[0].equals("@")) {
            cursor = db.rawQuery("SELECT * FROM CHORD", null);
            return cursor;
        }

        else if (args[0].equals("*"))
        {
            all_cursor = new MatrixCursor(new String[]{"key","value"});
            cursor = db.rawQuery("SELECT * FROM CHORD", null);
            String message = "all" + ":#:" + myPort;
            String succ_port = successor;
            if(succ_port == null)
            {
                return cursor;
            }
            if(succ_port.equals(myPort))
                return cursor;
            else {

                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message, succ_port);

                while (!all) {
                    try{
                        Thread.sleep(5);
                    }catch(InterruptedException e)
                    {
                        Log.e(TAG,"thread interrupted exception");
                    }
                }

                int rows = key_value.size();
                System.out.println("No of rows in map: "+rows);

                for(String k:key_value.keySet())
                {
                    String val1 = k;
                    String val2 = key_value.get(k);
                    all_cursor.addRow(new String[]{val1,val2});
                }

                MergeCursor star = new MergeCursor(new Cursor[]{cursor,all_cursor});

                System.out.println("rows: "+star.getCount());
                return star;
            }

        }

        else
        {
            String[] hash_args = new String[size];
            String key = args[0];
            String hash_key = "";
            try {
                hash_key = genHash(key);
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Query Exception");
            }
            hash_args[0] = hash_key;
            //System.out.println(hash_args[0]);
            single_cursor = new MatrixCursor(new String[]{"key","value"},1);

            boolean decide = inPartition(hash_key);
            if (decide) {
                cursor = db.query(database.TABLE_NAME, null, "key = ?", args, null, null, null);
                return cursor;
            } else {
                String query_message = "one" + ":#:" + hash_key + ":#:" + key + ":#:"+ myPort;
                String succ_port = successor;
                System.out.println("checking succ for querying " + succ_port + "for " + key);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, query_message, succ_port);
                single=false;


                while(!single)
                {

                }

                return single_cursor;
            }

            /*Cursor cursor ;
            cursor = db.query(database.TABLE_NAME,null,"key = ?",args,null,null,null);*/

        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private void clientTask(String message,String port)
    {
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message,port);

    }


    //Server Task
    class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {

                while (true) {
                    // Listening for a connection
                    Socket client = serverSocket.accept();  //Referred from https://docs.oracle.com/javase/tutorial/displayCode.html?code=https://docs.oracle.com/javase/tutorial/networking/sockets/examples/EchoServer.java
                    //Reading data from client
                    InputStream is = client.getInputStream();
                    byte data[] = new byte[5000];
                    is.read(data);
                    String message = new String(data);

                    if (message.contains("join")) {
                        node_join(message);
                    }

                    if (message.contains("insert")) {
                        String[] message_args = message.split(":#:");
                        String hashed_key = message_args[1].trim();
                        String original_key = message_args[2].trim();
                        String original_value = message_args[3].trim();

                        boolean decide = inPartition(hashed_key);
                        if (decide) {
                            ContentValues cv = new ContentValues();
                            cv.put("key", original_key);
                            cv.put("value", original_value);
                            System.out.println("Inserted " + original_key);
                            getContext().getContentResolver().insert(uri, cv);
                        } else {
                            String port = successor;
                            //System.out.println("before client "+port);
                            clientTask(message, port);
                        }
                    }

                    if (message.contains("nodes")) {
                        String[] message_args = message.split(":#:");
                        String pred = message_args[1].trim();
                        String succ = message_args[2].trim();
                        String least = message_args[3].trim();
                        String greatest = message_args[4].trim();

                        predecessor = pred;
                        successor = succ;
                        min = least;
                        max = greatest;
                    }

                    if (message.contains("query")) {
                        String[] message_args = message.split(":#:");
                        String hashed_key = message_args[1].trim();
                        String original_key = message_args[2].trim();

                        boolean decide = inPartition(hashed_key);
                        if (decide) {
                            System.out.println("Querying " + original_key);
                            query(uri, null, original_key, null, null, null);
                        } else {
                            String port = successor;
                            clientTask(message, port);
                        }
                    }

                    if (message.contains("all")) {
                        String[] message_args = message.split(":#:");
                        String origin_port = message_args[1].trim();

                        String avd_num = get_avd_num(origin_port);
                        String origin_id = "";
                        try {
                            origin_id = genHash(avd_num);
                        } catch (NoSuchAlgorithmException e) {
                            Log.e(TAG, "Error in all");
                        }
                        if (node_id.equals(origin_id))
                        {
                            clientTask("true", origin_port);
                        }
                        else {
                            //Read from content provider
                            db = database.getReadableDatabase();

                            Cursor cursor = db.rawQuery("SELECT * FROM CHORD", null);
                            String key_value = "";

                            System.out.println("rows in each cursor: " + cursor.getCount());

                            int i=0;

                            while (cursor.moveToNext()) {
                                String key = cursor.getString(0);
                                String value = cursor.getString(1);
                                String pair = key + "##" + value;
                                key_value = key_value + "&&&" + pair;
                                i++;
                            }

                            System.out.println("i = "+i);

                            System.out.println("Sent msg from " + myPort);

                            String message1 = key_value;
                            String message2 = message.trim();
                            String succ_port = successor;

                            System.out.println("message1 " + message1);
                            System.out.println("message2 " + message2);

                            System.out.println("all successor port: " + succ_port);

                            clientTask(message1,origin_port);
                            clientTask(message2,succ_port); //Forwarding the message

                        }
                    }

                    if (message.contains("&&&")) {
                        String[] pairs = message.split("&&&");
                        System.out.println(message);


                        int pairs_size = pairs.length;
                        System.out.println("No of pairs: " + pairs_size);

                        if(pairs_size >1) {
                            for (int i = 1; i < pairs_size; i++) {
                                System.out.println("pair: " + pairs[i]);
                                pairs[i] = pairs[i].trim();
                                String[] key_value_pair = pairs[i].split("##");
                                key_value.put(key_value_pair[0], key_value_pair[1]);
                            }
                        }

                    }

                    if (message.contains("one")) {
                        String[] message_args = message.split(":#:");
                        String hashed_key = message_args[1].trim();
                        String original_key = message_args[2].trim();
                        String origin_port = message_args[3].trim();

                        boolean decide = inPartition(hashed_key);

                        if (decide)
                        {
                            db = database.getReadableDatabase();

                            Cursor cursor = db.query(database.TABLE_NAME, null, "key = ?", new String[]{original_key}, null, null, null);
                            String val1 = "", val2 = "";
                            while (cursor.moveToNext())
                            {
                                val1 = cursor.getString(0);
                                val2 = cursor.getString(1);
                            }
                            String message1 = "set" + ":#:" + val1 + ":#:" + val2;
                            clientTask(message1, origin_port);
                        }
                        else
                        {
                            clientTask(message, successor);
                        }
                    }

                    if (message.contains("set"))
                    {
                        String[] message_args = message.split(":#:");
                        String key = message_args[1].trim();
                        String val = message_args[2].trim();
                        single_cursor.addRow(new String[]{key, val});
                        single = true;
                    }

                    if (message.contains("true")) {
                        all = true;
                    }

                    if(message.contains("delete"))
                    {
                        String[] message_args = message.split(":#:");
                        String origin_port = message_args[1];
                        String origin_id="";
                        try{
                            origin_id = genHash(get_avd_num(origin_port));
                        }catch(NoSuchAlgorithmException e)
                        {
                            Log.e(TAG,"genHash error in delete");
                        }

                        if(node_id.equals(origin_id))
                        {
                            clientTask("star",origin_port);
                        }

                        else
                        {
                            db.rawQuery("DROP table CHORD",null);
                            //forward to successor
                            clientTask(message,successor);
                        }
                    }

                    if(message.contains("star"))
                    {
                        delete_all = true;
                    }


                    //System.out.println("pred "+predecessor);
                    //System.out.println("succ "+successor);

                    /*int size = node_list.size();
                    int i=0;
                    while(i<size)
                    {
                        System.out.println(port_list.get(i) + ": "+node_list.get(i));
                        i++;
                    }*/

                    /*try {
                        String pred = getPredecessor("11108");
                        String succ = getSuccessor("11112");
                        System.out.println(pred);
                        System.out.println(succ);
                    }catch(NullPointerException e)
                    {
                        Log.d(TAG,"Null Pointer Exception");
                    }
                    boolean res = inPartition("218f7f72b198dadd244e61801abe1ec3a4857bc9");
                    System.out.println(res);*/
                }
            } catch (IOException e) {
                System.out.println("IO Exception in Server");
            }

            return null;
        }

    }

    void node_join(String msg)
    {
        String message = msg;
        String token = ":"+"#"+":";
        String[] message_parts = message.split(token);
        String id = message_parts[1];
        String port = message_parts[2].trim();

        //node_list.add(id);
        int location = node_list.indexOf(id);
        int size = node_list.size();
        int i = 0;

        if(size == 0)
        {
            node_list.add(id);
            port_list.add(port);
        }
        else
        {
            //System.out.println("size = "+size);
            while(i<size) {
                int compare = id.compareTo((String) node_list.get(i));
                //System.out.println("compare= "+compare + "i= "+i);
                if(compare > 0) {
                    //System.out.println("i = " + i);
                    i++;
                }
                else
                    break;
            }
            node_list.add(i,id);
            port_list.add(i,port);
        }

        int new_size = node_list.size();
        i=0;
        while(i<new_size)
        {
            String avd_id = (String)port_list.get(i);
            String pred = getPredecessor(avd_id);
            String succ = getSuccessor(avd_id);

            /*System.out.println("avd: "+avd_id);
            System.out.println("pred: "+pred);
            System.out.println("succ: "+succ);*/

            String least = (String)node_list.get(0);
            String greatest = (String)node_list.get(new_size-1);

            String nodes_message = "nodes" + ":#:" + pred + ":#:" + succ + ":#:" + least + ":#:" + greatest;

            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, nodes_message, avd_id);

            i++;
        }


    }

    String getPredecessor(String port)
    {
        int index = port_list.indexOf(port);
        int size = port_list.size();

        String ele;

        if(index == 0)
        {
            if(size == 1)
                return port;
            else {
                ele = (String) port_list.get(size - 1);
                return ele;
            }
        }

        if(index > 0)
        {
            ele = (String) port_list.get(index - 1);
            return ele;
        }

        return null;
    }

    String getSuccessor(String port)
    {
        int index = port_list.indexOf(port);
        int size = port_list.size();
        String ele;
        if( index == 0)
        {
            if(size == 1)
                return port;
            else
            {
                ele = (String) port_list.get(index + 1);
                return ele;
            }
        }

        if(index > 0)
        {
            if(index == size-1)
            {
                ele = (String) port_list.get(0);
                return ele;
            }
            else
            {
                ele = (String) port_list.get(index + 1);
                return ele;
            }
        }


        return null;
    }

    boolean inPartition(String hashed_key)
    {
        String key = hashed_key;
        //int index = node_list.indexOf(node_id);
        //String port = (String)port_list.get(index);
        //String pred = getPredecessor(port);
        //String succ = getSuccessor(port);
        String pred = predecessor;
        String succ = successor;
        String max_val = max;
        String min_val = min;
        String pred_value="",min_hash="",max_hash ="";

        //System.out.println("pred "+pred);
        //System.out.println("succ "+succ);

        if(node_list.size() == 1)
            return true;

        try{
            if(pred == null && succ == null)
            {
                //Only one node
                return true;
            }
            else
            {
                int p = port_list.indexOf(pred);
                int s = port_list.indexOf(succ);
                try
                {
                    String avd1 = get_avd_num(pred);
                    String avd2 = get_avd_num(succ);

                    pred_value = genHash(avd1);
                    min_hash = genHash(get_avd_num(min_val));
                    max_hash = genHash(get_avd_num(max_val));
                }catch(NoSuchAlgorithmException e)
                {
                    Log.e(TAG,"In partition : genHash error");
                }


                //System.out.println(key.compareTo(pred));
                //System.out.println(key.compareTo(pred));


                //border condition
                if(pred_value.compareTo(node_id) > 0 && key.compareTo(pred_value) > 0)
                {
                    return true;
                }

                else if(pred_value.compareTo(node_id) > 0 && key.compareTo(node_id)<=0)
                    return true;

                else if(key.compareTo(pred_value) > 0 && key.compareTo(node_id) <= 0)
                    return true;
                else
                    return false;


            }
        }catch(NullPointerException e)
        {
            Log.d(TAG,"inPartition");
        }

        return false;
    }

    String get_avd_num(String port)
    {
        if(port.equals("11108"))
            return "5554";
        if(port.equals("11112"))
            return "5556";
        if(port.equals("11116"))
            return "5558";
        if(port.equals("11120"))
            return "5560";
        if(port.equals("11124"))
            return "5562";
        return "";
    }

    //Client Task
    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            String msgToSend = msgs[0];
            System.out.println("msgs[0] "+msgs[0]);
            //System.out.println("msgs[1] "+msgs[1]);
            try{
                //msgs[1] = msgs[1].trim();
                System.out.println("msgs[1] "+msgs[1]);
                String num = msgs[1];
                int remotePort = Integer.parseInt(msgs[1].trim());
                //System.out.println(remotePort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),remotePort);
                OutputStream os = socket.getOutputStream();
                //System.out.println(msgToSend);
                msgToSend = msgToSend.trim();
                os.write(msgToSend.getBytes());
                socket.shutdownOutput(); // Referred from https://docs.oracle.com/javase/7/docs/api/java/net/Socket.html#shutdownOutput()
                //System.out.println(socket.isConnected());
                socket.close();

            }catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                System.out.println(e.toString());
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }

}