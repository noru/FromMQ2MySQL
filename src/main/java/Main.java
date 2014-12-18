import com.aliyun.mqs.client.CloudQueue;
import com.aliyun.mqs.client.DefaultMQSClient;
import com.aliyun.mqs.model.Message;

import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by xiua on 12/16/2014.
 */
public class Main {

    private static boolean DEBUG = true;

    private static String OWNER ="dyaah7poyv";
    private static final String ACCESS_ID = "9WpjvtjQhiLcE5xf";
    private static final String ACCESS_KEY = "NsO8dXQoIYmO6ZhSk8sjgUXcyvBX29";
    private static String PUBLIC_HOST_URL = "http://%s.mqs-cn-qingdao.aliyuncs.com";
    private static String PRIVATE_HOST_URL = "http://%s.mqs-cn-qingdao-internal.aliyuncs.com";
    private static String QUEUE = "test-queue";


    private static DefaultMQSClient mMQSClient;
    private static CloudQueue mQueue;


    public static void main( String[] args )
    {

        System.out.println("task begin");
        //putMessage("hello there!");
        Message msg = null;

        while ((msg = getMessage()) != null){
            DBManager dm = null;
            try {
                dm = DBManager.GetInstance();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            if (dm.putMessageToDB(msg)){
                removeMessage(msg);
            };
        }

        finish();
        System.out.println("task finished");

    }

    private static void finish() {
        // close MQS
        getMQSClient().close();
        // close MySQL
        if (DBManager.isValid()){
            try {
                DBManager.GetInstance().finalize();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static Message getMessage() {
        Message msg = null;

        try {
            CloudQueue queue = getQueue();
            msg = queue.popMessage();

        } catch (Exception e){
            System.out.println("Failed getting message from MQS");
        }
        System.out.println("Got message.");
        System.out.println("ID: " + msg.getMessageId());
        System.out.println("Body: " + msg.getMessageBodyAsString());
        return msg;
    }

    private static void removeMessage(Message msg){
        CloudQueue queue = getQueue();
        queue.deleteMessage(msg.getReceiptHandle());
        System.out.println("Message removed. ID: " + msg.getMessageId());
    }

    private static DefaultMQSClient getMQSClient() {
        if (mMQSClient == null){
            mMQSClient = new DefaultMQSClient(String.format(DEBUG ? PUBLIC_HOST_URL: PRIVATE_HOST_URL , OWNER),
                    ACCESS_ID,
                    ACCESS_KEY);
        }
        return mMQSClient;
    }

    private static CloudQueue getQueue(){
        if (mQueue == null){
            mQueue = getMQSClient().getQueueRef(QUEUE);
        }
        return mQueue;
    }

    private static boolean putMessage(String msg) {
        try {
            CloudQueue queue = getQueue();
            Message newMsg = new Message();
            newMsg.setMessageBody(msg);
            queue.putMessage(newMsg);
        } catch (Exception e){
            System.out.println("Failed putting message to MQS");
            return false;
        }
        System.out.println("Message: " + msg + " has been put in queue");
        return true;
    }

    private static class DBManager{

        private String TEST_MYSQL_URL = "jdbc:mysql://localhost:8889/anderson";
        private String MYSQL_DOMAIN = "jdbc:mysql://rds3qvbbr3qvbbr.mysql.rds.aliyuncs.com";
        private String DB_NAME = "test_mqs";
        private String TABLE_NAME = "flat";
        private String MYSQL_URL = MYSQL_DOMAIN + "/" + DB_NAME;
        private String USER = DEBUG ? "root" : "test_mqs";
        private String PSWD = DEBUG ? "root" : "123456";

        private static DBManager self;
        private ResultSet rs;
        private Statement stmt;
        private Connection conn;

        private DBManager() throws Exception {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(DEBUG ? TEST_MYSQL_URL : MYSQL_URL, USER, PSWD);
                stmt = conn.createStatement();
                stmt.execute("create table if not exists flat (ID int(4) not null primary key auto_increment, STR varchar(100));");

            } catch (ClassNotFoundException e) {
                System.out.println("failed connecting db");
                e.printStackTrace();
                throw new Exception("failed get DB connection");
            }
        }

        public static DBManager GetInstance() throws Exception {
            if (self == null){
                self = new DBManager();
            }
            return self;
        }
        public static boolean isValid(){
            return self != null;
        }
        public void finalize(){
            // close Result set, Statement, Connection in order
            if(rs != null){
                try{
                    rs.close() ;
                }catch(SQLException e){
                    e.printStackTrace() ;
                }
            }
            if(stmt != null){
                try{
                    stmt.close() ;
                }catch(SQLException e){
                    e.printStackTrace() ;
                }
            }
            if(conn != null){
                try{
                    conn.close() ;
                }catch(SQLException e){
                    e.printStackTrace() ;
                }
            }
        }

        public boolean putMessageToDB(Message msg) {
            try {
                stmt.executeUpdate(String.format("insert into %s values(null, \"%s\")", TABLE_NAME, msg.getMessageBodyAsString()));
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }
            return true;
        }
    }
}

