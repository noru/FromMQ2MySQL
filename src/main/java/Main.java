import com.aliyun.mqs.client.CloudQueue;
import com.aliyun.mqs.client.DefaultMQSClient;
import com.aliyun.mqs.model.Message;
import com.mysql.jdbc.StringUtils;

import java.sql.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Date;
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
        int total = 0, success = 0;
        Date d = new Date();
        System.out.println("---=== Task begin... ===---");
        System.out.println("***" + d);

//        putMessage("hello there again! haha");
//        putMessage("hello there again! haha1");
//        putMessage("hello there again! haha2");
        putMessage("org\n123,123,col2\nrow1a,row1b , kkasdf");
        //System.exit(0);

        /**
         * Iterate all the messages in queue, when success push to DB,
         * delete current message and continue
         *
         * Messages that failed stay in Queue and wait to be processed by next task
        */
        Message msg = null;
        while ((msg = getMessage()) != null){
            total++;
            DBManager dm = null;
            try {
                dm = DBManager.GetInstance();
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Connect DB failed. Message " + e.getMessage());
                System.out.println("Exit");
                System.exit(1);
            }
            if (dm.putMessageToDB(msg)){
                removeMessage(msg);
                success++;
            }
        }

        finish();

        System.out.println(String.format("*** Task finished. %s messages total, %s successfully put to RDS", total, success));
        System.out.println("***" + ((new Date()).getTime() - d.getTime()) + "ms elapsed.");
        System.out.println();
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
        System.out.println("Cleaned up.");
    }

    private static Message getMessage() {
        Message msg = null;

        try {
            CloudQueue queue = getQueue();
            msg = queue.popMessage();

        } catch (Exception e){
            System.out.println("Failed getting message from MQS");
        }
        if (msg != null){
            System.out.println("Got message.");
            System.out.println("ID: " + msg.getMessageId());
            System.out.println("Body: " + msg.getMessageBodyAsString());
        }
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
                // temp
                //stmt.execute("create table if not exists flat (ID int(4) not null primary key auto_increment, STR varchar(100));");

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

            String body = msg.getMessageBodyAsString();
            String[] split = body.split("\n");
            String type;
            String[] cols;
            ArrayList<String[]> rows = new ArrayList<String[]>();

            // message syntax check
            try {
                type = split[0];
                cols = split[1].split(",");
                String [] rowStr = split[2].split("\n");
                int colCount = cols.length;
                for (String s : rowStr){
                    String [] row = s.split(",");
                    if (row.length == colCount){
                        rows.add(row);
                    } else {
                        throw new Exception();
                    }
                }
            } catch (Exception e){
                System.out.println("Invalid message syntax!");
                return false;
            }


            try{
                switch (type){
                    // type = table_name
                    case "org":
                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key, EXTID varchar(20), NAME varchar(100))", type));
                        break;
                    case "dept":
                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key" +
                                ", ORGID char(24)" +
                                ", NAME varchar(100))", type));
                        break;
                    case "equipmodel":
                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key" +
                                ", ORGID char(24)" +
                                ", NAME varchar(100)" +
                                ", DESC varchar(100)" +
                                ", MODEL varchar(100)" +
                                ")", type));
                        break;
                    case "spot":
                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key" +
                                ", ORGID char(24)" +
                                ", EQUIPMODELID char(24)" +
                                ", NAME varchar(100)" +
                                ")", type));
                        break;
                    case "equip":
                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key" +
                                ", ORGID char(24)" +
                                ", EQUIPMODELID char(24)" +
                                ", DEPTID char(24)" +
                                ", SERIALNO varchar(20" +
                                ", CHKPTID char(24)" +
                                ", CHKPTNAME varchar(100)" +
                                ")", type));
                        break;
//                    case "subject":
//                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key" +
//                                ", ORGID char(24)" +
//                                ", EQUIPMODELID char(24)" +
//                                ", CHKPTID char(24)" +
//                                ", SPOTID char(24)" +
//                                ", SUBJECTID char(24)" +
//                                ", ITEMID char(24)" +
//                                ", STARTAT datetime" +
//                                ", EXPIREAT datetime" +
//                                ", GENAT datetime" +
//                                ")", type));
//                        break;
                    default:
                        System.out.println("Unknown type: " + type);
                        return false;
                }
                for ( String[] s : rows){
                    stmt.executeUpdate(MapValueToSQL(type, s));
                }

            } catch (SQLException e){
                System.out.println("Failed execute insert command");
                e.printStackTrace();
                return false;
            }



            System.out.println("Command executed successfully");
            return true;
        }

        private String MapValueToSQL(String type, String[] s) {

            ArrayList<String> values = new ArrayList<>();
            for (String S : s){
                String temp = "\"" + S.trim() + "\"";
                values.add(temp);
            }
            return String.format("replace into %s values(%s)", type, values.toString().replaceAll("^\\[|\\]$", ""));
        }
    }
}

