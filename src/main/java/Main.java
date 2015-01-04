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

//        // org
//        putMessage("org\n" +
//                "id,extId,name\n" +
//                "5475db7956b9ac2d464ba738,SANY,三一重工\n" +
//                "5475db7956b9ac2d464ba739,JIKE,上海迹客科技\n" +
//                "5485c8f3ec6e4b670357d5e0,BARUN,巴润矿业分公司");
//        // dpt
//        putMessage("dept\n" +
//                "id,orgId,name\n" +
//                "5475db7956b9ac2d464ba73a,5475db7956b9ac2d464ba738,三一售后\n" +
//                "5475db7956b9ac2d464ba73b,5475db7956b9ac2d464ba739,迹客Demo\n" +
//                "5485c8f4ec6e4b670357d5e1,5485c8f3ec6e4b670357d5e0,选矿厂\n" +
//                "5485c8f4ec6e4b670357d5e2,5485c8f3ec6e4b670357d5e0,采矿厂");

        // equipmodel
        putMessage("equipmodel\n" +
                "id,orgId,name,desc,model\n" +
                "5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba739,SRT45,SRT45,SRT45\n" +
                "5485c8f9ec6e4b670357d5f8,5485c8f3ec6e4b670357d5e0,XX型皮带机,XX型皮带机,XX型皮带机\n" +
                "5485c8f9ec6e4b670357d627,5485c8f3ec6e4b670357d5e0,旋回破碎机,旋回破碎机,旋回破碎机\n" +
                "5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba738,SRT45,SRT45,SRT45\n" +
                "5485c8f9ec6e4b670357d60b,5485c8f3ec6e4b670357d5e0,溢流型球磨机4系列,溢流型球磨机4系列,溢流型球磨机4系列\n" +
                "5485c8f9ec6e4b670357d633,5485c8f3ec6e4b670357d5e0,中碎旋回破碎机,中碎旋回破碎机,中碎旋回破碎机\n" +
                "5485c8f9ec6e4b670357d645,5485c8f3ec6e4b670357d5e0,YZ55B型牙轮钻机,YZ55B型牙轮钻机,YZ55B型牙轮钻机\n" +
                "5485c8f9ec6e4b670357d63c,5485c8f3ec6e4b670357d5e0,细碎旋回破碎机,细碎旋回破碎机,细碎旋回破碎机");
        // spot
//        putMessage("spot\n" +
//                "id,orgId,equipModelid,name\n" +
//                "5475db7a56b9ac2d464ba7b1,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,发动机左后侧\n" +
//                "5475db7a56b9ac2d464ba7b0,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,组合仪表\n" +
//                "5475db7a56b9ac2d464ba7af,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,角度传感器\n" +
//                "5475db7a56b9ac2d464ba7ae,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,前照灯\n" +
//                "5475db7a56b9ac2d464ba7ad,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,后照灯\n" +
//                "5475db7a56b9ac2d464ba7ac,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,刹车灯\n" +
//                "5475db7a56b9ac2d464ba7ab,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,倒车显示屏\n" +
//                "5475db7a56b9ac2d464ba7aa,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,举升按钮\n" +
//                "5475db7a56b9ac2d464ba7a9,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,下降按钮\n" +
//                "5475db7a56b9ac2d464ba7a8,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,浮动按钮\n" +
//                "5475db7a56b9ac2d464ba7a7,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,驾驶室仪表\n" +
//                "5475db7a56b9ac2d464ba7a6,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,车右后侧\n" +
//                "5475db7a56b9ac2d464ba7a5,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,车头盖内\n" +
//                "5475db7a56b9ac2d464ba7a4,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,变速箱侧面\n" +
//                "5475db7a56b9ac2d464ba7a3,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,膨胀水箱侧面\n" +
//                "5475db7a56b9ac2d464ba7a2,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,PTO\n" +
//                "5475db7a56b9ac2d464ba7a1,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,制动器壳体\n" +
//                "5475db7a56b9ac2d464ba7a0,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,后桥壳\n" +
//                "5475db7a56b9ac2d464ba79f,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,液压油箱\n" +
//                "5475db7a56b9ac2d464ba79e,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,发动机\n" +
//                "5475db7a56b9ac2d464ba79d,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,风扇\n" +
//                "5475db7a56b9ac2d464ba79c,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,发动机管路\n" +
//                "5475db7a56b9ac2d464ba79b,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,前后传动轴\n" +
//                "5475db7a56b9ac2d464ba79a,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,左后纵梁外侧\n" +
//                "5475db7a56b9ac2d464ba799,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,与左后纵梁焊接对应的4条棱边\n" +
//                "5475db7a56b9ac2d464ba798,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,扭力筒左右铸钢件方形法兰对应的8条焊缝\n" +
//                "5475db7a56b9ac2d464ba797,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,耳板与铸钢件腔体连接处\n" +
//                "5475db7a56b9ac2d464ba796,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,车中部\n" +
//                "5475db7a56b9ac2d464ba795,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,其他\n" +
//                "5485c8f9ec6e4b670357d60a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,漏斗\n" +
//                "5485c8f9ec6e4b670357d609,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,架体\n" +
//                "5485c8f9ec6e4b670357d608,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,油泵站\n" +
//                "5485c8f9ec6e4b670357d607,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,定滑轮\n" +
//                "5485c8f9ec6e4b670357d606,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,联轴器\n" +
//                "5485c8f9ec6e4b670357d605,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,头轮减速机\n" +
//                "5485c8f9ec6e4b670357d604,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,蛇形联轴器\n" +
//                "5485c8f9ec6e4b670357d603,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,滚筒\n" +
//                "5485c8f9ec6e4b670357d602,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,卸料小车\n" +
//                "5485c8f9ec6e4b670357d601,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,压带轮\n" +
//                "5485c8f9ec6e4b670357d600,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,除铁器\n" +
//                "5485c8f9ec6e4b670357d5ff,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,走台梯子\n" +
//                "5485c8f9ec6e4b670357d5fe,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,滚轮\n" +
//                "5485c8f9ec6e4b670357d632,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,机座\n" +
//                "5485c8f9ec6e4b670357d631,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,架体\n" +
//                "5485c8f9ec6e4b670357d630,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,横梁\n" +
//                "5485c8f9ec6e4b670357d62f,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,主轴\n" +
//                "5485c8f9ec6e4b670357d62e,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,水平轴\n" +
//                "5485c8f9ec6e4b670357d62d,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,液压站\n" +
//                "5485c8f9ec6e4b670357d62c,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,正压装置\n" +
//                "5485c8f9ec6e4b670357d62b,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,平衡缸\n" +
//                "5475db7a56b9ac2d464ba78c,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,发动机左后侧\n" +
//                "5475db7a56b9ac2d464ba78b,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,组合仪表\n" +
//                "5475db7a56b9ac2d464ba78a,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,角度传感器\n" +
//                "5475db7a56b9ac2d464ba789,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,前照灯\n" +
//                "5475db7a56b9ac2d464ba788,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,后照灯\n" +
//                "5475db7a56b9ac2d464ba787,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,刹车灯\n" +
//                "5475db7a56b9ac2d464ba786,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,倒车显示屏\n" +
//                "5475db7a56b9ac2d464ba785,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,举升按钮\n" +
//                "5475db7a56b9ac2d464ba784,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,下降按钮\n" +
//                "5475db7a56b9ac2d464ba783,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,浮动按钮\n" +
//                "5475db7a56b9ac2d464ba782,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,驾驶室仪表\n" +
//                "5475db7a56b9ac2d464ba781,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,车右后侧\n" +
//                "5475db7a56b9ac2d464ba780,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,车头盖内\n" +
//                "5475db7a56b9ac2d464ba77f,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,变速箱侧面\n" +
//                "5475db7a56b9ac2d464ba77e,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,膨胀水箱侧面\n" +
//                "5475db7a56b9ac2d464ba77d,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,PTO\n" +
//                "5475db7a56b9ac2d464ba77c,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,制动器壳体\n" +
//                "5475db7a56b9ac2d464ba77b,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,后桥壳\n" +
//                "5475db7a56b9ac2d464ba77a,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,液压油箱\n" +
//                "5475db7a56b9ac2d464ba779,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,发动机\n" +
//                "5475db7a56b9ac2d464ba778,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,风扇\n" +
//                "5475db7a56b9ac2d464ba777,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,发动机管路\n" +
//                "5475db7a56b9ac2d464ba776,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,前后传动轴\n" +
//                "5475db7a56b9ac2d464ba775,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,左后纵梁外侧\n" +
//                "5475db7a56b9ac2d464ba774,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,与左后纵梁焊接对应的4条棱边\n" +
//                "5475db7a56b9ac2d464ba773,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,扭力筒左右铸钢件方形法兰对应的8条焊缝\n" +
//                "5475db7a56b9ac2d464ba772,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,耳板与铸钢件腔体连接处\n" +
//                "5475db7a56b9ac2d464ba771,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,车中部\n" +
//                "5475db7a56b9ac2d464ba770,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,其他\n" +
//                "5485c8f9ec6e4b670357d626,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,齿轮组\n" +
//                "5485c8f9ec6e4b670357d625,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,进出料口\n" +
//                "5485c8f9ec6e4b670357d624,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,进料口\n" +
//                "5485c8f9ec6e4b670357d623,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,出料口\n" +
//                "5485c8f9ec6e4b670357d622,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,桶体和端盖\n" +
//                "5485c8f9ec6e4b670357d621,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,进、排矿端轴承箱\n" +
//                "5485c8f9ec6e4b670357d620,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,液压站\n" +
//                "5485c8f9ec6e4b670357d61f,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,小齿轮\n" +
//                "5485c8f9ec6e4b670357d61e,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,大齿轮\n" +
//                "5485c8f9ec6e4b670357d61d,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,减速机\n" +
//                "5485c8f9ec6e4b670357d61c,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,离合器\n" +
//                "5485c8f9ec6e4b670357d61b,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,甘喷控制柜\n" +
//                "5485c8f9ec6e4b670357d61a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,液压站油箱\n" +
//                "5485c8f9ec6e4b670357d619,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,稀油站油箱\n" +
//                "5485c8f9ec6e4b670357d618,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,操作室\n" +
//                "5485c8f9ec6e4b670357d617,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,分流马达\n" +
//                "5485c8f9ec6e4b670357d616,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,加球机\n" +
//                "5485c8f9ec6e4b670357d615,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,防护罩\n" +
//                "5485c8f9ec6e4b670357d614,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,旋流器平台\n" +
//                "5485c8f9ec6e4b670357d613,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,端盖\n" +
//                "5485c8f9ec6e4b670357d612,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,中空轴\n" +
//                "5485c8f9ec6e4b670357d611,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,其他\n" +
//                "5485c8f9ec6e4b670357d63b,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d633,主轴\n" +
//                "5485c8f9ec6e4b670357d63a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d633,上、下架体\n" +
//                "5485c8f9ec6e4b670357d639,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d633,水平轴\n" +
//                "5485c8f9ec6e4b670357d638,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d633,偏心套总称\n" +
//                "5485c8f9ec6e4b670357d637,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d633,工作介质\n" +
//                "5485c8f9ec6e4b670357d67f,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,行程支架\n" +
//                "5485c8f9ec6e4b670357d67e,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,圆轴\n" +
//                "5485c8f9ec6e4b670357d67d,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,平衡梁\n" +
//                "5485c8f9ec6e4b670357d67c,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,履带板\n" +
//                "5485c8f9ec6e4b670357d67b,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,减速箱\n" +
//                "5485c8f9ec6e4b670357d67a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,铜套\n" +
//                "5485c8f9ec6e4b670357d679,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,行程一级链条\n" +
//                "5485c8f9ec6e4b670357d678,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,行程二级链条\n" +
//                "5485c8f9ec6e4b670357d677,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,行程末级链条\n" +
//                "5485c8f9ec6e4b670357d676,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,支重轮、托轮、张紧轮\n" +
//                "5485c8f9ec6e4b670357d675,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,中空轴\n" +
//                "5485c8f9ec6e4b670357d674,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,提升链条\n" +
//                "5485c8f9ec6e4b670357d673,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,减震器\n" +
//                "5485c8f9ec6e4b670357d672,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,制动器\n" +
//                "5485c8f9ec6e4b670357d671,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,钻杆\n" +
//                "5485c8f9ec6e4b670357d670,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,导向套\n" +
//                "5485c8f9ec6e4b670357d66f,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,稳杆器\n" +
//                "5485c8f9ec6e4b670357d66e,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,运动部分\n" +
//                "5485c8f9ec6e4b670357d66d,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,支架、底角螺丝\n" +
//                "5485c8f9ec6e4b670357d66c,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,冷却器\n" +
//                "5485c8f9ec6e4b670357d66b,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,进气阀\n" +
//                "5485c8f9ec6e4b670357d66a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,维持阀\n" +
//                "5485c8f9ec6e4b670357d669,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,止回阀\n" +
//                "5485c8f9ec6e4b670357d668,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,空滤、油滤、油分\n" +
//                "5485c8f9ec6e4b670357d667,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,压缩机\n" +
//                "5485c8f9ec6e4b670357d666,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,各管路\n" +
//                "5485c8f9ec6e4b670357d665,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,A型架轴座\n" +
//                "5485c8f9ec6e4b670357d664,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,主传动链条\n" +
//                "5485c8f9ec6e4b670357d663,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,齿条、齿轮\n" +
//                "5485c8f9ec6e4b670357d662,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,主副离合\n" +
//                "5485c8f9ec6e4b670357d661,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,钻架各构件\n" +
//                "5485c8f9ec6e4b670357d660,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,主抱闸\n" +
//                "5485c8f9ec6e4b670357d65f,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,加压离合器\n" +
//                "5485c8f9ec6e4b670357d65e,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,管路、接头\n" +
//                "5485c8f9ec6e4b670357d65d,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,阀泵\n" +
//                "5485c8f9ec6e4b670357d65c,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,固定螺丝\n" +
//                "5485c8f9ec6e4b670357d65b,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,辅空\n" +
//                "5485c8f9ec6e4b670357d65a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,行走机构\n" +
//                "5485c8f9ec6e4b670357d659,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,提升加压机构\n" +
//                "5485c8f9ec6e4b670357d658,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,回转机构及钻具\n" +
//                "5485c8f9ec6e4b670357d657,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,钻架部分\n" +
//                "5485c8f9ec6e4b670357d656,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,主辅空压机\n" +
//                "5485c8f9ec6e4b670357d655,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,液压系统\n" +
//                "5485c8f9ec6e4b670357d654,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,平台与机棚\n" +
//                "5485c8f9ec6e4b670357d653,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,润滑与除尘\n" +
//                "5485c8f9ec6e4b670357d652,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,电气部分\n" +
//                "5485c8f9ec6e4b670357d644,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d63c,主轴\n" +
//                "5485c8f9ec6e4b670357d643,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d63c,上、下架体\n" +
//                "5485c8f9ec6e4b670357d642,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d63c,水平轴\n" +
//                "5485c8f9ec6e4b670357d641,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d63c,偏心套总称\n" +
//                "5485c8f9ec6e4b670357d640,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d63c,工作介质");
        //equip
//        putMessage("equip\n" +
//                "id,orgId,equipModelId,deptId,serialNo,chkptId,chkptName\n" +
//                "5475db7a56b9ac2d464ba7c6,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,14,5475db7a56b9ac2d464ba7c7,13RT45000002\n" +
//                "5475db7a56b9ac2d464ba7c8,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,13,5475db7a56b9ac2d464ba7c9,13RT45000007舱\n" +
//                "5475db7a56b9ac2d464ba7ca,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,15,5475db7a56b9ac2d464ba7cb,14RT45000009\n" +
//                "5475db7a56b9ac2d464ba7cc,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,14,5475db7a56b9ac2d464ba7cd,13RT45000002\n" +
//                "5475db7a56b9ac2d464ba7ce,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,16,5475db7a56b9ac2d464ba7cf,14RT45000010\n" +
//                "5475db7a56b9ac2d464ba7d0,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,15,5475db7a56b9ac2d464ba7d1,14RT45000009\n" +
//                "5475db7a56b9ac2d464ba7d2,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,17,5475db7a56b9ac2d464ba7d3,14RT45000011\n" +
//                "5475db7a56b9ac2d464ba7d4,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,16,5475db7a56b9ac2d464ba7d5,14RT45000010\n" +
//                "5475db7a56b9ac2d464ba7d6,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,18,5475db7a56b9ac2d464ba7d7,14RT45000012\n" +
//                "5475db7a56b9ac2d464ba7d8,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,17,5475db7a56b9ac2d464ba7d9,14RT45000011\n" +
//                "5475db7a56b9ac2d464ba7da,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,19,5475db7a56b9ac2d464ba7db,14RT45000013\n" +
//                "5475db7a56b9ac2d464ba7dc,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,18,5475db7a56b9ac2d464ba7dd,14RT45000012\n" +
//                "5475db7a56b9ac2d464ba7de,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,20,5475db7a56b9ac2d464ba7df,14RT45000014\n" +
//                "5475db7a56b9ac2d464ba7e0,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,19,5475db7a56b9ac2d464ba7e1,14RT45000013\n" +
//                "5475db7a56b9ac2d464ba7e2,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,20,5475db7a56b9ac2d464ba7e3,14RT45000014\n" +
//                "5475db7a56b9ac2d464ba78d,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,8,5475db7a56b9ac2d464ba78e,12RT45000001\n" +
//                "5475db7a56b9ac2d464ba7b2,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,9,5475db7a56b9ac2d464ba7b3,13RT45000003\n" +
//                "5475db7a56b9ac2d464ba7b4,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,8,5475db7a56b9ac2d464ba7b5,12RT45000001\n" +
//                "5475db7a56b9ac2d464ba7b6,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,10,5475db7a56b9ac2d464ba7b7,13RT45000005\n" +
//                "5475db7a56b9ac2d464ba7b8,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,9,5475db7a56b9ac2d464ba7b9,13RT45000003\n" +
//                "5475db7a56b9ac2d464ba7ba,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,11,5475db7a56b9ac2d464ba7bb,13RT45000006\n" +
//                "5475db7a56b9ac2d464ba7bc,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,10,5475db7a56b9ac2d464ba7bd,13RT45000005\n" +
//                "5475db7a56b9ac2d464ba7be,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,12,5475db7a56b9ac2d464ba7bf,13RT45000004\n" +
//                "5475db7a56b9ac2d464ba7c0,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,11,5475db7a56b9ac2d464ba7c1,13RT45000006\n" +
//                "5475db7a56b9ac2d464ba7c2,5475db7956b9ac2d464ba738,5475db7a56b9ac2d464ba76a,5475db7956b9ac2d464ba73a,13,5475db7a56b9ac2d464ba7c3,13RT45000007舱\n" +
//                "5475db7a56b9ac2d464ba7c4,5475db7956b9ac2d464ba739,5475db7a56b9ac2d464ba78f,5475db7956b9ac2d464ba73b,12,5475db7a56b9ac2d464ba7c5,13RT45000004\n" +
//                "5485c8f9ec6e4b670357d680,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,5485c8f4ec6e4b670357d5e1,QMJ4-1,5485c8f9ec6e4b670357d681,溢流型球磨机巡检点\n" +
//                "5485c8f9ec6e4b670357d684,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d645,5485c8f4ec6e4b670357d5e2,YLZJ-1,5485c8f9ec6e4b670357d685,YZ55B型牙轮钻机巡检点\n" +
//                "5485c8f9ec6e4b670357d686,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d633,5485c8f4ec6e4b670357d5e1,中1,5485c8f9ec6e4b670357d687,中碎旋回破碎机巡检点\n" +
//                "5485c8f9ec6e4b670357d688,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d63c,5485c8f4ec6e4b670357d5e1,细1,5485c8f9ec6e4b670357d689,细碎旋回破碎机巡检点\n" +
//                "5485c8f9ec6e4b670357d682,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d5f8,5485c8f4ec6e4b670357d5e1,1,5485c8f9ec6e4b670357d683,XX型皮带机巡检点\n" +
//                "5485c8f9ec6e4b670357d68a,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d627,5485c8f4ec6e4b670357d5e1,粗破,5485c8f9ec6e4b670357d68b,旋回破碎机巡检点\n" +
//                "5485c8f9ec6e4b670357d68c,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,5485c8f4ec6e4b670357d5e1,QMJ4-2,5485c8f9ec6e4b670357d68d,溢流型球磨机巡检点\n" +
//                "5485c8f9ec6e4b670357d68e,5485c8f3ec6e4b670357d5e0,5485c8f9ec6e4b670357d60b,5485c8f4ec6e4b670357d5e1,QMJ4-3,5485c8f9ec6e4b670357d68f,溢流型球磨机巡检点");

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
                int colCount = cols.length;
                int i = 0;
                for (String s : split){
                    i++;
                    if (i < 3) continue;;
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
                                ", DESCRIPTION varchar(100)" +
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
                                ", SERIALNO varchar(20)" +
                                ", CHKPTID char(24)" +
                                ", CHKPTNAME varchar(100)" +
                                ")", type));
                        break;
                    case "subject":
                        stmt.execute(String.format("create table if not exists %s (ID char(24) not null primary key" +
                                ", ORGID char(24)" +
                                ", CHKPTID char(24)" +
                                ", SPOTID char(24)" +
                                ", NAME varchar(100)" +
                                ", ITEMID char(24)" +
                                ", ITEMNAME varchar(100)" +
                                ", ITEMDESC varchar(200)," +
                                ", ITEMSTOPCHECK varchar(10)" +
                                ", ITEMOPTIONAL varchar(10)" +
                                ", CHECKNAME varchar()" +
                                ", CHECKUNIT varchar(20)" +
                                ", CHECKTYPE varchar(10)" +
                                ", RANGEDEF varchar(300)" +
                                ", SERVERITY tinyint(1)" +
                                ", RANGEDESC varchar(100)" +
                                ")", type));
                        break;
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

