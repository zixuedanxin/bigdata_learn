package com.hy.mykafka;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
public class hello {
    public static void main(String[] args) {
        System.out.printf("hau");
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        System.out.println(new Date());// new Date()为获取当前系统时间
        System.out.println(System.currentTimeMillis()%100000);
        HashMap<String, String> rows=new HashMap<String, String>();
        rows.put("pfrm_nm","andriod");
        rows.put("app_nm","掌上海银");
        rows.put("uuid","uuid"+String.valueOf((System.currentTimeMillis()/1000)));
        rows.put("user_id","user"+String.valueOf(System.currentTimeMillis()%100000));
        rows.put("curr_page_id","pageid"+String.valueOf(System.currentTimeMillis()%100));
        rows.put("cat_id","cat"+String.valueOf(System.currentTimeMillis()%10));
        rows.put("curr_page_tag","curr_page_tag");
        rows.put("to_url","http:"+String.valueOf(System.currentTimeMillis()%1000000));
        rows.put("dmn_nm","web.test");
        rows.put("act_cntnt","test"+String.valueOf(System.currentTimeMillis()%1000));
        rows.put("act_type","a");
        rows.put("crt_dtm",String.valueOf(System.currentTimeMillis()));
        System.out.println(ConsumerConfig.MAX_POLL_RECORDS_CONFIG);
    }
}
