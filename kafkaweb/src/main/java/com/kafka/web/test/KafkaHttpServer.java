package com.kafka.web.test;


// import com.alibaba.fastjson.JSONArray;

import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;


public class KafkaHttpServer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHttpServer.class);
    private final Counter statistic = new Counter();
    private static boolean deleteFile = true;
    private static final String back_dir = Configuration.conf.getString("webbase.backup.dir");

    private void error(HttpServerResponse response, String message) {
        response.setStatusCode(500).end(new JsonObject()
                .put("code", 3).put("msg", message)
                .encode());
    }

    private void ok(HttpServerResponse response, String message) {
        response.putHeader("Access-Control-Allow-Origin", "*");
        response.setStatusCode(200).end(new JsonObject()
                .put("code", 0)
                .put("msg", message)
                .encode());
    }

    private void startService(int port) {
        KafkaProducerWrapper sender = new KafkaProducerWrapper();
        Vertx vertx = Vertx.vertx();
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        String authFilePath=System.getProperty("user.dir")+ "/auths.csv";
        Map authMap = readAuth(authFilePath);
        router.route("/logs/:topic").handler(ctx -> {
            String tp = ctx.getBodyAsString();//ctx.request().params().toString();
            //LOG.info(tp);
            String topic = ctx.request().getParam("topic");
            String ip = ctx.request().remoteAddress().host();
            String authip=ip.trim() + topic.trim();
            System.out.println(tp);
            System.out.println(topic);
            ok(ctx.response(), "null data");
//            if (authMap.containsKey(authip) || readAuth(authFilePath).containsKey(authip)) {
//                if (tp.length() > 9) {
//                    try {
//                        JSONObject json = JSONObject.parseObject(tp);
//                        if (json.containsKey("data")) {
//                            json.getJSONArray("data");
//                            String[] messages = new String[1];
//                            messages[0] = tp; //json.getString("data");
//    //                        JSONArray jsonArray = json.getJSONArray("data");
//    //                        String[] messages = new String[jsonArray.size()];
//    //                        for (int i = 0; i < jsonArray.size(); i++) {
//    //                            JSONObject message = jsonArray.getJSONObject(i);
//    //                            // message.put("post_ip", ip);
//    //                            messages[i] = message.toString();
//    //                        }
//                            sendMessages(sender, ctx, topic, messages);
//                            //messages=null;
//                        } else {
//                            LOG.info(tp);
//                            LOG.error("json格式不对，需要{'data':{XXXX}}");
//                            error(ctx.response(),"json格式不对，格式{\"data\":[{rs1},{rs2}]}");
//                        }
//                        //json=null;
//                    } catch (Exception e) {
//                        LOG.info(tp);
//                        LOG.error(e.getMessage());
//                        error(ctx.response(), e.getMessage());
//                    }
//                }
//            }else {
//                LOG.error("无操作权限："+authip);
//                error(ctx.response(), "no auth to send message for:"+authip);
//            }

        });

        server.requestHandler(router::accept).listen(port, result -> {
            if (result.succeeded()) {
                LOG.info("listen on port:{0}", String.valueOf(port));
                this.statistic.start(vertx);
            } else {
                LOG.error(result.cause());
                vertx.close();
            }
        });
        //如果你需要在你的程序关闭前采取什么措施，那么关闭钩子（shutdown hook）是很有用的，类似finally
        Runtime.getRuntime().addShutdownHook(new Thread(sender::close));
    }

    private void sendMessages(KafkaProducerWrapper sender, RoutingContext ctx, String topic, String[] messages) {
        AtomicInteger counter = new AtomicInteger(0);
        for (String message : messages) {
            if (message == null || "".equals(message)) {
                ok(ctx.response(), "null data");
                continue;
            }
            //topic, message, callback，以匿名函数的形式实现接口中的onCompletion函数
            sender.send(topic, message, (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn(exception);
                    String msg = new JsonObject()
                            .put("error", exception.getMessage())
                            .put("commit", counter.get())
                            .encode();
                    error(ctx.response(), msg);
                    cacheLocal(message, back_dir +"/"+ topic + ".txt");
                    LOG.info("连接kafka失败，写入cache缓存目录以备份数据");
                } else {
                    statistic.messages.incrementAndGet();  // Counter
                    statistic.bytes.addAndGet(message.length());
                    if (counter.incrementAndGet() == messages.length) {
                        ok(ctx.response(), "Success");
                    }
                }
            });
        }
    }

    /**
     * 将发送到kafka失败的消息缓存到本地
     *
     * @param message   message
     * @param cachePath cachePath
     */
    private void cacheLocal(String message, String cachePath) {

        BufferedWriter bw = null;
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(cachePath, true);
            bw = new BufferedWriter(fileWriter);
            bw.write(message);
            bw.newLine();
            bw.flush();
            bw.close();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                    bw = null;
                }
            }
            if (fileWriter !=null) {
                try {
                    fileWriter.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                    fileWriter = null;
                }
            }
        }
    }

    /**
     * 发送缓存数据到kafka，发送成功，删除缓存数据，失败过10分钟重试
     *
     * @param path 保存缓存数据的[目录]
     */
    private static void sendToKafka(String path) {
        String message;
        KafkaProducerWrapper sender = new KafkaProducerWrapper();
        File file = new File(path);
        if (file.isDirectory()) {
            String[] fileList = file.list();
            if (fileList != null && fileList.length != 0) {
                LOG.info("正在将缓存目录中的备份数据发送到kafka中...");
                for (String str : fileList) {
                    LOG.info(str);
                    String topic = str.split("\\.")[0];
                    String filepath =path +"/"+ str;
                    BufferedReader reader =null;
                    try {
                        reader = new BufferedReader(new FileReader(path +"/"+ str));
                        while ((message = reader.readLine()) != null) {
                            sender.send(topic, message, (metadata, exception) -> {
                                if (metadata != null) {
                                    LOG.info("缓存的备份数据正在一条一条的插入kafka中");
                                } else {
                                    //程序错误重新运行
                                    LOG.error("kafka连接异常为：===> 10分钟后会自动重试，" + exception.getMessage(), exception);
                                    deleteFile = false;
                                }
                            });
                        }
                        if (deleteFile) {
                            LOG.info("开始删除已经插入到kafka中的缓存备份数据"+filepath);
                            deleteFile(filepath);
                            LOG.info("删除完毕！");
                        }
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        if(reader!=null) {
                            try {
                                reader.close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                                reader = null;
                            }
                        }
                    }
                }
            } else {
                LOG.info("缓存目录中没有备份文件");
            }
        }
        sender.close();
        file=null;
    }

    private static void deleteFile(String filePath) {
        File file = new File(filePath);
        boolean del = file.delete();
        if (del) {
            LOG.info("成功删除备份数据" + filePath );
        }else{
            LOG.info("删除文件失败" + filePath );
        }
        file=null;
    }

    private static Map<String,String> readAuth(String path) {
        Map<String,String> map = new HashMap<>();
        BufferedReader reader=null;
        try {
            reader = new BufferedReader(new FileReader(path));//你的文件名
            reader.readLine();//第一行信息，为标题信息，不用,如果需要，注释掉
            String line;
            while ((line = reader.readLine()) != null) {
                String item[] = line.split(",");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
                map.put(item[0].trim() + item[1].trim(), "true");
            }
            reader.close();
            return map;
        } catch (Exception e) {
            e.printStackTrace();
            if (reader!=null){
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                    reader=null;
                }
            }
            return map;
        }
    }


    public static void main(String[] args)// throws Exception
    {
        // System.getProperty("user.dir");
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                // appkeys.addAll(getAppkeys());
                LOG.info("同步备份文件上的数据(每隔60分钟)");
                sendToKafka(Configuration.conf.getString("webbase.backup.dir"));
            }
        }, 0L, 60 * 60 * 1000L);

        try {
            int port = Configuration.conf.getInt("server.port");
            KafkaHttpServer front = new KafkaHttpServer();
            front.startService(port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}