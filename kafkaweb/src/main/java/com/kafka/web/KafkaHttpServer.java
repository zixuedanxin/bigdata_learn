package com.kafka.web;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.*;
import java.sql.*;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by reynold on 16-6-22.
 *
 */

public class KafkaHttpServer {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHttpServer.class);
    private final Counter statistic = new Counter();
    private static final String DBDRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = Configuration.conf.getString("mysql.url");
    private static final String USER = Configuration.conf.getString("mysql.user");
    private static final String PASSWORD = Configuration.conf.getString("mysql.password");
    private static HashSet<String> appkeys = new HashSet<>();
    private static boolean deleteFile = true;

    private void error(HttpServerResponse response, String message) {
        response.setStatusCode(500).end(new JsonObject()
                .put("code", 3)
                .put("msg", message)
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
        router.route("/mininfo/logs").handler(ctx -> {
            System.out.println(ctx.getBodyAsJson());
            try {
                JsonArray array = ctx.getBodyAsJsonArray();
                String[] messages = new String[array.size()];
                System.out.println("开始接收");
                for (int i = 0; i < array.size(); i++) {
                    JsonObject message = array.getJsonObject(i);
                    message.put("ip", ctx.request().remoteAddress().host());
                    if (!message.containsKey("timestamp")) {
                        message.put("timestamp", Instant.now().toString());
                    }
                    messages[i] = array.getJsonObject(i).encode();
                    System.out.println(messages[i]);
                }
                sendMessages(sender, ctx, "app_log", messages);
            } catch (Exception e) {
                error(ctx.response(), e.getMessage());
            }

        });
        router.routeWithRegex("/mininfo/v1/logs/[^/]+").handler(routingContext -> {
            String path = routingContext.request().path();
            String topic = path.substring(path.lastIndexOf("/") + 1);
            LOG.info("现在处理的topic(appkey)为：" + topic);
            if (appkeys.contains(topic)) {
                LOG.info("经过验证，该topic(appkey)有效");
                String[] messages = routingContext.getBodyAsString().split("\n");
                //用于执行阻塞任务(有序执行和无序执行)，默认顺序执行提交的阻塞任务
                vertx.executeBlocking(future -> {
                    sendMessages(sender, routingContext, topic, messages);
                    future.complete();
                }, result -> {
                });
            } else {
                LOG.info("您的topic(appkey)还没有配置，请在mysql中配置先");
                error(routingContext.response(), "please configurate " + topic + "(appkey) in Mysql first! After 10mins it`ll take action");
            }
        });
        router.route("/mininfo/v1/ip").handler(ctx -> {
            LOG.info("x-real-for" + ctx.request().getHeader("x-real-for"));
            LOG.info("x-forwarded-for" + ctx.request().getHeader("x-forwarded-for"));
            ok(ctx.response(), ctx.request().getHeader("x-forwarded-for"));
        });
        router.route("/*").handler(ctx -> error(ctx.response(), "wrong! check your path..."));
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
                ok(ctx.response(), "Success");
                continue;
            }
            //将ip增加到数据的ip字段
            JSONObject jsonObject = JSON.parseObject(message);
            if (jsonObject.get("ip") == null) {
                LOG.info("正在增加ip字段");
                String ip;
                String header = ctx.request().getHeader("x-forwarded-for");
                if (!(header == null || header.trim().length() == 0 || header.trim().equals("null"))) {
                    ip = header.split(",")[0];
                } else {
                    ip = ctx.request().remoteAddress().host();
                }
                jsonObject.put("ip", ip);
                LOG.info("ip增加成功");
            }
            //topic, message, callback，以匿名函数的形式实现接口中的onCompletion函数
            sender.send(topic, jsonObject.toString(), (metadata, exception) -> {
                if (exception != null) {
                    LOG.warn(exception);
                    String msg = new JsonObject()
                            .put("error", exception.getMessage())
                            .put("commit", counter.get())
                            .encode();
                    error(ctx.response(), msg);
                    cacheLocal(jsonObject.toString(), "/home/yuhui/httpkafka/data_bak/" + topic + ".txt");
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
        try {
            FileWriter fileWriter = new FileWriter(cachePath, true);
            BufferedWriter bw = new BufferedWriter(fileWriter);
            bw.write(message);
            bw.newLine();
            bw.flush();
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
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
                    String topic = str.split("\\.")[0];
                    try {
                        BufferedReader reader = new BufferedReader(new FileReader(path + str));
                        while ((message = reader.readLine()) != null) {
                            sender.send(topic, message, (metadata, exception) -> {
                                if (metadata != null) {
                                    LOG.info("缓存的备份数据正在一条一条的插入kafka中");
                                } else {
                                    //程序错误重新运行
//                                    exception.printStackTrace();
                                    LOG.error("kafka连接异常为：===> 10分钟后会自动重试，" + exception.getMessage(), exception);
                                    deleteFile = false;
                                }
                            });
                        }
                        if (deleteFile) {
                            LOG.info("开始删除已经插入到kafka中的缓存备份数据");
                            deleteFile(path, topic);
                            LOG.info("删除完毕！");
                        }
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            } else {
                LOG.info("缓存目录中没有备份文件");
            }
        }
    }

    private static void deleteFile(String path, String appkey) {
        String appkeyPath = path + "/" + appkey + ".txt";
        File file = new File(appkeyPath);
        file.delete();
        LOG.info("成功删除appkey为" + appkey + "的缓存数据");
    }

    private static Set<String> getAppkeys() {
        Set<String> appkeys = new HashSet<>();
        String sql = "select appkey from config_table";
        try {
            Class.forName(DBDRIVER);
            Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
            PreparedStatement ps = conn.prepareStatement(sql);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                appkeys.add(rs.getString(1));
            }
            rs.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return appkeys;
    }

    public static void main(String[] args) throws Exception {
//        Timer timer = new Timer();
//        timer.schedule(new TimerTask() {
//            @Override
//            public void run() {
//                appkeys.addAll(getAppkeys());
//                LOG.info("同步完数据库中的appkey(每隔十分钟)");
//                sendToKafka("/home/leixingzhi7/httpkafka/data_bak/");
////                sendToKafka("C:\\Dell\\UpdatePackage\\log");
//            }
//        }, 0L, 10 * 60 * 1000L);

        try {
            int port = Configuration.conf.getInt("server.port");
            KafkaHttpServer front = new KafkaHttpServer();
            front.startService(port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}