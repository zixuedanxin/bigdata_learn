package com.kafka.web;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Created by reynold on 16-6-23.
 *
 */
public class Configuration {
    public static  final Config conf= ConfigFactory.load();
}
