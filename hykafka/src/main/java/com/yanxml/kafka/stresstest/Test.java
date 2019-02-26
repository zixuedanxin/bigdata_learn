package com.yanxml.kafka.stresstest;

public class Test {
	public static void main(String[] args) {
		
//		String eventStr="{\"jsonString\":\"{\"event_handle\": \"1118702\",\"SOURCE_IP\": \"xx.xxx.xx.xxx:18280\",\"AREA\": \"xx中心\",\"date\": \"2018-06-13 13:40:54\",\"status\": \"OPEN\",\"mc_object_class\": \"AAA\",\"mc_parameter\": \"AAA\",\"mc_object\": \"CELL:xx.xxx.xx.xxx:xxx\",\"mc_host_address\": \"xx.xxx.xx.xxx\",\"mc_parameter_value\": \"85.06\",\"severity\": \"MAJOR\",\"mc_tool_class\": \"xxxx_xxx\",\"mc_appname\": \"\",\"updatetime\": \"2017-11-29 16:29:55\",\"msg\": \"xxx使用率为98.27%,告警级别[高]。\",\"xxx\": \"xxx\"}\"}";
//		JsonEventDTO jsonEventDTO = null;
//		jsonEventDTO = JSON.parseObject(eventStr, JsonEventDTO.class);
		int i=0;
		while(true){
			i++;
			System.out.println(i);
			if(i>5){
				break;
			}
		}
	}
}
