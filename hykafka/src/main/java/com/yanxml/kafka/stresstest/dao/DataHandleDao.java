package com.yanxml.kafka.stresstest.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

import com.yanxml.kafka.stresstest.config.DBConfig;
import com.yanxml.kafka.stresstest.util.RegexUtil;

public class DataHandleDao {
	
	public DataSource dataSource = DBConfig.getDataSource();
	
	public int insertAlert(Integer outid,String content){
//		 Connection conn = dataSource.getConnection();
		    int i = 0;
		    String sql = "insert into raw_data (out_id,content) values(?,?)";
		    PreparedStatement pstmt;
		    try {
				Connection conn = dataSource.getConnection();
		        pstmt = (PreparedStatement) conn.prepareStatement(sql);
		        pstmt.setInt(1, outid);

//		        pstmt.setString(1, outid);
		        pstmt.setString(2, content);
		        i = pstmt.executeUpdate();
		        pstmt.close();
		        conn.close();
		    } catch (SQLException e) {
		        e.printStackTrace();
		    }
		return i;
	}
	
	public static void main(String[] args) {
		//[100000{"jsonString":"{\"event_handle\": \"1529915456\",\"SOURCE_IP\": \"xx.xxx.xx.0:xxxx\",\"AREA\": \"xx中心\",\"date\": \"2016-06-25 16:30:56\",\"status\": \"OPEN\",\"mc_object_class\": \"mc_object_class\",\"mc_parameter\": \"ConnectionStatus\",\"mc_object\": \"xx.xxx.xx.xxx\",\"mc_host_address\": \"xxx.xxx.xxx.x\",\"mc_parameter_value\": \"85.06\",\"severity\": \"MAJOR\",\"mc_tool_class\": \"NGMS_BPPM\",\"mc_appname\": \"mc_appname\",\"updatetime\": \"2016-06-25 16:30:56\",\"msg\": \"xxx使用率为67%\",\"xxx\": \"xxx\"}"}]
		String content = "[100000{\"jsonString\"}]";
		
		String out_id_str = (RegexUtil.regex("\\[(.*)\\{", (content), 1));
		Integer out_id = Integer.parseInt(out_id_str);

//		String out_id = (RegexUtil.regex("\\[(.*)\\{", RegexUtil.escapeExprSpecialWord(content), 1));

//		String out_id = Integer.parseInt(RegexUtil.regex("[*{", RegexUtil.escapeExprSpecialWord(content), 1));
		
//		System.out.println(out_id);
		
		// It is ok .
		
		new DataHandleDao().insertAlert(out_id,content);
		
	}
}
