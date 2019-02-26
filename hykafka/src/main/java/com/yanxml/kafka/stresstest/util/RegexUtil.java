package com.yanxml.kafka.stresstest.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class RegexUtil {
	
	/** 
	 * 转义正则特殊字符 （$()*+.[]?\^{},|） 
	 *  
	 * @param keyword 
	 * @return 
	 */  
	public static String escapeExprSpecialWord(String keyword) {  
	    if (!StringUtils.isEmpty(keyword)) {  
	        String[] fbsArr = { "\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|" };  
	        for (String key : fbsArr) {  
	            if (keyword.contains(key)) {  
	                keyword = keyword.replace(key, "\\" + key);  
	            }  
	        }  
	    }  
	    return keyword;  
	}
	/**
	 * 正则匹配
	 * @param regex 正则表达式
	 * @param data 待匹配字段
	 * @param index 取表达式的分组
	 * @return
	 */
	public static String regex (String regex ,String data, Integer  index) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher =  pattern.matcher(data);
		if (matcher.find() && 
				(!StringUtils.isEmpty(matcher.group(index)))
		) {
			return  matcher.group(index);
		}
		return null;
	}
}

