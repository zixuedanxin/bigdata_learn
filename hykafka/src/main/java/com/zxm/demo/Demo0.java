package com.zxm.demo;

public class Demo0 {

	public static void main(String[] args) {

		for (int i = 0; i <= 10; i++) {
			for (int j = 20; j < 25; j++) {
				
				System.out.print(" " + i + ":" + j + " ");
				if(j==22) {
					continue;
				}
				System.out.print(" A ");
			}
		}

	}

}
