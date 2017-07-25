package com.asiainfo.sdtp;

import java.io.InputStream;
import java.util.Properties;

public class Utils {

	private static Properties props=new Properties();;

	public static String getProperties(String key) {

		if (props .isEmpty()) {
			InputStream is = Utils.class.getClassLoader().getResourceAsStream("config.properties");
			try {
				props.load(is);
			} catch (Exception e) {
				e.printStackTrace();
				props = null;
			}

		}
		return props.getProperty(key);
	}
}
