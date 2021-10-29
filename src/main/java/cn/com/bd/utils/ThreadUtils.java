package cn.com.bd.utils;

public class ThreadUtils {
	public static void sleepInMillSeconds(int millSeconds) {
		try {
			if (millSeconds > 0) {
				Thread.sleep(millSeconds);
			}
		} catch (Exception e) {
			
		}
	}
}
