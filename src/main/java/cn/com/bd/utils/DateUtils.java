package cn.com.bd.utils;

import org.apache.commons.lang.StringUtils;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.Format;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtils {
	private static final String DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss";
 
	public static int getCurrentDate() {
		return Integer.parseInt(getCurrentDate("yyyyMMdd")); 
	}
	
	public static Date parse(String dateStr, String dateFormat) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.parse(dateStr);
	}
	
	public static int daysDiff(Date sDate, Date eDate, String dateFormat) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date startDate = sdf.parse(sdf.format(sDate));
		Date endDate = sdf.parse(sdf.format(eDate));
		Calendar cal = Calendar.getInstance();
		cal.setTime(startDate);
		long startTime = cal.getTimeInMillis();
		cal.setTime(endDate);
		long endTime = cal.getTimeInMillis();
		long daysDiff = (endTime - startTime) / (1000 * 3600 * 24);
		return Integer.parseInt(String.valueOf(daysDiff));
	}
	
	public static boolean before(String first, String second) throws Exception {
		return before(first, second, DEFAULT_PATTERN);
	}
	
	public static boolean before(String first, String second, String format) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date firstDate = sdf.parse(first);
		Date secondDate = sdf.parse(second);
		return before(firstDate, secondDate);
	}
	
	public static boolean before(Date firstDate, Date secondDate) throws Exception {
		Calendar firstCal = Calendar.getInstance();
		firstCal.setTime(firstDate);
		Calendar secondCal = Calendar.getInstance();
		secondCal.setTime(secondDate);
		return firstCal.before(secondCal);
	}
	
	public static boolean after(String first, String second) throws Exception {
		return after(first, second, DEFAULT_PATTERN);
	}
	
	public static boolean after(String first, String second, String format) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date firstDate = sdf.parse(first);
		Date secondDate = sdf.parse(second);
		return after(firstDate, secondDate);
	}
	
	public static boolean after(Date firstDate, Date secondDate) throws Exception {
		Calendar firstCal = Calendar.getInstance();
		firstCal.setTime(firstDate);
		Calendar secondCal = Calendar.getInstance();
		secondCal.setTime(secondDate);
		return firstCal.after(secondCal);
	}
	
	/**
	 * 
	 * 日期的算术操作，可以增加或者减少，可以某一部分进行操作 year--年 month-月 1-12 day-天 1-31 hour -小时 0-23
	 * minute 分钟 0-59 second 秒 0-59 millisecond 毫秒 显示格式，可以任意组合
	 * 
	 * G Era designator Text AD y Year Year 1996; 96 M Month in year Month July;
	 * Jul; 07 w Week in year Number 27 W Week in month Number 2 D Day in year
	 * Number 189 d Day in month Number 10 F Day of week in month Number 2 E Day
	 * in week Text Tuesday; Tue a Am/pm marker Text PM H Hour in day (0-23)
	 * Number 0 k Hour in day (1-24) Number 24 K Hour in am/pm (0-11) Number 0 h
	 * Hour in am/pm (1-12) Number 12 m Minute in hour Number 30 s Second in
	 * minute Number 55 S Millisecond Number 978 z Time zone General time zone
	 * Pacific Standard Time; PST; GMT-08:00 Z Time zone RFC 822 time zone -0800
	 * 
	 * @version: V1.0
	 * 
	 * @param srcDate
	 * @param srcFormat
	 * @param destFormat
	 * @param operType
	 * @param operValue
	 * @return
	 */
	public static String evalTime(String srcDate, String srcFormat,
			String destFormat, String operType, int operValue) {
		if (srcDate == null || srcDate.equals(""))
			return "";
		if (srcFormat == null || srcFormat.equals(""))
			srcFormat = "yyyy-MM-dd";
		if (destFormat == null || destFormat.equals(""))
			destFormat = "yyyy-MM-dd";
		if (operType == null || operType.equals(""))
			operType = "day";

		// Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("AST"));
		Calendar cal = Calendar.getInstance();
		Date currentTime = null;
		try {
			currentTime = (Date) new SimpleDateFormat(srcFormat)
					.parse(srcDate);
		} catch (ParseException e) {
			System.err.println(""+e);
			currentTime = new Date();
		}
		cal.setTime(currentTime);
		if (operType.equals("year")) {
			cal.add(Calendar.YEAR, operValue);
		} else if (operType.equals("month")) {
			cal.add(Calendar.MONTH, operValue);
		} else if (operType.equals("day")) {
			cal.add(Calendar.DAY_OF_MONTH, operValue);
		} else if (operType.equals("hour")) {
			cal.add(Calendar.HOUR_OF_DAY, operValue);
		} else if (operType.equals("minute")) {
			cal.add(Calendar.MINUTE, operValue);
		} else if (operType.equals("second")) {
			cal.add(Calendar.SECOND, operValue);
		} else if (operType.equals("millisecond")) {
			cal.add(Calendar.MILLISECOND, operValue);
		}
		String curDay = new SimpleDateFormat(destFormat).format(cal
				.getTime());
		return curDay;
	}

	/**
	 *
	 * 获得当前时间 转换为时间戳类型
	 *
	 * @version: V1.0
	 *
	 * @return
	 * @throws ParseException
	 */
	public static Timestamp getCurTime2Timestamp() throws ParseException {
		Timestamp t = new Timestamp(System.currentTimeMillis());
		String d2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(t);
		Timestamp.valueOf(d2);
		return t;
	}

	/**
	 *
	 * 日期格式转化
	 *
	 * @version: V1.0
	 *
	 * @param srcDate
	 *            源日期字符串
	 * @param srcFormat
	 *            源日期格式，默认为yyyy-MM-dd
	 * @param destFormat
	 *            目的日期格式，默认为yyyy-MM-dd
	 * @return 转换格式后的日期字符串 或 空字符串
	 */
	public static String dateFormat(String srcDate, String srcFormat,
			String destFormat) {
		if (srcFormat == null || srcFormat.equals(""))
			srcFormat = "yyyy-MM-dd";
		if (destFormat == null || destFormat.equals(""))
			destFormat = "yyyy-MM-dd";
		if (srcDate == null || srcDate.equals(""))
			return "";

		Calendar cal = Calendar.getInstance();
		Date currentTime = null;
		try {
			currentTime = (Date) new SimpleDateFormat(srcFormat)
					.parse(srcDate);
		} catch (ParseException e) {
			System.err.println(""+e);
			currentTime = new Date();
		}

		cal.setTime(currentTime);
		String curDay = new SimpleDateFormat(destFormat).format(cal
				.getTime());

		return curDay;
	}

	/**
	 *
	 * 日期格式转化
	 *
	 * @version: V1.0
	 *
	 * @param srcDate
	 *            源日期字符串
	 * @param destFormat
	 *            目的日期格式，默认为yyyy-MM-dd
	 * @return 转换格式后的日期字符串 或 空字符串
	 */
	public static String dateFormat(Date srcDate, String destFormat) {
		if (srcDate == null)
			return "";
		if (destFormat == null || destFormat.equals(""))
			destFormat = "yyyy-MM-dd";
		String curDay = new SimpleDateFormat(destFormat)
				.format(srcDate);
		return curDay;
	}

	public static String dateFormat(long date, String format) {
		return dateFormat(new Date(date), format);
	}

	public static String dateFormat(long date) {
		return dateFormat(date, "yyyy-MM-dd HH:mm:ss");
	}

	/**
	 *
	 * 字符串日期转化为java.util.Date
	 *
	 * @version: V1.0
	 *
	 * @param srcDate
	 *            源日期字符串
	 * @param srcFormat
	 *            源日期格式，默认为yyyy-MM-dd
	 * @return 转换后的Date对象， 如果srcDate为空或异常，则返回当前日期
	 */
	public static Date StringToDate(String srcDate, String srcFormat) {
		if (srcDate == null || srcDate.equals(""))
			return new Date();

		if (srcFormat == null || srcFormat.equals(""))
			srcFormat = "yyyy-MM-dd";

		Calendar cal = Calendar.getInstance();
		Date currentTime = null;

		try {
			currentTime = (Date) new SimpleDateFormat(srcFormat)
					.parse(srcDate);
		} catch (ParseException e) {
			System.err.println("" + e);
			currentTime = new Date();
		}

		cal.setTime(currentTime);

		return cal.getTime();
	}

	public static Date parseDate(String dateStr, String srcFormat) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat(srcFormat);
		return sdf.parse(dateStr);
	}

	/**
	 *
	 * 将字符串格式化为‘yyyy-MM-dd HH:mm:ss’格式的java.util.Date
	 *
	 * @author xudongdong
	 * @date 2009-3-26 下午01:36:29
	 * @version: V1.0
	 *
	 * @param srcDate
	 *            源日期字符串
	 * @return 转换后的Date对象
	 */
	public static Date StringToDate(String srcDate) {
		Date d = null;
		SimpleDateFormat sdf = new SimpleDateFormat(DEFAULT_PATTERN);

		try {
			d = sdf.parse(srcDate);
		} catch (Exception e) {
			System.err.println(""+e);
		}

		return d;
	}

	/**
	 * 比较两个日期的大小
	 *
	 * @version: V1.0
	 *
	 * @param d1
	 * @param d2
	 * @return 如果d1大于d2，返回true，否则返回false
	 */
	public static boolean compare(Date d1, Date d2) {
		if (d1.after(d2)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 比较两个日期的大小
	 *
	 * @version: V1.0
	 *
	 * @param d1
	 * @param d2
	 * @return 如果d1大于d2，返回true，否则返回false
	 */
	public static boolean compare(String first, String second, String dateFormat) throws Exception {
		Date d1 = parseDate(first, dateFormat);
		Date d2 = parseDate(second, dateFormat);
		return compare(d1, d2);
	}

	/**
	 *
	 * 将指定的日期转换成字符串
	 *
	 * @version: V1.0
	 *
	 * @param aDteValue
	 *            要转换的日期
	 * @param aFmtDate
	 *            转换后日期的格式，默认为yyyy-MM-dd
	 * @return 转换之后的字符串，如果异常，则返回null
	 */
	public static String dateToStr(Date aDteValue, String aFmtDate) {
		String strRtn = null;

		if (aFmtDate.length() == 0) {
			aFmtDate = "yyyy/MM/dd";
		}
		Format fmtDate = new SimpleDateFormat(aFmtDate);
		try {
			strRtn = fmtDate.format(aDteValue);
		} catch (Exception e) {
			System.err.println(""+e);
		}

		return strRtn;
	}

	/**
	 * 名称：strToDate 功能：将指定的字符串转换成日期 输入：aStrValue: 要转换的字符串; aFmtDate: 转换日期的格式,
	 * 默认为:"yyyy/MM/dd" aDteRtn: 转换后的日期 输出： 返回：TRUE: 是正确的日期格式; FALSE: 是错误的日期格式
	 */
	/**
	 *
	 * 将指定的字符串转换成日期
	 *
	 * @version: V1.0
	 *
	 * @param aStrValue
	 *            要转换的字符串
	 * @param aFmtDate
	 *            转换日期的格式，默认为yyyy-MM-dd
	 * @param aDteRtn
	 *            转换之后的字符串
	 * @return true-是正确的日期格式; false-是错误的日期格式
	 */
	public static boolean strToDate(String aStrValue, String aFmtDate,
			Date aDteRtn) {
		if (aFmtDate.length() == 0) {
			aFmtDate = "yyyy/MM/dd";
		}

		SimpleDateFormat fmtDate = new SimpleDateFormat(aFmtDate);

		try {
			aDteRtn.setTime(fmtDate.parse(aStrValue).getTime());
		} catch (Exception e) {
			System.err.println(""+e);
			return false;
		}

		return true;
	}

	/**
	 *
	 * 将指定时间转为字符串格式（字符串格式：yyyy-MM-dd HH:mm:ss）
	 *
	 * @version: V1.0
	 *
	 * @param date
	 *            被转换的时间对象
	 * @return 转换后的字符串
	 */
	public static String date2String(Date date) {
		SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return sf.format(date);
	}

	/**
	 *
	 * 将指定时间转为指定字符串格式
	 *
	 * @version: V1.0
	 *
	 * @param date
	 *            被转换的时间对象
	 * @param format
	 *            转换后格式串
	 * @return 转换后的字符串
	 */
	public static String date2Str(Date date, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}

	/**
	 *
	 * 将当前时间转为字符串格式（字符串格式：yyyy-MM-dd HH:mm:ss）
	 *
	 * @version: V1.0
	 *
	 * @return
	 */
	public static String currentTime() {
		return dateFormat(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss");
	}

	/**
	 *
	 * java.util.Date转化为java.sql.Date
	 *
	 * @version: V1.0
	 *
	 * @param date
	 *            被转换的日期对象
	 * @return 转换后的日期对象, 若入参为null，则返回null
	 */
	public static java.sql.Date parseSqlDate(Date date) {
		if (date == null)
			return null;
		return new java.sql.Date(date.getTime());
	}

	/**
	 *
	 * java.util.Date转化为java.sql.Timestamp
	 *
	 * @version: V1.0
	 *
	 * @param date
	 *            被转换的日期对象
	 * @return 转换后的时间戳对象, 若入参为null，则返回null
	 */
	public static Timestamp parseTimestamp(Date date) {
		if (date == null)
			return null;

		long t = date.getTime();

		return new Timestamp(t);
	}

	/**
	 *
	 * 日期相减(date-date1)
	 *
	 * @version: V1.0
	 *
	 * @param date
	 * @param date1
	 * @return 返回相减后的日期毫秒数，若入参为null，则返回0
	 */
	public static long diffDate(Date date, Date date1) {
		if (date == null)
			return 0;
		if (date1 == null)
			return 0;

		return date.getTime() - date1.getTime();
	}

	/**
	 * 
	 * 针对年月取出最大的天数
	 * 
	 * @version: V1.0
	 * 
	 * @param yearmonth
	 * @return
	 */
	public static String getMaxDay(String yearmonth) {

		/*
		 * String day=""; int year=0; int month=0;
		 * 
		 * month=Integer.parseInt(yearmonth.substring(4));
		 * year=Integer.parseInt(yearmonth.substring(0, 4));
		 * 
		 * boolean isLeap=isLeapyear(year); if(isLeap==true){
		 * 
		 * switch(month){ case 1: case 3: case 5: case 7:case 8:case 10:case
		 * 12:day="31"; break; case 2:day="29"; break; case 4:case 6:case 9:case
		 * 11:day="30"; } }else{ switch(month){ case 1: case 3: case 5: case
		 * 7:case 8:case 10:case 12:day="31"; break; case 2:day="28"; break;
		 * case 4:case 6:case 9:case 11:day="30"; } }
		 * 
		 * return day;
		 */
		String tmp = evalTime(yearmonth, "yyyyMM", "yyyyMM", "month", 1);
		String tmp2 = tmp + "01";
		String tmp3 = evalTime(tmp2, "yyyyMMdd", "dd", "day", -1);
		return tmp3;
	}

	/**
	 * 
	 * 针对下月月初日期
	 * 
	 * @version: V1.0
	 * 
	 * @return
	 * 
	 */
	public static String getNextBeginDay() {
		Calendar cal = Calendar.getInstance();
		@SuppressWarnings("unused")
		String endDate_year = String.valueOf(cal.get(Calendar.YEAR));
		@SuppressWarnings("unused")
		String endDate_month = "";
		if (cal.get(Calendar.MONTH) + 1 < 10) {
			endDate_month = "0" + (cal.get(Calendar.MONTH) + 1);
		} else {
			endDate_month = String.valueOf(cal.get(Calendar.MONTH) + 1);
		}
		@SuppressWarnings("unused")
		String endDate_day = String.valueOf(cal
				.getActualMaximum(Calendar.DAY_OF_MONTH));
		String effDate_year = "";
		String effDate_month = "";
		// 当月为12月时，下月应该为下一年的1月，下个月的年份应该加1
		if (cal.get(Calendar.MONTH) + 1 == 12) {
			effDate_year = String.valueOf(cal.get(Calendar.YEAR) + 1);
			effDate_month = "01";
		} else {
			effDate_year = String.valueOf(cal.get(Calendar.YEAR));
			// 当下月为小于10月时，应该为下月的前面加0，如02到09
			if (cal.get(Calendar.MONTH) + 2 < 10) {
				effDate_month = "0" + (cal.get(Calendar.MONTH) + 2);
			} else {
				effDate_month = String.valueOf(cal.get(Calendar.MONTH) + 2);
			}
		}

		String effDate = effDate_year + effDate_month + "01";// 更改后套餐生效日期
		return effDate;
	}

	/**
	 * 
	 * 判断是否是闰年
	 * 
	 * @version: V1.0
	 * 
	 * @param year
	 * @return true-year是闰年，false-year不是闰年
	 */
	public boolean isLeapyear(int year) {

		if (year % 4 == 0 && year % 100 != 0 || year % 400 == 0) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 
	 * 获得当前月份 转换为Long类型
	 * 
	 * @version: V1.0
	 * 
	 * @return 当前月份值
	 * @throws ParseException
	 */
	public static Long getCurMonth2Long() throws ParseException {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("MM");
		// t_log_echn_servinvoke.setTime_range(Long.valueOf(sdf.format(date)));
		return Long.parseLong(sdf.format(date));
	}

	/**
	 * 
	 * 比较当前时间是否在这两个时间点之间
	 * 
	 * @version: V1.0
	 * 
	 * @param time1
	 *            起始时间串
	 * @param time2
	 *            终止时间串
	 * @return true-当前时间在tiem1与time2之间，false-当前时间不在tiem1与time2之间
	 */
	public static boolean checkTimeEarly(String time1, String time2) {
		Calendar calendar = Calendar.getInstance();
		Date date1 = calendar.getTime();
		Date date11 = DateUtils.StringToDate(DateUtils.dateToStr(date1,
				"yyyy-MM-dd")
				+ " " + time1, DEFAULT_PATTERN);// 起始时间
		Calendar c = Calendar.getInstance();
		/* c.add(Calendar.DATE, 1); */
		Date date2 = c.getTime();
		Date date22 = DateUtils.StringToDate(DateUtils.dateToStr(date2,
				"yyyy-MM-dd")
				+ " " + time2, DEFAULT_PATTERN);// 终止时间
		Calendar scalendar = Calendar.getInstance();
		scalendar.setTime(date11);// 起始时间
		Calendar ecalendar = Calendar.getInstance();
		ecalendar.setTime(date22);// 终止时间
		Calendar calendarnow = Calendar.getInstance();
		// modify by liuxw for 跨天问题
		if (calendarnow.after(scalendar) || calendarnow.before(ecalendar)) {
			return true;
		} else {
			return false;
		}

	}

	/**
	 * 
	 * 比较时间是否在这两个时间点之间 added by liuxw
	 * 
	 * @version: V1.0
	 * 
	 * @param time1
	 *            起始时间串 必须是yyyy-MM-dd HH:mm:ss格式
	 * @param time2
	 *            终止时间串 必须是yyyy-MM-dd HH:mm:ss格式
	 * @return true-当前时间在tiem1与time2之间，false-当前时间不在tiem1与time2之间
	 */
	public static boolean checkTimeBetween(String time1, String time2) {
		Date beginTime = DateUtils.StringToDate(time1);
		Date endTime = DateUtils.StringToDate(time2);
		Calendar scalendar = Calendar.getInstance();
		scalendar.setTime(beginTime);
		System.out.println(scalendar);
		Calendar ecalendar = Calendar.getInstance();
		ecalendar.setTime(endTime);// 终止时间
		System.out.println(ecalendar);
		Calendar calendarnow = Calendar.getInstance();
		System.out.println(calendarnow);
		if (calendarnow.after(scalendar) && calendarnow.before(ecalendar)) {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * 
	 * 判断日期date1是否失效
	 * 
	 * @version: V1.0
	 * 
	 * @param date1
	 *            失效时间
	 * @param date2
	 *            当前时间
	 * @param delay
	 *            失效阀值（天）
	 * @return ture-已失效， false-未失效
	 */
	public boolean toRemind(Date date1, Date date2, int delay) {

		Long time = DateUtils.diffDate(date1, date2);
		if (delay > time / (60 * 60 * 24 * 1000)) {
			return false;// 未失效
		} else {
			return true;// 即将失效，提醒
		}
	}

	/**
	 * 
	 * 获得上一个月的月份
	 * 
	 * @version: V1.0
	 * 
	 * @param cal
	 * @return 上月月份对象Calendar
	 */
	public static Calendar getPreviousMonth(Calendar cal) {
		// String str = "";
		// SimpleDateFormat sdf=new SimpleDateFormat("yyyyMM");

		if (cal == null) {
			cal = Calendar.getInstance();
		}
		cal.add(Calendar.MONTH, -1);// 减一个月，变为下月的1号
		return cal;
	}

	/**
	 * 
	 * 根据日期类型更改时间
	 * 
	 * @version: V1.0
	 * 
	 * @param date
	 *            源日期对象
	 * @param type
	 *            date, year, month
	 * @param value
	 *            增加值
	 * @return 更改后的日期对象
	 */
	public static Date changeDate(Date date, String type, int value) {
		// Calendar calendar = Calendar.getInstance(TimeZone.getDefault());
		Calendar calendar = new GregorianCalendar();
		calendar.setTime(date);
		// Calendar calendar = Calendar.
		if (type.equals("month")) {
			calendar.set(Calendar.MONTH, calendar.get(Calendar.MONTH) + value);
		} else if (type.equals("date")) {
			calendar.set(Calendar.DATE, calendar.get(Calendar.DATE) + value);
		} else if (type.endsWith("year")) {
			calendar.set(Calendar.YEAR, calendar.get(Calendar.YEAR) + value);
		}
		return calendar.getTime();
	}

	/**
	 * 
	 * 月份相减
	 * 
	 * @version: V1.0
	 * 
	 * @param startDate
	 *            开始时间
	 * @param enDate
	 *            结束时间
	 * @return 相差月数
	 */
	public static int divMonth(Date startDate, Date enDate) {
		Calendar startCalendar = Calendar.getInstance();
		startCalendar.setTime(startDate);

		Calendar endCalendar = Calendar.getInstance();
		endCalendar.setTime(enDate);

		int yearDiv = startCalendar.get(Calendar.YEAR)
				- endCalendar.get(Calendar.YEAR);

		int monthDiv = startCalendar.get(Calendar.MONTH)
				- endCalendar.get(Calendar.MONTH);

		return monthDiv + yearDiv * 12;
	}

	/**
	 * 返回日期对应是星期几
	 * @param date
	 * @return
	 */
	public static String displayDayOfWeek(Date date) {
		String result = "";
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
		switch (dayOfWeek) {
		case 1:
			result="星期日";
			break;
		case 2:
			result="星期一";
			break;
		case 3:
			result="星期二";
			break;
		case 4:
			result="星期三";
			break;
		case 5:
			result="星期四";
			break;
		case 6:
			result="星期五";
			break;
		case 7:
			result="星期六";
			break;
		}
		return result;
	}
	/**
	 * 字符串格式的日期转成long类型日期格式
	 * @param date
	 * @return
	 * @throws ParseException
	 */
	public static long stringDateParseInt(String date) throws ParseException{
		DateFormat format2 = new SimpleDateFormat("yyyyMMddHHmmss");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date dt1 = df.parse(date);
		String a = format2.format(dt1);
		return Long.valueOf(a);
	}
	/**
	  * @desc 获取当前时间
	  * @return 时间
	  */
	public static String genDateStr(String format) {
		
		if (StringUtils.isEmpty(format)) {
			format ="yyyyMMddHHmmssSSSS";
		}
		Date date = new Date();
		DateFormat df = new SimpleDateFormat(format);
		return df.format(date);
	}
	
	/**
	 * 获取指定月份第一天/最后一天的字串
	 * @param date	指定日期字符串，例如：201402
	 * @param getLastDay 是否获取最后一天，true-返回最后一天字串， false-返回第一天字串
	 * @return	
	 */
	public static String getFirstOrLastDayStr(String date, boolean getLastDay){
		if( !getLastDay ){
			return date + "01";
		}
		Calendar cal = Calendar.getInstance();
		//设置当前年月日：x年x月1日
		cal.set(Calendar.YEAR, Integer.parseInt(date.substring(0, 4)));	//年
		//月 是从0开始，所以要-1
	    cal.set(Calendar.MONTH,Integer.parseInt(date.substring(4, 6)) - 1);	
	    //日
		cal.set(Calendar.DATE, 1);
		//当前月份+1
		cal.add(Calendar.MONTH,1);
		//减1为上月最后一日，即所求年月最后一天
		cal.add(Calendar.DATE, -1); 
		return new StringBuffer(date).append(cal.get(Calendar.DAY_OF_MONTH)).toString();
	}
	
	/**
	 * 获取2个日期间的天数
	 * @param begin	开始日期
	 * @param end	结束日期
	 * @param format  字串日期格式
	 * @return
	 * @throws ParseException
	 */
	public static int getBetweenDays(String begin, String end, String format) throws Exception{
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Calendar cal = Calendar.getInstance(); 
		cal.setTime(sdf.parse(begin)); 
		long time1 = cal.getTimeInMillis();              
		cal.setTime(sdf.parse(end)); 
		long time2 = cal.getTimeInMillis();      
		long between_days = (time2 - time1) / (1000 * 3600 * 24);
		return Integer.parseInt(String.valueOf(between_days)) + 1;  
	}
	
	public static int getBetweenMonthsNum(String startDate, String endDate, String dayDateFormat) throws Exception {
	    Calendar c1 = Calendar.getInstance();
	    Calendar c2 = Calendar.getInstance();
	    c1.setTime(parseDate(startDate, dayDateFormat));
	    c2.setTime(parseDate(endDate, dayDateFormat));
        int year = c2.get(Calendar.YEAR) - c1.get(Calendar.YEAR);
        return year * 12 + c2.get(Calendar.MONTH) - c1.get(Calendar.MONTH) + 1;
	}
	
	public static List<String> getBetweenMonths(String startDate, String endDate, String dayDateFormat, String monthDateFormat) throws Exception {
		int monthsNum = getBetweenMonthsNum(startDate, endDate, dayDateFormat);
		String firstDateStr = getFirstDateOfMonth(startDate, dayDateFormat, dayDateFormat);
		Date firstDate = parseDate(firstDateStr, dayDateFormat);
		List<String> result = new ArrayList<String>();
		
		for (int i = 0; i < monthsNum; i++) {
			result.add(addMonth(i, firstDate, monthDateFormat));
		}
		
		return result;
	}
	
	public static String addMonth(int num, Date date, String monthDateFormat) {
		Calendar c1 = Calendar.getInstance();
		c1.setTime(date);
		c1.add(Calendar.MONTH, num);
		return dateFormat(c1.getTime(), monthDateFormat);
	}
	
	/**
	 * 获取指定时间前后N天的时间
	 * @param date
	 * @param day
	 * @return
	 */
	public static Date getDate(Date date, int day){
		if (day == 0) {
			return date;
		}
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_YEAR, day);
		return calendar.getTime();
	}
	
	public static String getAddDate(String startDateStr, String dateFormat, int addDays) {
		try {
			Date startDate = parseDate(startDateStr, dateFormat);
			Date tgtDate = getDate(startDate, addDays);
			return dateFormat(tgtDate, dateFormat);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "";
		}
	}
	
    public static String getPreviousDate(String dateFormat) {
    	Date preDate = getDate(new Date(), -1);
    	return dateFormat(preDate, dateFormat);
    }
    
    public static String getPreviousDate(String dateStr, String dateFormat) throws Exception {
    	Date date = parseDate(dateStr, dateFormat);
    	return dateFormat(date, dateFormat);
    }
    
    public static String getCurrentDate(String dateFormat) {
    	return dateFormat(new Date(), dateFormat);
    }
    
    public static String getFirstDateOfPreviousMonth(String dayDateFormat) throws Exception {
    	return getFirstDateOfPreviousMonth(new Date(), dayDateFormat);
    }
    
    public static String getFirstDateOfPreviousMonth(String dateStr, String dayDateFormat) throws Exception {
    	return getFirstDateOfPreviousMonth(parseDate(dateStr, dayDateFormat), dayDateFormat);
    }
    
    public static String getFirstDateOfPreviousMonth(Date date, String dayDateFormat) throws Exception {
    	String firstDate = getFirstDateOfMonth(date, dayDateFormat);
    	Date firstDateOfPreviousMonth = getDateByMonth(parseDate(firstDate, dayDateFormat), -1);
    	return dateFormat(firstDateOfPreviousMonth, dayDateFormat);
    }
    
    public static String getLastDateOfPreviousMonth(String dayDateFormat) throws Exception {
    	return getLastDateOfPreviousMonth(new Date(), dayDateFormat);
    }
    
    public static String getLastDateOfPreviousMonth(String dateStr, String dayDateFormat) throws Exception {
    	return getLastDateOfPreviousMonth(parseDate(dateStr, dayDateFormat), dayDateFormat);
    }
    
    public static String getLastDateOfPreviousMonth(Date date, String dayDateFormat) throws Exception {
    	String firstDate = getFirstDateOfMonth(date, dayDateFormat);
    	Calendar calendar = Calendar.getInstance();
		calendar.setTime(parseDate(firstDate, dayDateFormat));
		calendar.add(Calendar.DATE, -1);
    	return dateFormat(calendar.getTime(), dayDateFormat);
    }
    
    public static String getFirstDateOfMonth(Date date, String dayDateFormat) throws Exception {
    	Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		return dateFormat(calendar.getTime(), dayDateFormat);
    }
    
    public static String getFirstDateOfMonth(String dateStr, String srcDateFormat, String dayDateFormat) throws Exception {
    	return getFirstDateOfMonth(parseDate(dateStr, srcDateFormat), dayDateFormat);
    }
    
    public static String getLastDateOfMonth(Date date, String dayDateFormat) throws Exception {
    	Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1); // 设置当月的1日
		calendar.add(Calendar.MONTH, 1); // 增加一个月，值为下月1日
		calendar.add(Calendar.DATE, -1); // 减去1天，即上月最后一天
		return dateFormat(calendar.getTime(), dayDateFormat);
    }
    
    public static String getLastDateOfMonth(String dateStr, String srcDateFormat, String dayDateFormat) throws Exception {
    	return getLastDateOfMonth(parseDate(dateStr, srcDateFormat), dayDateFormat);
    }
    
	/***
	 * 获取指定日期前后N个月的日期
	 * @param date
	 * @param month
	 * @return
	 */
	public static Date getDateByMonth(Date date, int month){
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		
		if (month != 0) {
			calendar.add(Calendar.MONTH, month);
		}
		
		return calendar.getTime();
	}
	
	/**
	 * 当前时间的上下文
	 * @param type
	 * @param value
	 * @return
	 */
	public static Date getDate(String type, long value){
		if(type == null){
			System.out.println("type_can_not_be_null");
			return null;
		}
		Calendar calendar = Calendar.getInstance();
		if(type.equals("year")){
			
		}else if(type.equals("month")){
			
		}else if(type.equals("date")){
			
		}else if(type.equals("hour")){
			calendar.add(Calendar.HOUR, (int) value);
		}else if(type.equals("minute")){
			calendar.add(Calendar.MINUTE, (int) value);
		}else if(type.equals("second")){
		}else{
			System.out.println("type_is_error");
		}
		return calendar.getTime();
	}
	
	/**
	 * 检查最近时间
	 * @param begin	起始时间
	 * @param end	结束时间	
	 * @param checkMin	校验分钟
	 * @param checkHour	校验小时
	 * @param dur	时差
	 * @return
	 */
	public static boolean checkTime(Date begin, Date end, boolean checkMin, Integer durM, boolean checkHour, float durH){
		if(checkMin && checkHour){
			return false;
		}
		boolean flag = false;
		double divisor = 0;
		if(checkHour){//比对小时
			divisor = 1000*60*60;
			double dividend = (end.getTime() - begin.getTime());
			double time = (double)(dividend / divisor);
			System.out.println("  time = " + time + ", durH = " + durH);
			if(time < durH){
				flag = true;
			}
		}else{//比对分钟
			divisor = 1000*60;
			double dividend = (end.getTime() - begin.getTime());
			double time = (double)(dividend / divisor);
			System.out.println("  time = " + time + ", durM = " + durM);
			if(time < durM){
				flag = true;
			}
		}
		return flag;
	}
	
	public static boolean isToday(String createdDate,String srcDateFormat,String destDateFormat) {
		try {
			if(StringUtils.isEmpty(createdDate))return false;
			String date1 = DateUtils.dateFormat(createdDate, srcDateFormat, destDateFormat);
			String date2 = DateUtils.dateFormat(new Date(), destDateFormat);
			if(date1.equals(date2))
				return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public static Long dateDiff(String startTime, String endTime, String format, String str) {     
        // 按照传入的格式生成一个simpledateformate对象     
        SimpleDateFormat sd = new SimpleDateFormat(format);
        
        try {
			return dateDiff(sd.parse(startTime), sd.parse(endTime), str);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        return 0L;
    }  
	
	public static Long dateDiff(Date startTime, Date endTime, String type) {
		return dateDiff(startTime.getTime(), endTime.getTime(), type);
	}
	
	public static Long dateDiff(long startTime, long endTime, String type) {   
		// 按照传入的格式生成一个simpledateformate对象     
        long nd = 1000 * 24 * 60 * 60;// 一天的毫秒数     
        long nh = 1000 * 60 * 60;// 一小时的毫秒数     
        long nm = 1000 * 60;// 一分钟的毫秒数     
        long ns = 1000;// 一秒钟的毫秒数     
        long diff;     
        long day = 0;     
        long hour = 0;     
        long min = 0;     
        long sec = 0;     
        // 获得两个时间的毫秒时间差异     
        try {     
            diff = endTime - startTime;     
            day = diff / nd;// 计算差多少天     
            hour = diff / nh;// 计算差多少小时     
            min = diff / nm;// 计算差多少分钟     
            sec = diff / ns;// 计算差多少秒     
        } catch (Exception e) {     
            // TODO Auto-generated catch block     
            e.printStackTrace();     
        }    
        
        if (type.equalsIgnoreCase("d")) {
        	return day;
        } else if (type.equalsIgnoreCase("h")) {     
            return hour;     
        } else if (type.equalsIgnoreCase("m")) {     
            return min;     
        } else {
        	return sec;
        }
	}
	
	public static String dateDiffDesc(long startTime, long endTime) {
        long day = dateDiff(startTime, endTime, "d");
        long hour = dateDiff(startTime, endTime, "h");
        long min = dateDiff(startTime, endTime, "m");
        long sec = dateDiff(startTime, endTime, "s");
        String desc = day + "天" + (hour - day * 24) + "小时"
                + (min - hour * 60) + "分" + (sec - min * 60) + "秒。";
        return desc;
    }
	
    /**
     * 获取unix时间戳，单位为秒
     * @param dateStr
     * @return
     */
	public static long getSecUnixTimestamp(String dateStr, String dateFormat) {
		long dateTime = StringToDate(dateStr, dateFormat).getTime();
		return (long) (dateTime / 1000);
	}
	
	public static long getCurDateSecUnixTimestamp() {
		return getSecUnixTimestamp(getCurrentDate(DEFAULT_PATTERN), DEFAULT_PATTERN);
	}
	
	public static List<String> getBetweenDates(String startDate, String endDate, String dateFormat) throws Exception {
		List<String> result = new ArrayList<String>();
		int betweenDays = getBetweenDays(startDate, endDate, dateFormat);
		
		for (int i = 0; i < betweenDays; i++) {
			result.add(getAddDate(startDate, dateFormat, i));
		}
		
		return result;
	}
	
	public static void main(String[] args) throws Exception {
//		System.out.println(getSecUnixTimestamp("2013-1-1", "yyyy-MM-dd"));
//		System.out.println(getPreviousDate("yyyy-MM-dd"));
//		System.out.println(getBetweenDays("2016-6-1", "2016-6-29", "yyyy-MM-dd"));
//		System.out.println(getAddDate("2016-1-30", "yyyy-MM-dd", 5));
		List<String> list = getBetweenDates("2016-1-25", "2016-3-10", "yyyy-MM-dd");
		
		for (String str : list) {
			System.out.println(str);
		}
		
		System.out.println(dateFormat(System.currentTimeMillis()));
	}
	
	public static boolean isSameHour(int value,String dateFormat,String compareDate) {
		Date date = DateUtils.getDate("hour", value);
		String date2Str = DateUtils.date2Str(date, dateFormat);
		if(date2Str.equals(compareDate))
			return true;
		else 
			return false;
	}


}
