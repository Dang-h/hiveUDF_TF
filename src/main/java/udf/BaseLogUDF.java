package udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @ClassName BaseFieldUDF
 * @Description TODO 一进一出,解析公共字段
 * @Author Dang-h
 * @Email 54danghao@gmail.com
 * @Date 2019-9-4 0004 11:36
 * @Version 1.0
 **/
public class BaseLogUDF extends UDF {

	public String evaluate(String line, String key) throws JSONException {



		//处理line,以|切分,分为time和log 服务器时间 | json
		String[] log = line.split("\\|");

		//取出Json,以kv形式对数据进行处理
		JSONObject baseJson = new JSONObject(log[1].trim());

		//根据key找出对应的value
		if ("st".equals(key)){
			return log[0];
		} else if ("et".equals(key)){
			//判断json中是否有et
			if (!baseJson.has("et")){
				return "";
			}

			//获取et
			String et = baseJson.getString(key);
			if (et != null && !"".equals(et)){
				return et;
			}

		} else {

			if (!baseJson.has("cm")) {
				return "";
			}

			JSONObject cm = baseJson.getJSONObject("cm");
			if (cm.has(key)){
				return cm.getString(key);
			}
		}
		return "";
	}

//	public static void main(String[] args) throws JSONException {
//		String line = "1564589753760|{\"cm\":{\"ln\":\"-85.9\",\"sv\":\"V2.2.4\",\"os\":\"8.1.9\",\"g\":\"8409SV5B@gmail.com\",\"mid\":\"3\",\"nw\":\"4G\",\"l\":\"es\",\"vc\":\"10\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"3\",\"t\":\"1564564133134\",\"la\":\"-12.2\",\"md\":\"HTC-2\",\"vn\":\"1.3.1\",\"ba\":\"HTC\",\"sr\":\"K\"},\"ap\":\"app\",\"et\":[{\"ett\":\"1564535342874\",\"en\":\"ad\",\"kv\":{\"entry\":\"3\",\"show_style\":\"2\",\"action\":\"4\",\"detail\":\"\",\"source\":\"4\",\"behavior\":\"1\",\"content\":\"1\",\"newstype\":\"7\"}},{\"ett\":\"1564539829612\",\"en\":\"active_background\",\"kv\":{\"active_source\":\"2\"}},{\"ett\":\"1564511533510\",\"en\":\"favorites\",\"kv\":{\"course_id\":0,\"id\":0,\"add_time\":\"1564567430191\",\"userid\":3}}]}";
//		String line1 = "1564589753760|{\"cm\":{\"ln\":\"-85.9\",\"sv\":\"V2.2.4\",\"os\":\"8.1.9\",\"g\":\"8409SV5B@gmail.com\",\"mid\":\"3\",\"nw\":\"4G\",\"l\":\"es\",\"vc\":\"10\",\"hw\":\"750*1134\",\"ar\":\"MX\",\"uid\":\"3\",\"t\":\"1564564133134\",\"la\":\"-12.2\",\"md\":\"HTC-2\",\"vn\":\"1.3.1\",\"ba\":\"HTC\",\"sr\":\"K\"},\"ap\":\"app\"}";
//		String ln = new BaseLogUDF().evaluate(line1, "ett");
//		System.out.println(ln);
//	}
}
