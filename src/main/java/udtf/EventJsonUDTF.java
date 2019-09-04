package udtf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName EventJsonUDTF
 * @Description TODO 一进多出,解析json的一个key对应多个value的值
 * @Author Dang-h
 * @Email 54danghao@gmail.com
 * @Date 2019-9-4 0004 16:07
 * @Version 1.0
 * <p>
 * https://cwiki.apache.org/confluence/display/Hive/DeveloperGuide+UDTF
 **/

public class EventJsonUDTF extends GenericUDTF {


	/**
	 * 传入参数校验,由Hive来监视
	 *
	 * @param argOIs
	 * @return
	 * @throws UDFArgumentException
	 */
	@Override
	public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

		//获取参数的描述信息
		List<? extends StructField> allStructFieldRefs = argOIs.getAllStructFieldRefs();

		//期望传入参数的数量
		if (allStructFieldRefs.size() != 1) {
			throw new UDFArgumentException("参数只能为1");
		}

		//期望传入参数的类型
		if (!"string".equals(allStructFieldRefs.get(0).getFieldObjectInspector().getTypeName())) {
			throw new UDFArgumentException("类型只能为string");
		}

		//获取返回结果名字
		ArrayList<String> fieldName = new ArrayList<>();
		//获取返回结果类型
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();

		fieldName.add("event_name");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);


		fieldName.add("event_json");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(fieldName, fieldOIs);
	}


	@Override
	public void process(Object[] args) throws HiveException {

		//通过initialize处理,只会有一个参数
		String events = (String) args[0];

		if (StringUtils.isBlank(events)) {
			return;
		}

		try {
			JSONArray eventArray = new JSONArray(events);

			for (int i = 0; i < eventArray.length(); i++) {
				String[] result = new String[2];
				JSONObject event = eventArray.getJSONObject(i);

				result[0] = event.getString("en");
				result[1] = event.toString();

				//一行数据一个forward
				forward(result);
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void close() throws HiveException {

	}
}
