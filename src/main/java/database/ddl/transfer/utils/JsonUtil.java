package database.ddl.transfer.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.springframework.core.io.ClassPathResource;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName JsonUtil
 * @Description TODO
 * @Author luoyuntian
 * @Date 2019-12-25 14:32
 * @Version
 **/
public class JsonUtil {

	private volatile static Map<String, Map<String, String>> jsonMap = null;

	private JsonUtil() {

	}

	/**
	 * 读取映射数据加入到map缓存中
	 * 
	 * @throws IOException
	 */
	public static Map<String, Map<String, String>> readJsonData(String jsonPath) throws IOException {
		if (jsonMap == null) {
			synchronized (JsonUtil.class) {
				if (jsonMap == null) {
					jsonMap = new ConcurrentHashMap<>();
					ClassPathResource resource = new ClassPathResource(jsonPath);
					File file = resource.getFile();
					String jsonString = FileUtils.readFileToString(file, "utf-8");
					JSONObject jsonObject = JSONObject.parseObject(jsonString);
					Set<String> keySet = jsonObject.keySet();
					Iterator<String> iterator = keySet.iterator();
					while (iterator.hasNext()) {
						Map<String, String> mapingMap = new HashMap<>();
						// 获取转换类型
						String convertType = iterator.next();
						// 获取mapping
						String mapping = jsonObject.getString(convertType);
						// 将mapping解析为object对象
						JSONObject mappingJson = JSONObject.parseObject(mapping);
						// 遍历mapping
						Set<String> orginalTypeSet = mappingJson.keySet();
						for (String orginalType : orginalTypeSet) {
							String targetType = mappingJson.getString(orginalType);
							mapingMap.put(orginalType, targetType);
							jsonMap.put(convertType, mapingMap);
						}
					}
				}
			}
		}

		return jsonMap;

	}

}
