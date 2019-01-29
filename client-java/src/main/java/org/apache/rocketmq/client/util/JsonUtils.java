package org.apache.rocketmq.client.util;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.client.Obj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: codefans
 * @Date: 2018-11-03 19:57
 */

public class JsonUtils {

    /**
     * 格式化json串
     * @param jsonStr
     * @return
     */
    public static String formatJson(String jsonStr) {
        if (null == jsonStr || "".equals(jsonStr)) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        char last = '\0';
        char current = '\0';
        int indent = 0;
        boolean isInQuotationMarks = false;
        for (int i = 0; i < jsonStr.length(); i++) {
            last = current;
            current = jsonStr.charAt(i);
            switch (current) {
                case '"':
                    if (last != '\\') {
                        isInQuotationMarks = !isInQuotationMarks;
                    }
                    sb.append(current);
                    break;

                case '{':
                case '[':
                    sb.append(current);
                    if (!isInQuotationMarks) {
                        sb.append('\n');
                        indent++;
                        addIndentBlank(sb, indent);
                    }
                    break;

                case '}':
                case ']':
                    if (!isInQuotationMarks) {
                        sb.append('\n');
                        indent--;
                        addIndentBlank(sb, indent);
                    }
                    sb.append(current);
                    break;

                case ',':
                    sb.append(current);
                    if (last != '\\' && !isInQuotationMarks) {
                        sb.append('\n');
                        addIndentBlank(sb, indent);
                    }
                    break;

                default:
                    sb.append(current);
            }
        }

        return sb.toString();
    }

    /**
     * 添加space
     * @param sb
     * @param indent
     */
    private static void addIndentBlank(StringBuilder sb, int indent) {
        for (int i = 0; i < indent; i++) {
            sb.append('\t');
        }
    }

    public static void main(String[] args) {
        String jsonStr = "{data:[{name:'zhangsan'},{name:'lisi'}]}";
        System.out.println(formatJson(jsonStr));

        Obj obj = new Obj();
        obj.setKey("name");
        obj.setVal("张三");
        Obj obj2 = new Obj();
        obj2.setKey("name");
        obj2.setVal("李四");
        List<Obj> objList = new ArrayList<Obj>();
        objList.add(obj);
        objList.add(obj2);
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("data", objList);
        System.out.println(JSON.toJSONString(map));

    }


}
