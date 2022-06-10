package com.yxqiche.udf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class ParseJson extends ScalarFunction {
    public @DataTypeHint("String") String eval(String jsonStr,String value) {
        String result = null;
        try {
            JSONArray jsonArr = JSONObject.parseArray(jsonStr);
            JSONObject tmpJsonObj = jsonArr.getJSONObject(0);
            String[] split = value.split("\\.");
            for (int i = 0; i < split.length; i++) {
                if (i != split.length -1){
                     tmpJsonObj = tmpJsonObj.getJSONObject(split[i]);
                }else {
                    result = tmpJsonObj.getString(split[i]);
                }
            }
        }catch (Exception e){
            return null;
        }
        return result;
    }

}
