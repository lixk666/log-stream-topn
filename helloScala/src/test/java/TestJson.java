import bean.InternalLog;
import com.alibaba.fastjson.JSONObject;

public class TestJson {

    public static void main(String[] args) {
        String json = "{\"client_ip\":\"192.168.1.1\"}";

        InternalLog log = JSONObject.parseObject(json, InternalLog.class);

        System.out.println(log);

    }
}
