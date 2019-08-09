package filter;

import org.json.JSONObject;

import java.util.Base64;

public class Test {

    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject("{\"name\":\"Olga\", \"class\":2, \"middle-mark\":6}");

        System.out.println(jsonObject);

        String name = jsonObject.getString("name");
        System.out.println(name);

        String encodedName = Base64.getEncoder().encodeToString(name.getBytes());
        System.out.println(encodedName);

        byte[] decodeBytes = Base64.getDecoder().decode(encodedName);
        System.out.println(new String(decodeBytes));

        int cl = jsonObject.getInt("class");
        System.out.println("class = " + cl);

        if(cl > 0 ){
            System.out.println(true);
        }

    }
}
