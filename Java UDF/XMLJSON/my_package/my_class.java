package my_package;

import org.json.JSONObject;
import org.json.XML;
import org.json.JSONException;

// export JAVA_HOME=`/usr/libexec/java_home -v 11.0`
// https://en.wikipedia.org/wiki/Candlestick_pattern
// javac -d classDirectory my_package/my_class.java
// jar cmf my_manifest.manifest ./my_jar.jar -C classDirectory my_package/my_class.class
   
public class my_class{
    public static String TEST_XML_STRING = "<?xml version=\"1.0\" ?><test attrib=\"moretest\">Turn this to JSON</test>";

    public static String xmlJSON(String xml){
        try {
            JSONObject xmlJSONObj = XML.toJSONObject(xml);
            String jsonPrettyPrintString = xmlJSONObj.toString(0);
            return jsonPrettyPrintString;
        } catch (JSONException je) {
            System.out.println(je.toString());
        }
        return "";
    }//end candle

    


    public static void main(String[] argv){
       
    }

}





