package edu.umass.cs.utils;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class UtilTest extends DefaultTest {
    /**
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_01_JSONObjectToMap() throws JSONException {
        Util.assertAssertionsEnabled();
        Map<String, ?> map = (Util.JSONObjectToMap(new JSONObject()
                .put("hello", "world")
                .put("collField", Arrays.asList("hello", "world", 123))
                .put("jsonField", new JSONObject().put("foo", true))));
        System.out.println(map);
        org.junit.Assert.assertTrue(map.get("jsonField") instanceof Map
                && (Boolean) ((Map<String, ?>) map.get("jsonField"))
                .get("foo"));

    }

    /**
     * @throws JSONException
     */
    @SuppressWarnings("unchecked")
    @Test
    public void test_01_JSONArrayToMap() throws JSONException {
        Util.assertAssertionsEnabled();
        List<?> list = (Util.JSONArrayToList(new JSONArray()

                .put("hello") // 0

                .put(Arrays.asList("hello", "world", 123)) // 1

                .put(new JSONObject().put("foo", true)))); // 2

        System.out.println(list);
        org.junit.Assert.assertTrue(list.get(2) instanceof Map
                && (Boolean) ((Map<String, ?>) list.get(2)).get("foo"));

    }
}
