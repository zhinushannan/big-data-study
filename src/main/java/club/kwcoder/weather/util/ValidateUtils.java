package club.kwcoder.weather.util;

import org.apache.commons.lang3.StringUtils;

/**
 * 校验工具类，统一返回方式，针对本项目，合法返回false，非法返回true
 *
 * @author zhinushannan
 */
public class ValidateUtils {

    /**
     * 校验输入文本数据
     * @param line 文本数据
     * @return 空返回true
     */
    public static boolean validate(String line) {
        return StringUtils.isBlank(line);
    }

    /**
     * 校验字符串数组长度
     * @param items 字符串数组
     * @param length 长度
     * @return 不相同返回true
     */
    public static boolean validate(String[] items, int length) {
        return items.length != length;
    }

    /**
     * 校验输入文本数据并分割
     * @param line 输入文本数据
     * @param sep 分隔符
     * @param limit 目标长度
     * @return 当字符串为空或无法分割到目标长度时，返回true
     */
    public static String[] splitAndValidate(String line, String sep, int limit) {
        if (validate(line)) {
            return null;
        }
        String[] split = line.split(sep, limit);
        return validate(split, limit) ? null : split;
    }

}
