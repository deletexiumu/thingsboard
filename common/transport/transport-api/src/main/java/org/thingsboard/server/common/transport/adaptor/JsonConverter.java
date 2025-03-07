/**
 * Copyright © 2016-2019 The Thingsboard Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.thingsboard.server.common.transport.adaptor;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import javafx.util.Pair;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.util.StringUtils;
import org.thingsboard.server.common.data.kv.AttributeKey;
import org.thingsboard.server.common.data.kv.AttributeKvEntry;
import org.thingsboard.server.common.data.kv.BaseAttributeKvEntry;
import org.thingsboard.server.common.data.kv.BooleanDataEntry;
import org.thingsboard.server.common.data.kv.DoubleDataEntry;
import org.thingsboard.server.common.data.kv.KvEntry;
import org.thingsboard.server.common.data.kv.LongDataEntry;
import org.thingsboard.server.common.data.kv.StringDataEntry;
import org.thingsboard.server.common.msg.kv.AttributesKVMsg;
import org.thingsboard.server.gen.transport.TransportProtos;
import org.thingsboard.server.gen.transport.TransportProtos.AttributeUpdateNotificationMsg;
import org.thingsboard.server.gen.transport.TransportProtos.GetAttributeResponseMsg;
import org.thingsboard.server.gen.transport.TransportProtos.KeyValueProto;
import org.thingsboard.server.gen.transport.TransportProtos.KeyValueType;
import org.thingsboard.server.gen.transport.TransportProtos.PostAttributeMsg;
import org.thingsboard.server.gen.transport.TransportProtos.PostTelemetryMsg;
import org.thingsboard.server.gen.transport.TransportProtos.TsKvListProto;
import org.thingsboard.server.gen.transport.TransportProtos.TsKvProto;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JsonConverter {

    private static final Gson GSON = new Gson();
    private static final String CAN_T_PARSE_VALUE = "Can't parse value: ";
    private static final String DEVICE_PROPERTY = "device";

    private static boolean isTypeCastEnabled = true;

    private static int maxStringValueLength = 0;

    public static PostTelemetryMsg convertToTelemetryProto(JsonElement jsonObject) throws JsonSyntaxException {
        long systemTs = System.currentTimeMillis();
        PostTelemetryMsg.Builder builder = PostTelemetryMsg.newBuilder();
        if (jsonObject.isJsonObject()) {
            parseObject(builder, systemTs, jsonObject);
        } else if (jsonObject.isJsonArray()) {
            jsonObject.getAsJsonArray().forEach(je -> {
                if (je.isJsonObject()) {
                    parseObject(builder, systemTs, je.getAsJsonObject());
                } else {
                    throw new JsonSyntaxException(CAN_T_PARSE_VALUE + je);
                }
            });
        } else {
            throw new JsonSyntaxException(CAN_T_PARSE_VALUE + jsonObject);
        }
        return builder.build();
    }

    public static PostAttributeMsg convertToAttributesProto(JsonElement jsonObject) throws JsonSyntaxException {
        if (jsonObject.isJsonObject()) {
            PostAttributeMsg.Builder result = PostAttributeMsg.newBuilder();
            List<KeyValueProto> keyValueList = parseProtoValues(jsonObject.getAsJsonObject());
            result.addAllKv(keyValueList);
            return result.build();
        } else {
            throw new JsonSyntaxException(CAN_T_PARSE_VALUE + jsonObject);
        }
    }

    public static JsonElement toJson(TransportProtos.ToDeviceRpcRequestMsg msg, boolean includeRequestId) {
        JsonObject result = new JsonObject();
        if (includeRequestId) {
            result.addProperty("id", msg.getRequestId());
        }
        result.addProperty("method", msg.getMethodName());
        result.add("params", new JsonParser().parse(msg.getParams()));
        return result;
    }

    private static void parseObject(PostTelemetryMsg.Builder builder, long systemTs, JsonElement jsonObject) {
        JsonObject jo = jsonObject.getAsJsonObject();
        if (jo.has("ts") && jo.has("values")) {
            parseWithTs(builder, jo);
        } else {
            parseWithoutTs(builder, systemTs, jo);
        }
    }

    private static void parseWithoutTs(PostTelemetryMsg.Builder request, long systemTs, JsonObject jo) {
        TsKvListProto.Builder builder = TsKvListProto.newBuilder();
        builder.setTs(systemTs);
        builder.addAllKv(parseProtoValues(jo));
        request.addTsKvList(builder.build());
    }

    private static void parseWithTs(PostTelemetryMsg.Builder request, JsonObject jo) {
        TsKvListProto.Builder builder = TsKvListProto.newBuilder();
        builder.setTs(jo.get("ts").getAsLong());
        builder.addAllKv(parseProtoValues(jo.get("values").getAsJsonObject()));
        request.addTsKvList(builder.build());
    }

    /**
     * 解析基础属性值
     * @param cJsonObj 传入json对象
     * @return 返回解析后的值
     */
    private static List<KeyValueProto> parseValue(String strKeyName, JsonElement cJsonObj, List<KeyValueProto> result) {
        // 获取属性值
        JsonPrimitive value = cJsonObj.getAsJsonPrimitive();
        // 判断属性类型
        // 字符串类型
        if (value.isString()) {
            // 长度判断
            if (maxStringValueLength > 0 && value.getAsString().length() > maxStringValueLength) {
                String message = String.format("String value length [%d] for key [%s] is greater than maximum allowed [%d]", value.getAsString().length(), strKeyName, maxStringValueLength);
                throw new JsonSyntaxException(message);
            }
            result.add(KeyValueProto.newBuilder().setKey(strKeyName).setType(KeyValueType.STRING_V)
                    .setStringV(value.getAsString()).build());
        } else if (value.isBoolean()) {
            result.add(KeyValueProto.newBuilder().setKey(strKeyName).setType(KeyValueType.BOOLEAN_V)
                    .setBoolV(value.getAsBoolean()).build());
        } else if (value.isNumber()) {
            if (value.getAsString().contains(".")) {
                result.add(KeyValueProto.newBuilder().setKey(strKeyName).setType(KeyValueType.DOUBLE_V)
                        .setDoubleV(value.getAsDouble()).build());
            } else {
                result.add(KeyValueProto.newBuilder().setKey(strKeyName).setType(KeyValueType.LONG_V)
                        .setLongV(value.getAsLong()).build());
            }
        } else {
            throw new JsonSyntaxException(CAN_T_PARSE_VALUE + cJsonObj);
        }

        return result;
    }

    /**
     * 子节点解析
     * @param strKeyName 节点名称
     * @param cJsonElement 节点element对象
     * @return 返回pair类型，K为解析成基本类型值；V为pair类型的list，即下一层节点，K为节点名称，V为element对象
     */
    private static Pair<List<KeyValueProto>, List<Pair<String, JsonElement>>> parseChildNode(String strKeyName, JsonElement cJsonElement) {
        List<Pair<String, JsonElement>> nextNodes = new ArrayList<>();
        List<KeyValueProto> result = new ArrayList<>();

        for (Entry<String, JsonElement> valueEntry : cJsonElement.getAsJsonObject().entrySet()) {
            // 获取节点属性
            JsonElement element = valueEntry.getValue();
            // 判断是否基本类型
            if (element.isJsonPrimitive()) {
                // 基本类型解析
                result = parseValue(strKeyName+"_"+valueEntry.getKey(), element, result);
            } else if (element.isJsonArray()) {
                // 针对数组解析
                // 获取整体数组
                JsonArray cJsonArray = element.getAsJsonArray();

                for (int i=0; i<cJsonArray.size(); ++i) {
                    // 判断是否基本类型
                    if (cJsonArray.get(i).isJsonPrimitive()) {
                        // 基本类型解析
                        result = parseValue(strKeyName+"_"+valueEntry.getKey()+"_"+(i+1), cJsonArray.get(i), result);
                    } else {
                        // 对象节点解析
                        nextNodes.add(new Pair<>(strKeyName+"_"+valueEntry.getKey()+"_"+(i+1), cJsonArray.get(i)));
                    }
                }
            } else {
                // 对象节点解析
                nextNodes.add(new Pair<>(strKeyName+"_"+valueEntry.getKey(), element));
            }
        }

        return new Pair<>(result, nextNodes);
    }

    private static List<KeyValueProto> parseProtoValues(JsonObject valuesObject) {
        List<KeyValueProto> result = new ArrayList<>();
        // 遍历节点
        for (Entry<String, JsonElement> valueEntry : valuesObject.entrySet()) {
            // 获取节点属性
            JsonElement element = valueEntry.getValue();
            // 判断是否基本类型
            if (element.isJsonPrimitive()) {
                result = parseValue(valueEntry.getKey(), element, result);
            } else {
                // 复杂节点循环解析
                String strKeyName = valueEntry.getKey();
                JsonElement cChildElement = element;
                // 子节点存储
                List<Pair<String, JsonElement>> cChildNodeList = new ArrayList<>();

                // 首层数组解析
                if (element.isJsonArray()) {
                    // 针对数组解析
                    // 获取整体数组
                    JsonArray cJsonArray = element.getAsJsonArray();
                    for (int i=0; i<cJsonArray.size(); ++i) {
                        // 判断是否基本类型
                        if (cJsonArray.get(i).isJsonPrimitive()) {
                            // 基本类型解析
                            result = parseValue(strKeyName+"_"+(i+1), cJsonArray.get(i), result);
                        } else {
                            // 对象节点解析
                            cChildNodeList.add(new Pair<>(strKeyName+"_"+(i+1), cJsonArray.get(i)));
                        }
                    }

                    // 设置下个节点
                    if (0 < cChildNodeList.size()) {
                        // 取列表中第一个元素
                        strKeyName = cChildNodeList.get(0).getKey();
                        cChildElement = cChildNodeList.get(0).getValue();
                        cChildNodeList.remove(0);
                    } else {
                        cChildElement = null;
                    }
                }

                // 循环解析
                while (null != cChildElement) {
                    // 节点解析
                    Pair<List<KeyValueProto>, List<Pair<String, JsonElement>>> cRet = parseChildNode(strKeyName, cChildElement);
                    // 添加元素
                    result.addAll(cRet.getKey());
                    // 添加至待解析列表
                    if (0 < cRet.getValue().size()) {
                        cChildNodeList.addAll(cRet.getValue());
                    }
                    // 判断列表是否为空
                    if (0 == cChildNodeList.size()) {
                        break;
                    } else {
                        // 取列表中第一个元素
                        strKeyName = cChildNodeList.get(0).getKey();
                        cChildElement = cChildNodeList.get(0).getValue();
                        cChildNodeList.remove(0);
                    }
                }
            }
        }

        return result;
    }

    private static KeyValueProto buildNumericKeyValueProto(JsonPrimitive value, String key) {
        if (value.getAsString().contains(".")) {
            return KeyValueProto.newBuilder()
                    .setKey(key)
                    .setType(KeyValueType.DOUBLE_V)
                    .setDoubleV(value.getAsDouble())
                    .build();
        } else {
            try {
                long longValue = Long.parseLong(value.getAsString());
                return KeyValueProto.newBuilder().setKey(key).setType(KeyValueType.LONG_V)
                        .setLongV(longValue).build();
            } catch (NumberFormatException e) {
                throw new JsonSyntaxException("Big integer values are not supported!");
            }
        }
    }

    public static TransportProtos.ToServerRpcRequestMsg convertToServerRpcRequest(JsonElement json, int requestId) throws JsonSyntaxException {
        JsonObject object = json.getAsJsonObject();
        return TransportProtos.ToServerRpcRequestMsg.newBuilder().setRequestId(requestId).setMethodName(object.get("method").getAsString()).setParams(GSON.toJson(object.get("params"))).build();
    }

    private static void parseNumericValue(List<KvEntry> result, Entry<String, JsonElement> valueEntry, JsonPrimitive value) {
        if (value.getAsString().contains(".")) {
            result.add(new DoubleDataEntry(valueEntry.getKey(), value.getAsDouble()));
        } else {
            try {
                long longValue = Long.parseLong(value.getAsString());
                result.add(new LongDataEntry(valueEntry.getKey(), longValue));
            } catch (NumberFormatException e) {
                throw new JsonSyntaxException("Big integer values are not supported!");
            }
        }
    }

    public static JsonObject toJson(GetAttributeResponseMsg payload) {
        JsonObject result = new JsonObject();
        if (payload.getClientAttributeListCount() > 0) {
            JsonObject attrObject = new JsonObject();
            payload.getClientAttributeListList().forEach(addToObjectFromProto(attrObject));
            result.add("client", attrObject);
        }
        if (payload.getSharedAttributeListCount() > 0) {
            JsonObject attrObject = new JsonObject();
            payload.getSharedAttributeListList().forEach(addToObjectFromProto(attrObject));
            result.add("shared", attrObject);
        }
        if (payload.getDeletedAttributeKeysCount() > 0) {
            JsonArray attrObject = new JsonArray();
            payload.getDeletedAttributeKeysList().forEach(attrObject::add);
            result.add("deleted", attrObject);
        }
        return result;
    }

    public static JsonElement toJson(AttributeUpdateNotificationMsg payload) {
        JsonObject result = new JsonObject();
        if (payload.getSharedUpdatedCount() > 0) {
            payload.getSharedUpdatedList().forEach(addToObjectFromProto(result));
        }
        if (payload.getSharedDeletedCount() > 0) {
            JsonArray attrObject = new JsonArray();
            payload.getSharedDeletedList().forEach(attrObject::add);
            result.add("deleted", attrObject);
        }
        return result;
    }

    public static JsonObject toJson(AttributesKVMsg payload, boolean asMap) {
        JsonObject result = new JsonObject();
        if (asMap) {
            if (!payload.getClientAttributes().isEmpty()) {
                JsonObject attrObject = new JsonObject();
                payload.getClientAttributes().forEach(addToObject(attrObject));
                result.add("client", attrObject);
            }
            if (!payload.getSharedAttributes().isEmpty()) {
                JsonObject attrObject = new JsonObject();
                payload.getSharedAttributes().forEach(addToObject(attrObject));
                result.add("shared", attrObject);
            }
        } else {
            payload.getClientAttributes().forEach(addToObject(result));
            payload.getSharedAttributes().forEach(addToObject(result));
        }
        if (!payload.getDeletedAttributes().isEmpty()) {
            JsonArray attrObject = new JsonArray();
            payload.getDeletedAttributes().forEach(addToObject(attrObject));
            result.add("deleted", attrObject);
        }
        return result;
    }

    public static JsonObject getJsonObjectForGateway(String deviceName, TransportProtos.GetAttributeResponseMsg responseMsg) {
        JsonObject result = new JsonObject();
        result.addProperty("id", responseMsg.getRequestId());
        result.addProperty(DEVICE_PROPERTY, deviceName);
        if (responseMsg.getClientAttributeListCount() > 0) {
            addValues(result, responseMsg.getClientAttributeListList());
        }
        if (responseMsg.getSharedAttributeListCount() > 0) {
            addValues(result, responseMsg.getSharedAttributeListList());
        }
        return result;
    }

    public static JsonObject getJsonObjectForGateway(String deviceName, AttributeUpdateNotificationMsg notificationMsg) {
        JsonObject result = new JsonObject();
        result.addProperty(DEVICE_PROPERTY, deviceName);
        result.add("data", toJson(notificationMsg));
        return result;
    }

    private static void addValues(JsonObject result, List<TransportProtos.TsKvProto> kvList) {
        if (kvList.size() == 1) {
            addValueToJson(result, "value", kvList.get(0).getKv());
        } else {
            JsonObject values;
            if (result.has("values")) {
                values = result.get("values").getAsJsonObject();
            } else {
                values = new JsonObject();
                result.add("values", values);
            }
            kvList.forEach(value -> addValueToJson(values, value.getKv().getKey(), value.getKv()));
        }
    }

    private static void addValueToJson(JsonObject json, String name, TransportProtos.KeyValueProto entry) {
        switch (entry.getType()) {
            case BOOLEAN_V:
                json.addProperty(name, entry.getBoolV());
                break;
            case STRING_V:
                json.addProperty(name, entry.getStringV());
                break;
            case DOUBLE_V:
                json.addProperty(name, entry.getDoubleV());
                break;
            case LONG_V:
                json.addProperty(name, entry.getLongV());
                break;
        }
    }

    private static Consumer<AttributeKey> addToObject(JsonArray result) {
        return key -> result.add(key.getAttributeKey());
    }

    private static Consumer<TsKvProto> addToObjectFromProto(JsonObject result) {
        return de -> {
            JsonPrimitive value;
            switch (de.getKv().getType()) {
                case BOOLEAN_V:
                    value = new JsonPrimitive(de.getKv().getBoolV());
                    break;
                case DOUBLE_V:
                    value = new JsonPrimitive(de.getKv().getDoubleV());
                    break;
                case LONG_V:
                    value = new JsonPrimitive(de.getKv().getLongV());
                    break;
                case STRING_V:
                    value = new JsonPrimitive(de.getKv().getStringV());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + de.getKv().getType());
            }
            result.add(de.getKv().getKey(), value);
        };
    }

    private static Consumer<AttributeKvEntry> addToObject(JsonObject result) {
        return de -> {
            JsonPrimitive value;
            switch (de.getDataType()) {
                case BOOLEAN:
                    value = new JsonPrimitive(de.getBooleanValue().get());
                    break;
                case DOUBLE:
                    value = new JsonPrimitive(de.getDoubleValue().get());
                    break;
                case LONG:
                    value = new JsonPrimitive(de.getLongValue().get());
                    break;
                case STRING:
                    value = new JsonPrimitive(de.getStrValue().get());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported data type: " + de.getDataType());
            }
            result.add(de.getKey(), value);
        };
    }

    public static JsonElement toJson(TransportProtos.ToServerRpcResponseMsg msg) {
        if (StringUtils.isEmpty(msg.getError())) {
            return new JsonParser().parse(msg.getPayload());
        } else {
            JsonObject errorMsg = new JsonObject();
            errorMsg.addProperty("error", msg.getError());
            return errorMsg;
        }
    }

    public static JsonElement toErrorJson(String errorMsg) {
        JsonObject error = new JsonObject();
        error.addProperty("error", errorMsg);
        return error;
    }

    public static JsonElement toGatewayJson(String deviceName, TransportProtos.ToDeviceRpcRequestMsg rpcRequest) {
        JsonObject result = new JsonObject();
        result.addProperty(DEVICE_PROPERTY, deviceName);
        result.add("data", JsonConverter.toJson(rpcRequest, true));
        return result;
    }

    public static Set<AttributeKvEntry> convertToAttributes(JsonElement element) {
        Set<AttributeKvEntry> result = new HashSet<>();
        long ts = System.currentTimeMillis();
        result.addAll(parseValues(element.getAsJsonObject()).stream().map(kv -> new BaseAttributeKvEntry(kv, ts)).collect(Collectors.toList()));
        return result;
    }

    /**
     * 复杂节点解析，未考虑Json数组。
     * @param valuesObject: Json对象
     * @param strPrekey: 前置key名称
     * @return 返回基本类型的List以及剩余复杂类型的JsonElement
     */
    private static Pair<List<KvEntry>, Pair<String, JsonElement>> parseMultiJsonNode(JsonObject valuesObject, String strPrekey) {
        List<KvEntry> result = new ArrayList<>();
        Pair<String, JsonElement> cNextNode = null;

        for (Entry<String, JsonElement> valueEntry : valuesObject.entrySet()) {
            JsonElement element = valueEntry.getValue();
            if (element.isJsonPrimitive()) {
                JsonPrimitive value = element.getAsJsonPrimitive();
                if (value.isString()) {
                    if (maxStringValueLength > 0 && value.getAsString().length() > maxStringValueLength) {
                        String message = String.format("String value length [%d] for key [%s] is greater than maximum allowed [%d]", value.getAsString().length(), strPrekey+"."+valueEntry.getKey(), maxStringValueLength);
                        throw new JsonSyntaxException(message);
                    }
                    if(isTypeCastEnabled && NumberUtils.isParsable(value.getAsString())) {
                        try {
                            if (value.getAsString().contains(".")) {
                                result.add(new DoubleDataEntry(strPrekey+"."+valueEntry.getKey(), value.getAsDouble()));
                            } else {
                                try {
                                    long longValue = Long.parseLong(value.getAsString());
                                    result.add(new LongDataEntry(strPrekey+"."+valueEntry.getKey(), longValue));
                                } catch (NumberFormatException e) {
                                    throw new JsonSyntaxException("Big integer values are not supported!");
                                }
                            }
                        } catch (RuntimeException th) {
                            result.add(new StringDataEntry(strPrekey+"."+valueEntry.getKey(), value.getAsString()));
                        }
                    } else {
                        result.add(new StringDataEntry(strPrekey+"."+valueEntry.getKey(), value.getAsString()));
                    }
                } else if (value.isBoolean()) {
                    result.add(new BooleanDataEntry(strPrekey+"."+valueEntry.getKey(), value.getAsBoolean()));
                } else if (value.isNumber()) {
                    if (value.getAsString().contains(".")) {
                        result.add(new DoubleDataEntry(strPrekey+"."+valueEntry.getKey(), value.getAsDouble()));
                    } else {
                        try {
                            long longValue = Long.parseLong(value.getAsString());
                            result.add(new LongDataEntry(strPrekey+"."+valueEntry.getKey(), longValue));
                        } catch (NumberFormatException e) {
                            throw new JsonSyntaxException("Big integer values are not supported!");
                        }
                    }
                } else {
                    throw new JsonSyntaxException(CAN_T_PARSE_VALUE + value);
                }
            } else {
                cNextNode = new Pair<>(strPrekey+"."+valueEntry.getKey(), element);
            }
        }

        return new Pair<>(result, cNextNode);
    }

    private static List<KvEntry> parseValues(JsonObject valuesObject) {
        List<KvEntry> result = new ArrayList<>();
        for (Entry<String, JsonElement> valueEntry : valuesObject.entrySet()) {
            JsonElement element = valueEntry.getValue();
            if (element.isJsonPrimitive()) {
                JsonPrimitive value = element.getAsJsonPrimitive();
                if (value.isString()) {
                    if (maxStringValueLength > 0 && value.getAsString().length() > maxStringValueLength) {
                        String message = String.format("String value length [%d] for key [%s] is greater than maximum allowed [%d]", value.getAsString().length(), valueEntry.getKey(), maxStringValueLength);
                        throw new JsonSyntaxException(message);
                    }
                    if(isTypeCastEnabled && NumberUtils.isParsable(value.getAsString())) {
                        try {
                            parseNumericValue(result, valueEntry, value);
                        } catch (RuntimeException th) {
                            result.add(new StringDataEntry(valueEntry.getKey(), value.getAsString()));
                        }
                    } else {
                        result.add(new StringDataEntry(valueEntry.getKey(), value.getAsString()));
                    }
                } else if (value.isBoolean()) {
                    result.add(new BooleanDataEntry(valueEntry.getKey(), value.getAsBoolean()));
                } else if (value.isNumber()) {
                    parseNumericValue(result, valueEntry, value);
                } else {
                    throw new JsonSyntaxException(CAN_T_PARSE_VALUE + value);
                }
            } else {
                System.out.println(element);
//                // 设置key name
//                String strKeyName = valueEntry.getKey();
//                // 循环解析
//                for (JsonElement cIteraElement = element; null!=cIteraElement; ) {
//                    // 进行下一层解析
//                    Pair<List<KvEntry>, Pair<String, JsonElement>> cResultPair = parseMultiJsonNode(cIteraElement.getAsJsonObject(), strKeyName);
//                    // 解析数据插入list
//                    result.addAll(cResultPair.getKey());
//                    // 获取下一个复杂节点
//                    strKeyName = cResultPair.getValue().getKey();
//                    cIteraElement = cResultPair.getValue().getValue();
//                }
//                throw new JsonSyntaxException(CAN_T_PARSE_VALUE + element);
            }
        }
        return result;
    }

    public static Map<Long, List<KvEntry>> convertToTelemetry(JsonElement jsonObject, long systemTs) throws JsonSyntaxException {
        Map<Long, List<KvEntry>> result = new HashMap<>();
        if (jsonObject.isJsonObject()) {
            parseObject(result, systemTs, jsonObject);
        } else if (jsonObject.isJsonArray()) {
            jsonObject.getAsJsonArray().forEach(je -> {
                if (je.isJsonObject()) {
                    parseObject(result, systemTs, je.getAsJsonObject());
                } else {
                    throw new JsonSyntaxException(CAN_T_PARSE_VALUE + je);
                }
            });
        } else {
            throw new JsonSyntaxException(CAN_T_PARSE_VALUE + jsonObject);
        }
        return result;
    }

    private static void parseObject(Map<Long, List<KvEntry>> result, long systemTs, JsonElement jsonObject) {
        JsonObject jo = jsonObject.getAsJsonObject();
        if (jo.has("ts") && jo.has("values")) {
            parseWithTs(result, jo);
        } else {
            parseWithoutTs(result, systemTs, jo);
        }
    }

    private static void parseWithoutTs(Map<Long, List<KvEntry>> result, long systemTs, JsonObject jo) {
        for (KvEntry entry : parseValues(jo)) {
            result.computeIfAbsent(systemTs, tmp -> new ArrayList<>()).add(entry);
        }
    }

    public static void parseWithTs(Map<Long, List<KvEntry>> result, JsonObject jo) {
        long ts = jo.get("ts").getAsLong();
        JsonObject valuesObject = jo.get("values").getAsJsonObject();
        for (KvEntry entry : parseValues(valuesObject)) {
            result.computeIfAbsent(ts, tmp -> new ArrayList<>()).add(entry);
        }
    }

    public static void setTypeCastEnabled(boolean enabled) {
        isTypeCastEnabled = enabled;
    }

    public static void setMaxStringValueLength(int length) {
        maxStringValueLength = length;
    }

}
