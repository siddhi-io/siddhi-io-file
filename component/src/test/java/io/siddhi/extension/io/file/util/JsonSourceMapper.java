/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.siddhi.extension.io.file.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.core.stream.input.source.AttributeMapping;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.core.stream.input.source.SourceMapper;
import io.siddhi.core.util.AttributeConverter;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import net.minidev.json.JSONArray;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;



/**
 * This mapper converts binary data related to json strings to
 * {@link io.siddhi.core.event.ComplexEventChunk}.
 * This is only for testing purposes.
 */

@Extension(
        name = "binary",
        namespace = "sourceMapper",
        description = "TBD",
        parameters = {},
        examples = {
                @Example(
                        syntax = "TBD",
                        description =  "TBD"
                )
        }
)

public class JsonSourceMapper extends SourceMapper {

    private static final String DEFAULT_JSON_MAPPING_PREFIX = "$.";
    private static final String DEFAULT_JSON_EVENT_IDENTIFIER = "event";
    private static final String DEFAULT_ENCLOSING_ELEMENT = "$";
    private static final String FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER = "fail.on.missing.attribute";
    private static final String ENCLOSING_ELEMENT_IDENTIFIER = "enclosing.element";
    private static final Logger log = Logger.getLogger(JsonSourceMapper.class);

    private StreamDefinition streamDefinition;
    private MappingPositionData[] mappingPositions;
    private List<Attribute> streamAttributes;
    private boolean isCustomMappingEnabled = false;
    private boolean failOnMissingAttribute = true;
    private String enclosingElement = null;
    private AttributeConverter attributeConverter = new AttributeConverter();
    private JsonFactory factory;
    private int attributesSize;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping> list,
                     ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        this.streamDefinition = streamDefinition;
        this.streamAttributes = this.streamDefinition.getAttributeList();
        attributesSize = this.streamDefinition.getAttributeList().size();
        this.mappingPositions = new MappingPositionData[attributesSize];
        failOnMissingAttribute = Boolean.parseBoolean(optionHolder.
                validateAndGetStaticValue(FAIL_ON_MISSING_ATTRIBUTE_IDENTIFIER, "true"));
        factory = new JsonFactory();
        if (list != null && list.size() > 0) {
            isCustomMappingEnabled = true;
            enclosingElement = optionHolder.validateAndGetStaticValue(ENCLOSING_ELEMENT_IDENTIFIER,
                    DEFAULT_ENCLOSING_ELEMENT);
            for (int i = 0; i < list.size(); i++) {
                AttributeMapping attributeMapping = list.get(i);
                String attributeName = attributeMapping.getName();
                int position;
                if (attributeName != null) {
                    position = this.streamDefinition.getAttributePosition(attributeName);
                } else {
                    position = i;
                }
                this.mappingPositions[i] = new MappingPositionData(position, attributeMapping.getMapping());
            }
        } else {
            for (int i = 0; i < attributesSize; i++) {
                this.mappingPositions[i] = new MappingPositionData(i, DEFAULT_JSON_MAPPING_PREFIX + this
                        .streamDefinition.getAttributeList().get(i).getName());
            }
        }
    }

    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler) throws InterruptedException {
        Object convertedEvent;
        convertedEvent = convertToEvent(eventObject);
        if (convertedEvent != null) {
            if (convertedEvent instanceof Event[]) {
                inputEventHandler.sendEvents((Event[]) convertedEvent);
            } else {
                inputEventHandler.sendEvent((Event) convertedEvent);
            }
        }
    }

    @Override
    protected boolean allowNullInTransportProperties() {
        return !failOnMissingAttribute;
    }

    /**
     * Convert the given JSON string to {@link Event}.
     *
     * @param eventObject JSON string
     * @return the constructed Event object
     */
    private Object convertToEvent(Object eventObject) {
        if (!(eventObject instanceof byte[])) {
            log.error("Invalid JSON object received. Expected a byte array, but found " +
                    eventObject.getClass()
                            .getCanonicalName());
            return null;
        }

        String tmpStr = new String((byte[]) eventObject);
        int splitPoint = tmpStr.indexOf("{");
        String eventString = tmpStr.substring(splitPoint);
        eventString = eventString.trim();

        if (!isJsonValid(eventString)) {
            log.error("Invalid Json String :" + eventString);
            return null;
        }

        Object jsonObj;
        ReadContext readContext = JsonPath.parse(eventString);
        if (isCustomMappingEnabled) {
            jsonObj = readContext.read(enclosingElement);
            if (jsonObj == null) {
                log.error("Enclosing element " + enclosingElement + " cannot be found in the json string " +
                        eventObject.toString() + ".");
                return null;
            }
            if (jsonObj instanceof JSONArray) {
                JSONArray jsonArray = (JSONArray) jsonObj;
                List<Event> eventList = new ArrayList<Event>();
                for (Object eventObj : jsonArray) {
                    Event event = processCustomEvent(JsonPath.parse(eventObj));
                    if (event != null) {
                        eventList.add(event);
                    }
                }
                Event[] eventArray = eventList.toArray(new Event[0]);
                return eventArray;
            } else {
                try {
                    Event event = processCustomEvent(JsonPath.parse(jsonObj));
                    return event;
                } catch (SiddhiAppRuntimeException e) {
                    log.error(e.getMessage());
                    return null;
                }
            }
        } else {
            jsonObj = readContext.read(DEFAULT_ENCLOSING_ELEMENT);
            if (jsonObj instanceof JSONArray) {
                return convertToEventArrayForDefaultMapping(eventString);
            } else {
                try {
                    return convertToSingleEventForDefaultMapping(eventString);
                } catch (IOException e) {
                    log.error("Json string " + eventObject + " cannot be parsed to json object.");
                    return null;
                }
            }
        }
    }

    private Event convertToSingleEventForDefaultMapping(String eventString) throws IOException {
        Event event = new Event(attributesSize);
        Object[] data = event.getData();
        JsonParser parser;
        int numberOfProvidedAttributes = 0;
        try {
            parser = factory.createParser(eventString);
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Initializing a parser failed for the event string."
                    + eventString);
        }
        int position;
        while (!parser.isClosed()) {
            JsonToken jsonToken = parser.nextToken();
            if (JsonToken.START_OBJECT.equals(jsonToken)) {
                parser.nextToken();
                if (DEFAULT_JSON_EVENT_IDENTIFIER.equalsIgnoreCase(parser.getText())) {
                    parser.nextToken();
                } else {
                    log.error("Default json message " + eventString
                            + " contains an invalid event identifier. Required \"event\", " +
                            "but found \"" + parser.getText() + "\". Hence dropping the message.");
                    return null;
                }
            } else if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                String key = parser.getCurrentName();
                numberOfProvidedAttributes++;
                position = findDefaultMappingPosition(key);
                if (position == -1) {
                    log.error("Stream \"" + streamDefinition.getId() +
                            "\" does not have an attribute named \"" + key +
                            "\", but the received event " + eventString +
                            " does. Hence dropping the message.");
                    return null;
                }
                jsonToken = parser.nextToken();
                Attribute.Type type = streamAttributes.get(position).getType();

                if (JsonToken.VALUE_NULL.equals(jsonToken)) {
                    data[position] = null;
                } else {
                    switch (type) {
                        case BOOL:
                            if (JsonToken.VALUE_TRUE.equals(jsonToken) || JsonToken.VALUE_FALSE.equals(jsonToken)) {
                                data[position] = parser.getValueAsBoolean();
                            } else {
                                log.error("Json message " + eventString +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type BOOL. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case INT:
                            if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                data[position] = parser.getValueAsInt();
                            } else {
                                log.error("Json message " + eventString +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type INT. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case DOUBLE:
                            if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken)) {
                                data[position] = parser.getValueAsDouble();
                            } else {
                                log.error("Json message " + eventString +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type DOUBLE. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case STRING:
                            if (JsonToken.VALUE_STRING.equals(jsonToken)) {
                                data[position] = parser.getValueAsString();
                            } else {
                                log.error("Json message " + eventString +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type STRING. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case FLOAT:
                            if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken) ||
                                    JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                data[position] = attributeConverter.getPropertyValue(parser.getValueAsString(),
                                        Attribute.Type.FLOAT);
                            } else {
                                log.error("Json message " + eventString +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type FLOAT. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        case LONG:
                            if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken)) {
                                data[position] = parser.getValueAsLong();
                            } else {
                                log.error("Json message " + eventString +
                                        " contains incompatible attribute types and values. Value " +
                                        parser.getText() + " is not compatible with type LONG. " +
                                        "Hence dropping the message.");
                                return null;
                            }
                            break;
                        default:
                            return null;
                    }
                }
            }
        }

        if (failOnMissingAttribute && (numberOfProvidedAttributes != attributesSize)) {
            log.error("Json message " + eventString +
                    " contains missing attributes. Hence dropping the message.");
            return null;
        }
        return event;
    }

    private Event[] convertToEventArrayForDefaultMapping(String eventString) {
        Gson gson = new Gson();
        JsonObject[] eventObjects = gson.fromJson(eventString, JsonObject[].class);
        Event[] events = new Event[eventObjects.length];
        int index = 0;
        JsonObject eventObj = null;
        for (JsonObject jsonEvent : eventObjects) {
            if (jsonEvent.has(DEFAULT_JSON_EVENT_IDENTIFIER)) {
                eventObj = jsonEvent.get(DEFAULT_JSON_EVENT_IDENTIFIER).getAsJsonObject();
                if (failOnMissingAttribute && eventObj.size() < streamAttributes.size()) {
                    log.error("Json message " + eventObj.toString() + " contains missing attributes. " +
                            "Hence dropping the message.");
                    continue;
                }
            } else {
                log.error("Default json message " + eventObj.toString()
                        + " in the array does not have the valid event identifier \"event\". " +
                        "Hence dropping the message.");
                continue;
            }
            Event event = new Event(streamAttributes.size());
            Object[] data = event.getData();


            int position = 0;
            for (Attribute attribute : streamAttributes) {
                String attributeName = attribute.getName();
                Attribute.Type type = attribute.getType();
                String attributeValue = eventObj.get(attributeName).getAsString();
                if (attributeValue == null) {
                    data[position++] = null;
                } else {
                    data[position++] = attributeConverter.getPropertyValue(
                            attributeValue, type);
                }
            }
            events[index++] = event;
        }
        return Arrays.copyOfRange(events, 0, index);
    }

    private Event processCustomEvent(ReadContext readContext) {
        Configuration conf = Configuration.defaultConfiguration();
        Event event = new Event(attributesSize);
        Object[] data = event.getData();
        Object childObject = readContext.read(DEFAULT_ENCLOSING_ELEMENT);
        readContext = JsonPath.using(conf).parse(childObject);
        for (MappingPositionData mappingPositionData : this.mappingPositions) {
            int position = mappingPositionData.getPosition();
            Object mappedValue;
            try {
                mappedValue = readContext.read(mappingPositionData.getMapping());
                if (mappedValue == null) {
                    data[position] = null;
                } else {
                    data[position] = attributeConverter.getPropertyValue(mappedValue.toString(),
                            streamAttributes.get(position).getType());
                }
            } catch (PathNotFoundException e) {
                if (failOnMissingAttribute) {
                    log.error("Json message " + childObject.toString() +
                            " contains missing attributes. Hence dropping the message.");
                    return null;
                }
                data[position] = null;
            }
        }
        return event;
    }

    private int findDefaultMappingPosition(String key) {
        for (int i = 0; i < streamAttributes.size(); i++) {
            String attributeName = streamAttributes.get(i).getName();
            if (attributeName.equals(key)) {
                return i;
            }
        }
        return -1;
    }

    private boolean isJsonValid(String jsonInString) {
        Gson gson = new Gson();
        try {
            gson.fromJson(jsonInString, Object.class);
            return true;
        } catch (com.google.gson.JsonSyntaxException ex) {
            return false;
        }
    }

    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * A POJO class which holds the attribute position in output stream and the user defined mapping.
     */
    private static class MappingPositionData {
        /**
         * Attribute position in the output stream.
         */
        private int position;

        /**
         * The JSON mapping as defined by the user.
         */
        private String mapping;

        public MappingPositionData(int position, String mapping) {
            this.position = position;
            this.mapping = mapping;
        }

        public int getPosition() {
            return position;
        }

        public void setPosition(int position) {
            this.position = position;
        }

        public String getMapping() {
            return mapping;
        }

        public void setMapping(String mapping) {
            this.mapping = mapping;
        }
    }
}
