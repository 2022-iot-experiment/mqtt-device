package com.example.mqttdevice;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@EnableScheduling
public class UploadService {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HumitureData {
        long ts;
        float value;
    }

    /**
     * 数据集起始时间为 2017-03-09 09:11:35 1489021895
     * 加上偏移量偏移到 2021-03-09 09:11:35 1615252295
     */
    static final long TS_OFFSET = 126230400;

    /**
     * TB 默认的时间戳是毫秒级
     */
    static final long TS_FACTOR = 1000;

    /**
     * 模拟起始时间为 2021-03-09 09:00:00
     */
    static final long START_TIME = 1615251600;

    /**
     * 结束时间为一个月后 2021-04-09 09:00:00
     */
    static final long END_TIME = 1617930000;

    /**
     * 时间流动系数
     */
    static final long FACTOR = 500;

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    MqttTemperatureGateway mqttTemperatureGateway;

    @Autowired
    MqttHumidityGateway mqttHumidityGateway;

    long curTime = START_TIME;

    CSVParser humidityParser;
    CSVParser temperatureParser;

    Iterator<CSVRecord> humidityIt;
    Iterator<CSVRecord> temperatureIt;

    HumitureData curHumidity;
    HumitureData curTemperature;

    CSVParser readCsv(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        InputStream stream = resource.getInputStream();
        InputStreamReader reader = new InputStreamReader(stream);

        return new CSVParser(reader, CSVFormat.TDF);
    }

    HumitureData parseData(CSVRecord r) {
        return new HumitureData((Long.valueOf(r.get(0)) + TS_OFFSET) * TS_FACTOR, Float.valueOf(r.get(1)));
    }

    @PostConstruct
    void initReader() throws IOException {
        humidityParser = readCsv("humiture/Room1_Humidity.csv");
        temperatureParser = readCsv("humiture/Room1_Temperature.csv");

        humidityIt = humidityParser.iterator();
        temperatureIt = temperatureParser.iterator();

        if (humidityIt.hasNext())
            curHumidity = parseData(humidityIt.next());
        if (temperatureIt.hasNext())
            curTemperature = parseData(temperatureIt.next());

        log.info("温湿度数据上传开始, curTime: {}", curTime);
    }

    @PreDestroy
    void closeReader() throws IOException {
        humidityParser.close();
        temperatureParser.close();
    }

    /**
     * 每一秒进行一次上传
     */
    @Scheduled(cron = "0/1 * * * * *")
    void uploadData() throws JsonProcessingException {
        if (curTime >= END_TIME)
            return;

        curTime += 1L * FACTOR;

        if (curTime >= END_TIME)
            log.info("温湿度数据上传结束, curTime: {}", curTime);

        while (curHumidity != null && curHumidity.ts <= curTime * TS_FACTOR) {
            mqttHumidityGateway.sendToMqtt(objectMapper.writeValueAsString(curHumidity), "v1/devices/me/telemetry");
            if (humidityIt.hasNext())
                curHumidity = parseData(humidityIt.next());
            else
                curHumidity = null;
        }

        while (curTemperature != null && curTemperature.ts <= curTime * TS_FACTOR) {
            mqttTemperatureGateway.sendToMqtt(objectMapper.writeValueAsString(curTemperature),
                    "v1/devices/me/telemetry");
            if (temperatureIt.hasNext())
                curTemperature = parseData(temperatureIt.next());
            else
                curTemperature = null;
        }
    }
}
