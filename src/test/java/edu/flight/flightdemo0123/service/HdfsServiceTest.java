package edu.flight.flightdemo0123.service;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class HdfsServiceTest {

    @Autowired
    private HdfsService service;

    @Test
    void upload() throws Exception {
        service.uploadFile("src/main/resources/data_for_hadoop", "flightData");
    }

}