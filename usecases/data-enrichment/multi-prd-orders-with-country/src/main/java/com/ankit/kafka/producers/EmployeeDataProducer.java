package com.ankit.kafka.producers;

import com.ankit.kstreams.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class EmployeeDataProducer {

    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;
    @Value("${employee.input.topic.name}")
    private String employeeTopic;

    private Logger logger = LoggerFactory.getLogger(EmployeeDataProducer.class);

    public void produce(Integer id, String name, String status, int address){
        Employee employee = new Employee(id, name, status, String.valueOf(address));
        kafkaTemplate.send(employeeTopic, String.valueOf(id), employee)
                    .addCallback(
                            (successResult) -> logger.info("Successfully produced Employee {} with key {}", successResult.getProducerRecord().value(),
                                                successResult.getProducerRecord().key())
                            ,(failureResult) -> logger.info("Unable to produce Employee. Message - ", failureResult.getMessage())
                    );
    }


}
