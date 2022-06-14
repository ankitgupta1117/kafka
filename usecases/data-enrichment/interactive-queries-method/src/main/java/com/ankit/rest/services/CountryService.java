package com.ankit.rest.services;

import com.ankit.kstreams.Country;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyQueryMetadata;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

@Service
@Slf4j
public class CountryService {

    public static final String COUNTRY_STORE = "country-store";

    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private RestTemplate restTemplate;
    @Value("${server.port}")
    private int port;

    @Autowired
    public CountryService(StreamsBuilderFactoryBean streamsBuilderFactoryBean, RestTemplateBuilder restTemplateBuilder) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
        this.restTemplate = restTemplateBuilder.build();
    }

    public String getCountry(String countryId){
        KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
        //Find the Instance containing key and possibly its value.
        KeyQueryMetadata instanceMetadataForKey = streams.queryMetadataForKey(COUNTRY_STORE, countryId, new StringSerializer());
        HostInfo hostInfo = instanceMetadataForKey.activeHost();
        //If key is present in local instance then get the store and fetch the value
        if(isThisHost(hostInfo)){
            ReadOnlyKeyValueStore<Object, Object> store = streams.store(StoreQueryParameters.fromNameAndType(COUNTRY_STORE, QueryableStoreTypes.keyValueStore()));
            Country country = (Country) store.get(countryId);
            return country.getCountry();
        }else{
            // Else execute a REST call to target instance to get the value
            log.info("Could not find country data in local instance. Calling api to target instance: {}:{}", hostInfo.host(), hostInfo.port());
            String url = new StringBuilder("http://")
                            .append(hostInfo.host())
                    .append(":").append(hostInfo.port())
                    .append("/api/country/")
                    .append(countryId)
                    .toString();
            return restTemplate.getForObject(URI.create(url), String.class);
        }
    }

    private boolean isThisHost(HostInfo hostInfo) {
        return hostInfo.host().equals("localhost") && hostInfo.port() == port;
    }
}
