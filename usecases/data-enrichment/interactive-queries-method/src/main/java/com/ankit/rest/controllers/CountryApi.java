package com.ankit.rest.controllers;

import com.ankit.rest.services.CountryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/country")
public class CountryApi {

    private CountryService countryService;

    public CountryApi(CountryService countryService) {
        this.countryService = countryService;
    }

    @GetMapping("/{code}")
    public String getCountryByCode(@PathVariable String code){
        return countryService.getCountry(code);
    }

}
