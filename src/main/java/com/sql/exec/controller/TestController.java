package com.sql.exec.controller;

import com.sql.exec.perform.MapPerformanceTest;
import com.sql.exec.perform.PerformResult;
import com.sql.exec.perform.PerformanceTestBase;
import com.sql.exec.perform.PerformanceTestSetup;
import com.sql.exec.perform.PojoPerformanceTest;
import java.util.List;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Date: 2019-12-18
 */
@RequestMapping("/test")
@RestController
public class TestController {

    private PojoPerformanceTest pojoTest;
    private MapPerformanceTest mapTest;
    private PerformanceTestSetup before;

    public TestController() {
        before = new PerformanceTestSetup();
        before.setup();
    }

    @CrossOrigin("*")
    @GetMapping("/map")
    public ResponseEntity<List<PerformResult>> test() {
        mapTest = new MapPerformanceTest(before.getSql2o());

        return ResponseEntity.ok(mapTest.select());
    }

    @CrossOrigin("*")
    @GetMapping("/pojo")
    public ResponseEntity<List<PerformResult>>  pojoTest() {
        pojoTest = new PojoPerformanceTest(before.getSql2o());
        return ResponseEntity.ok(pojoTest.select());
    }
}
