package com.devkafka.cucumber;

import org.junit.platform.suite.api.ConfigurationParameter;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectClasspathResource;
import org.junit.platform.suite.api.Suite;

import static io.cucumber.junit.platform.engine.Constants.GLUE_PROPERTY_NAME;

/**
 * Named *IT on purpose, not *Test: these scenarios make real HTTP calls
 * against a running Kafka REST Proxy + Schema Registry, so they must NOT
 * run as part of the default mvn test/install (Surefire only picks up
 * *Test.java by default). Run explicitly once local-dev's docker-compose
 * stack is up (see source/README.md):
 *
 *   mvn test -Dtest=RunCucumberIT
 */
@Suite
@IncludeEngines("cucumber")
@SelectClasspathResource("features")
@ConfigurationParameter(key = GLUE_PROPERTY_NAME, value = "com.devkafka.cucumber")
public class RunCucumberIT {
}
