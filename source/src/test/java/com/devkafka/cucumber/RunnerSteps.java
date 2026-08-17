package com.devkafka.cucumber;

import com.devkafka.avro.AvroDummyFiller;
import com.devkafka.avro.AvroJsonConverter;
import com.devkafka.client.KafkaRestProxyClientService;
import com.devkafka.client.SchemaRegistryClientService;
import com.devkafka.config.SchemaRegistryProperties;
import com.devkafka.exception.ErrorMessageException;
import com.devkafka.runner.GenericRunnerBean;
import com.devkafka.runner.SchemaDownload;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Cucumber glue for RunCucumberIT: exercises the library's own clients
 * (SchemaRegistryClientService, KafkaRestProxyClientService, SchemaDownload,
 * GenericRunnerBean) end-to-end against a real REST Proxy + Schema
 * Registry, instead of reimplementing HTTP calls by hand.
 */
@Slf4j
public class RunnerSteps {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SchemaRegistryClientService schemaRegistryClient =
            new SchemaRegistryClientService(new SchemaRegistryProperties());
    private final KafkaRestProxyClientService restProxyClient =
            new KafkaRestProxyClientService(new SchemaRegistryProperties());

    private String restProxy;
    private List<String> topicsList;
    private Path outputDirPath;
    private String schemaRegistry;
    private String urlPrefix;
    private List<Integer> versionesSubjectKey;
    private List<Integer> versionesSubjectValue;
    private boolean descargarEsquemas;
    private String ultimoTopicDescargado;
    private String keyPayload;
    private String valuePayload;
    private String topic;
    private boolean exito;
    private Exception excepcion;

    // ================================
    //  Helper que asegura que SIEMPRE
    //  hay ruta desde el feature
    // ================================
    private Path getOutputDirPath() {
        if (outputDirPath == null) {
            throw new IllegalStateException(
                    "No se ha establecido la carpeta de salida de esquemas Avro desde el feature.\n" +
                            "Anade: Y establezco la carpeta de salida de esquemas Avro a \"RUTA\"");
        }
        return outputDirPath;
    }

    // ========================================================
    // ==================      STEPS CUCUMBER     =============
    // ========================================================

    @Given("el REST Proxy {string}")
    public void el_rest_proxy(String proxyUrl) {
        this.restProxy = proxyUrl;
        log.info("REST Proxy configurado: {}", restProxy);
    }

    @And("el Schema Registry {string}")
    public void el_schema_registry(String registryUrl) {
        this.schemaRegistry = registryUrl;
        log.info("Schema Registry configurado: {}", schemaRegistry);
    }

    @And("configuro la descarga de esquemas Avro a {string}")
    public void configuro_descarga_esquemas(String opcion) {
        this.descargarEsquemas = Boolean.parseBoolean(opcion);
        log.info("Descarga de esquemas Avro activada: {}", descargarEsquemas);
    }

    @And("establezco la carpeta de salida de esquemas Avro a {string}")
    public void establezco_la_carpeta_salida_esquemas(String carpeta) {
        log.info("Carpeta destino para guardar esquemas: {}", carpeta);

        // Directorio base seguro para la salida de los tests
        Path baseTestOutput = Paths.get("target", "test-output", "schemas");

        // Valida y resuelve la ruta de entrada para prevenir Path Traversal
        Path requestedPath = Paths.get(carpeta);
        if (requestedPath.isAbsolute()) {
            throw new IllegalArgumentException(
                    "La carpeta de salida no puede ser una ruta absoluta. Ruta proporcionada: " + carpeta);
        }
        if (requestedPath.normalize().startsWith(Paths.get(".."))) {
            throw new IllegalArgumentException(
                    "La carpeta de salida no puede contener '..' para evitar Path Traversal. Ruta proporcionada: " + carpeta);
        }

        this.outputDirPath = baseTestOutput.resolve(requestedPath).normalize();

        try {
            Files.createDirectories(this.outputDirPath);
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo crear la carpeta de salida: " + this.outputDirPath, e);
        }
    }

    @When("consulto la lista de topics Avro desde el REST Proxy")
    public void consulto_lista_de_topics_avro_v2_desde_el_rest_proxy() {
        topicsList = restProxyClient.listTopics(restProxy);
        log.info("Topics recibidos: {}", topicsList);

        if (descargarEsquemas) {
            for (String topicName : topicsList) {
                comprobarYDescargarEsquema(topicName + "-key");
                comprobarYDescargarEsquema(topicName + "-value");
            }
        }
    }

    @When("descargo el esquema Avro del topic {string}")
    public void descargo_el_esquema_avro_v2_del_topic(String topic) {
        log.info("Descargando esquema Avro del topic: {}", topic);
        this.ultimoTopicDescargado = topic;

        comprobarYDescargarEsquema(topic + "-key");
        comprobarYDescargarEsquema(topic + "-value");
    }

    @Then("debo tener el esquema Avro \\(key y value) del topic indicado en el directorio de salida")
    public void debo_tener_el_esquema_avro_key_value_en_directorio() {
        assertNotNull(ultimoTopicDescargado, "No se ha descargado ningun topic en este escenario.");

        Path keyFile = getOutputDirPath().resolve(ultimoTopicDescargado + "-key.json");
        Path valueFile = getOutputDirPath().resolve(ultimoTopicDescargado + "-value.json");

        assertTrue(Files.exists(keyFile), "Falta el esquema KEY para el topic: " + ultimoTopicDescargado);
        assertTrue(Files.exists(valueFile), "Falta el esquema VALUE para el topic: " + ultimoTopicDescargado);

        log.info("Esquema KEY encontrado: {}", keyFile);
        log.info("Esquema VALUE encontrado: {}", valueFile);
    }

    private void comprobarYDescargarEsquema(String subject) {
        Path schemaFile = getOutputDirPath().resolve(subject + ".json");

        if (Files.exists(schemaFile)) {
            log.info("Esquema ya existe, se omite descarga: {}", schemaFile);
            return;
        }

        String schema = schemaRegistryClient.getLatestSchema(schemaRegistry, subject, urlPrefix);
        writeSchemaFile(schemaFile, schema);
    }

    private void writeSchemaFile(Path file, String schema) {
        try {
            JsonNode node = MAPPER.readTree(schema);
            String pretty = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(node);
            Files.writeString(file, pretty, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            log.info("Esquema guardado en: {}", file.toAbsolutePath());
        } catch (Exception e) {
            throw new IllegalStateException("No se pudo guardar el esquema en " + file, e);
        }
    }

    @And("genero datos dummy a partir de los esquemas Avro")
    public void genero_datos_dummy_a_partir_de_los_esquemas_avro() throws Exception {
        Path carpeta = getOutputDirPath();
        assertTrue(Files.exists(carpeta), "La carpeta de esquemas no existe: " + carpeta);

        List<Path> jsonFiles;
        try (var stream = Files.list(carpeta)) {
            jsonFiles = stream.filter(p -> p.toString().endsWith(".json")).toList();
        }
        assertFalse(jsonFiles.isEmpty(), "No se encontraron ficheros JSON en " + carpeta);

        Path dummyDir = carpeta.resolve("dummy");
        Files.createDirectories(dummyDir);

        log.info("Generando datos dummy para {} esquemas...", jsonFiles.size());

        for (Path schemaFile : jsonFiles) {
            String dummyFileName = schemaFile.getFileName().toString().replace(".json", "-dummy.json");
            writeDummyFor(schemaFile, dummyDir.resolve(dummyFileName));
        }
    }

    @And("envío los registros dummy Avro al entorno dev")
    public void envio_los_registros_dummy_avro_al_entorno_dev() {
        Path dummyDir = getOutputDirPath().resolve("dummy");
        assertTrue(Files.exists(dummyDir), "No existe la carpeta dummy: " + dummyDir);

        List<Path> dummyValueFiles;
        try (var stream = Files.list(dummyDir)) {
            dummyValueFiles = stream.filter(p -> p.toString().endsWith("-value-dummy.json")).toList();
        } catch (Exception e) {
            fail("Error listando ficheros dummy: " + e.getMessage());
            return;
        }
        assertFalse(dummyValueFiles.isEmpty(), "No se encontraron ficheros -value-dummy.json en " + dummyDir);

        SchemaDownload schemaDownload = new SchemaDownload(schemaRegistryClient, new SchemaRegistryProperties());
        GenericRunnerBean runner = new GenericRunnerBean(schemaDownload);

        for (Path dummyValueFile : dummyValueFiles) {
            String baseName = dummyValueFile.getFileName().toString().replace("-value-dummy.json", "");
            Path dummyKeyFile = dummyDir.resolve(baseName + "-key-dummy.json");

            if (!Files.exists(dummyKeyFile)) {
                log.warn("No se encontro dummy key para {}, se omite.", baseName);
                continue;
            }

            try {
                log.info("Enviando dummy al topic: {}", baseName);
                runner.run(
                        dummyKeyFile.toAbsolutePath().toString(),
                        dummyValueFile.toAbsolutePath().toString(),
                        schemaRegistry,
                        baseName,
                        restProxy,
                        urlPrefix
                );
                log.info("Dummy enviado correctamente a topic: {}", baseName);
            } catch (ErrorMessageException e) {
                log.warn("Error enviando dummy para {}: {}", baseName, e.getMessage());
            }
        }
    }

    @When("ejecuto GenericRunnerBean")
    public void ejecuto_generic_runner_bean() {
        log.info("Ejecutando GenericRunnerBean con key={}, value={}, topic={}, proxy={}, schemaRegistry={}, urlPrefix={}",
                keyPayload, valuePayload, topic, restProxy, schemaRegistry, urlPrefix);

        try {
            SchemaDownload schemaDownload = new SchemaDownload(schemaRegistryClient, new SchemaRegistryProperties());
            GenericRunnerBean runner = new GenericRunnerBean(schemaDownload);
            runner.run(keyPayload, valuePayload, schemaRegistry, topic, restProxy, urlPrefix);
            exito = true;
        } catch (ErrorMessageException e) {
            excepcion = e;
            exito = false;
            log.error("Error ejecutando GenericRunnerBean: {}", e.getMessage());
        }
    }

    @And("genero los datos dummy del topic {string}")
    public void genero_datos_dummy_solo_topic(String topic) throws Exception {
        Path schemaKey = getOutputDirPath().resolve(topic + "-key.json");
        Path schemaValue = getOutputDirPath().resolve(topic + "-value.json");

        assertTrue(Files.exists(schemaKey), "No se encontro el esquema KEY del topic: " + schemaKey);
        assertTrue(Files.exists(schemaValue), "No se encontro el esquema VALUE del topic: " + schemaValue);

        Path dummyDir = getOutputDirPath().resolve("dummy");
        Files.createDirectories(dummyDir);

        writeDummyFor(schemaKey, dummyDir.resolve(topic + "-key-dummy.json"));
        writeDummyFor(schemaValue, dummyDir.resolve(topic + "-value-dummy.json"));

        log.info("Dummy KEY y VALUE generados para el topic: {}", topic);
    }

    private void writeDummyFor(Path schemaFile, Path outputFile) throws Exception {
        String schemaContent = Files.readString(schemaFile, StandardCharsets.UTF_8);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaContent);

        // Not every schema is a record (e.g. a key schema is often a plain
        // "string"), so use the general-purpose generator instead of
        // assuming a record at the top level.
        Object dummyValue = AvroDummyFiller.generateDummyValue(schema);
        String jsonOutput = AvroJsonConverter.toJson(dummyValue, schema);

        Files.writeString(outputFile, jsonOutput, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("Dummy generado: {}", outputFile.toAbsolutePath());
    }

    @Given("un keyPayload {string}")
    public void un_keypayload(String keyPath) {
        this.keyPayload = keyPath;
    }

    @And("un valuePayload {string}")
    public void un_valuepayload(String valuePath) {
        this.valuePayload = valuePath;
    }

    @And("el topic {string}")
    public void el_topic(String topicName) {
        this.topic = topicName;
    }

    @And("el URL Prefix {string}")
    public void el_url_prefix(String prefix) {
        this.urlPrefix = prefix;
    }

    @Then("el mensaje debe enviarse correctamente")
    public void el_mensaje_debe_enviarse_correctamente() {
        if (!exito && excepcion != null && excepcion.getMessage() != null && excepcion.getMessage().contains("400")) {
            fail("Error 400: el mensaje no se envio correctamente al REST Proxy.");
        } else {
            assertTrue(exito, excepcion != null
                    ? "Fallo al enviar el mensaje: " + excepcion.getMessage()
                    : "GenericRunnerBean no ejecutado correctamente");
            log.info("El mensaje fue enviado exitosamente al REST Proxy.");
        }
    }

    @When("consulto todas las versiones de los subjects del topic usando el Schema Registry v6")
    public void consulto_todas_las_versiones_schema_registry_v6() {
        assertNotNull(topic, "No se ha definido el topic para consultar versiones.");

        String subjectKey = topic + "-key";
        String subjectValue = topic + "-value";

        log.info("Consultando versiones en SR para: {} y {}", subjectKey, subjectValue);

        versionesSubjectKey = schemaRegistryClient.listVersions(schemaRegistry, subjectKey);
        versionesSubjectValue = schemaRegistryClient.listVersions(schemaRegistry, subjectValue);

        log.info("Versiones encontradas para {}: {}", subjectKey, versionesSubjectKey);
        log.info("Versiones encontradas para {}: {}", subjectValue, versionesSubjectValue);
    }

    @Then("debo ver una lista de versiones disponibles para cada subject en v6")
    public void debo_ver_lista_versiones_subjects_v6() {
        assertNotNull(versionesSubjectKey, "No se obtuvieron versiones para subject-key");
        assertNotNull(versionesSubjectValue, "No se obtuvieron versiones para subject-value");

        assertFalse(versionesSubjectKey.isEmpty(), "No hay versiones del KEY");
        assertFalse(versionesSubjectValue.isEmpty(), "No hay versiones del VALUE");

        assertTrue(versionesSubjectKey.stream().allMatch(v -> v > 0));
        assertTrue(versionesSubjectValue.stream().allMatch(v -> v > 0));
    }

    @Then("debo recibir una lista de topics del REST Proxy")
    public void debo_recibir_una_lista_de_topics_v2_del_rest_proxy() {
        assertNotNull(topicsList, "La lista de topics es nula");
        assertFalse(topicsList.isEmpty(), "La lista de topics esta vacia");
        log.info("Lista de topics obtenida correctamente.");
    }
}
