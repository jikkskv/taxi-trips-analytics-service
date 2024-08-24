# taxi-trips-analytics-service

Test project for analytics service for Taxi trips.

## Requirements
For building and running the application you need:

- [JDK 17](https://www.azul.com/downloads/?version=java-17-lts&os=linux&package=jdk#zulu)
- [Maven 3](https://maven.apache.org)
- [Spring Boot 3.3.2](https://github.com/spring-projects/spring-boot/wiki/Spring-Boot-3.3-Release-Notes)

## How to build
```shell
#Go to 'taxi-trips-analytics-service' folder 
sh bin/setup
```
This script will do below action:
1. Download the parquet file from provided gdrive path.
2. Download and Install dependencies
3. Compile the code
4. Run test cases
5. Print the code coverage(Instructions covered).

## How to Run
```shell
#Go to 'taxi-trips-analytics-service' folder
sh bin/run
```

## How to manually build
```shell
#Go to 'taxi-trips-analytics-service' folder
mvn -f ./pom.xml clean install
```
## How to manually test
```shell
#Go to 'taxi-trips-analytics-service' folder
mvn -f ./pom.xml test verify
```
## How to manually Run
```shell
#Go to 'taxi-trips-analytics-service' folder
java -jar -Dserver.port=$PORT ./trip-analytics-service.jar
```

## Swagger Url
http://localhost:8000/swagger-ui/index.html


## Implementation details
#### Initializer used:
[Spring Initializer](https://start.spring.io/)
#### Application Properties:

This section describes the configurable properties for the application. These properties can be set in the `application.properties` file or as environment variables.

##### General Configuration

| Property Name                                      | Default Value                                  | Description                                           |
|----------------------------------------------------|------------------------------------------------|-------------------------------------------------------|
| `parquet.file.download.location`                   | `file:./datasets/taxitrips/trips_dats.parquet` | The point to the location of downloaded parquet file. |
| `s2cellId.level`                                   | `16`                                           | This point to the geo level to calulate s2 cell id.  |
| `logging.level.com.xyztaxicompany.analytics.trips` | `INFO`                                         | The root logging level.                               |

