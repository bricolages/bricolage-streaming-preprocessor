# Bricolage Streaming Preprocessor

Bricolage Streaming Preprocessor service processes JSON data stream (S3 to S3).

This software is written in working time in Cookpad, Inc.

## Pre-requisites

- OpenJDK 11

## Development (Local)

Building Executable JAR file and running all tests:
```
% ./script/build
```

To run the application, you must copy config/*.example files to config/* and edit them.
Execute:
```
% (cd db && bundle && ./ridgepole.sh --merge)   # Migrates database schema
% ./script/server
```

## Building Docker Image

Copy following config files and edit it (DB host, port, user, password).
All config files has the corresponding example file (*.example), just copy and edit it.

- config.docker/streaming-preprocessor.yml
- config.docker/application.yml (for Spring)
- config.docker/logback.xml (for LogBack)
- env.docker

Then build image:
```
% ./gradlew build
% docker-compose build
```

Run:
```
% docker-compose -d up db    # Starts database in the background
% docker-compose up mig      # Migrates database schema and exits
% docker-compose up app      # Runs main application
```

## License

MIT license. See LICENSE file for details.

## Author

Minero Aoki
