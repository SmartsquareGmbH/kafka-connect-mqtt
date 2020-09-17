ARG BUILD_HOME=/usr/app

FROM openjdk:8 AS BUILD
ARG BUILD_HOME
WORKDIR $BUILD_HOME
COPY build.gradle settings.gradle gradlew ./
COPY gradle ./gradle/
COPY . .
RUN ./gradlew clean jar copyRuntimeLibs --no-daemon

FROM strimzi/kafka:0.18.0-kafka-2.5.0
ARG BUILD_HOME
ENV KAFKA_LIB_DIR=/opt/kafka/libs
# Copy build-product
COPY --from=BUILD $BUILD_HOME/build/libs/ $KAFKA_LIB_DIR/
# Copy runtime libs
COPY --from=BUILD $BUILD_HOME/build/output/lib/ $KAFKA_LIB_DIR/
