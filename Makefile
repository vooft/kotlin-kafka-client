benchmark-macos:
	./gradlew :kotlin-kafka-client-benchmark:jvmBenchmarkJar
	java -XX:-BackgroundCompilation -jar kotlin-kafka-client-benchmark/build/benchmarks/jvm/jars/kotlin-kafka-client-benchmark-jvm-jmh-1.0-SNAPSHOT-JMH.jar \
 		-prof async:libPath=$(shell pwd)/kotlin-kafka-client-benchmark/libasyncProfiler/libasyncProfiler.dylib\;output=jfr\;dir=profile-results \
 		io.github.vooft.kafka.MultiplatformKafkaBenchmark

start-kafka:
	docker compose -f docker-compose.local.yaml rm -svf
	docker compose -f docker-compose.local.yaml up -d
	while ! nc -z localhost 9092 ; do sleep 1 ; done
	while ! nc -z localhost 9093 ; do sleep 1 ; done

