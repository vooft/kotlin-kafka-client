val publishAllTaskName = "publishAndReleaseToMavenCentralAll"
tasks.create(publishAllTaskName)

val publishAllToMavenLocalTaskName = "publishAllToMavenLocal"
tasks.create(publishAllToMavenLocalTaskName)

allprojects {
    group = "io.github.vooft"
    version = System.getenv("TAG") ?: "1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }

    tasks.findByName("publishAndReleaseToMavenCentralAll")?.dependsOn(publishAllTaskName)
    tasks.findByName("publishToMavenLocal")?.dependsOn(publishAllToMavenLocalTaskName)
}
