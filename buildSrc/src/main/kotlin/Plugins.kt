import org.gradle.plugin.use.PluginDependenciesSpec

val PluginDependenciesSpec.`kotlin-base` get() = id("kotlin-base")
val PluginDependenciesSpec.`kotlin-library` get() = id("kotlin-library")
val PluginDependenciesSpec.`maven-central-publish` get() = id("maven-central-publish")
