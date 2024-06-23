plugins {
    id("kotlin-base")
}

kotlin {
    jvm()

    js {
        nodejs {
            testTask {
                useMocha {
                    timeout = "2m"
                }
            }
        }
        binaries.executable()
    }

    wasmJs {
        nodejs {
            testTask {
                useMocha {
                    timeout = "2m"
                }
            }
        }
        binaries.executable()
    }

    macosArm64()
    linuxX64()

    iosArm64()
    iosSimulatorArm64()

    applyDefaultHierarchyTemplate()
}
