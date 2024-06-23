import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest

fun runIntegrationTest(block: suspend TestScope.() -> Unit) = runTest {
    try {
        block()
    } catch (e: Exception) {
        @Suppress("detekt:PrintStackTrace")
        e.printStackTrace()
    }
}
