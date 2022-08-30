package ir.sahab.hbaserule;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.*;

/**
 * JUnit 5 extension which provides a mini cluster of Hbase, DFS, and ZooKeeper.
 */
public class HbaseExtension extends HbaseBase
        implements BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

    public static final TestInstance.Lifecycle lifecycle = TestInstance.Lifecycle.PER_CLASS;

    private HbaseExtension() {
    }

    public static Builder<HbaseExtension> newBuilder() {
        return new Builder<>(new HbaseExtension());
    }

    @Override
    public void afterAll(ExtensionContext context) {
        if (lifecycle == TestInstance.Lifecycle.PER_CLASS) {
            after();
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        if (lifecycle == TestInstance.Lifecycle.PER_METHOD) {
            after();
        }
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        if (lifecycle == TestInstance.Lifecycle.PER_CLASS) {
            before();
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        if (lifecycle == TestInstance.Lifecycle.PER_METHOD) {
            before();
        }
    }

}
