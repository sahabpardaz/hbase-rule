package ir.sahab.hbaserule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * JUnit 4 rule which provides a mini cluster of Hbase, DFS, and ZooKeeper.
 */
public class HbaseRule extends HbaseBase implements TestRule {

    private HbaseRule() {
    }

    public static Builder<HbaseRule> newBuilder() {
        return new Builder<>(new HbaseRule());
    }

    public Statement apply(Statement base, Description description) {
        return statement(base);
    }

    private Statement statement(final Statement base) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();

                List<Throwable> errors = new ArrayList<>();
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    try {
                        after();
                    } catch (Throwable t) {
                        errors.add(t);
                    }
                }
                MultipleFailureException.assertEmpty(errors);
            }
        };
    }

}
