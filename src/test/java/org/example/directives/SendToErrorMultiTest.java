package org.example.directives;

import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.test.TestingRig;
import io.cdap.wrangler.test.api.TestRecipe;
import io.cdap.wrangler.test.api.TestRows;
import io.cdap.wrangler.api.ErrorRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link SendToErrorMulti}
 */
public class SendToErrorMultiTest {

    @Test
    public void testErrorMultiMetric() throws Exception {
	TestRecipe recipe = new TestRecipe();
	recipe.add("parse-as-csv :body ',';");
	recipe.add("drop :body");
	recipe.add("set-headers :a,:b,:c;");

	recipe.add("send-to-error-multi \"!a.isEmpty(),b=='joltie'\" \"my_metric,joltie_metric\"");

	TestRows rows = new TestRows();
	rows.add(new Row("body", "root,joltie,mars avenue"));
	rows.add(new Row("body", ",root,venus blvd"));

	RecipePipeline pipeline = TestingRig.pipeline(SendToErrorMulti.class, recipe);
	List<Row> actual = pipeline.execute(rows.toList());
	List<ErrorRecord> errors = pipeline.errors();

	Assert.assertEquals(1, actual.size());
	Assert.assertEquals(1, errors.size());
	Assert.assertTrue(errors.get(0).getMessage().split(",")[1].startsWith("!a.isEmpty()"));
	Assert.assertTrue(errors.get(0).getMessage().split(",")[0].startsWith("b=='joltie'"));

	Assert.assertEquals("", actual.get(0).getValue("a"));
	Assert.assertEquals("root", actual.get(0).getValue("b"));
    }
    @Test
    public void testErrorMultiNoMetric() throws Exception {
	TestRecipe recipe = new TestRecipe();
	recipe.add("parse-as-csv :body ',';");
	recipe.add("drop :body");
	recipe.add("set-headers :a,:b,:c;");

	recipe.add("send-to-error-multi \"!a.isEmpty(),b=='joltie'\"");

	TestRows rows = new TestRows();
	rows.add(new Row("body", "root,joltie,mars avenue"));
	rows.add(new Row("body", ",root,venus blvd"));

	RecipePipeline pipeline = TestingRig.pipeline(SendToErrorMulti.class, recipe);
	List<Row> actual = pipeline.execute(rows.toList());
	List<ErrorRecord> errors = pipeline.errors();

	Assert.assertEquals(1, actual.size());
	Assert.assertEquals(1, errors.size());
	Assert.assertTrue(errors.get(0).getMessage().split(",")[1].startsWith("!a.isEmpty()"));
	Assert.assertTrue(errors.get(0).getMessage().split(",")[0].startsWith("b=='joltie'"));

	Assert.assertEquals("", actual.get(0).getValue("a"));
	Assert.assertEquals("root", actual.get(0).getValue("b"));

    }
}
