package org.example.directives;

import io.cdap.wrangler.api.RecipePipeline;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.test.TestingRig;
import io.cdap.wrangler.test.api.TestRecipe;
import io.cdap.wrangler.test.api.TestRows;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests {@link SendToErrorMulti}
 */
public class SendToErrorMultiTest {
    @Test
    public void testBasicReverse() throws Exception {
        TestRecipe recipe = new TestRecipe();
        recipe.add("parse-as-csv :body ',';");
        recipe.add("drop :body");
        recipe.add("set-headers :encounter,:age,:first,:last;");
        // TODO: Add the send-to-error-multi directive with two conditions:
        // 1. checks that the encounter is empty
        // 2. checks that age is less than or equal to 0

        TestRows rows = new TestRows();
        rows.add(new Row("body", "en1,0,john,doe"));
        rows.add(new Row("body", "en2,20,samuel,jackson"));
        rows.add(new Row("body", ",30,sam,foe"));

        RecipePipeline pipeline = TestingRig.pipeline(SendToErrorMulti.class, recipe);
        List<Row> actual = pipeline.execute(rows.toList());
        List errors = pipeline.errors();

        Assert.assertEquals(1, actual.size());
        Assert.assertEquals(2, errors.size());
    }
}