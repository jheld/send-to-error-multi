package org.example.directives;

import com.google.common.base.Joiner;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.wrangler.api.Arguments;
import io.cdap.wrangler.api.Directive;
import io.cdap.wrangler.api.DirectiveExecutionException;
import io.cdap.wrangler.api.DirectiveParseException;
import io.cdap.wrangler.api.ErrorRowException;
import io.cdap.wrangler.api.ExecutorContext;
import io.cdap.wrangler.api.Optional;
import io.cdap.wrangler.api.Row;
import io.cdap.wrangler.api.parser.Text;
import io.cdap.wrangler.api.annotations.Categories;
import io.cdap.wrangler.api.lineage.Lineage;
import io.cdap.wrangler.api.lineage.Mutation;
import io.cdap.wrangler.api.parser.TokenType;
import io.cdap.wrangler.api.parser.UsageDefinition;
import io.cdap.wrangler.expression.EL;
import io.cdap.wrangler.expression.ELContext;
import io.cdap.wrangler.expression.ELException;
import io.cdap.wrangler.expression.ELResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class <code>SendToErrorMulti</code>implements a <code>Directive</code> interface
 * for sending all respective matching errors on a given row to the error collector.
 */
@Plugin(type = Directive.TYPE)
@Name(SendToErrorMulti.NAME)
@Categories(categories = {"row", "data-quality"})
@Description("Send records and all respective matches of the provided conditions to the error collector.")
public final class SendToErrorMulti implements Directive, Lineage {
    public static final String NAME = "send-to-error-multi";
    private Map<String, ConditionInfo> conditions = new HashMap<>();
    
    private static final class ConditionInfo {
        private final EL compiledCondition;
        private final String metric;
	
        private ConditionInfo(EL compiledCondition, String metric) {
            this.compiledCondition = compiledCondition;
            this.metric = metric;
        }
    }
    
    @Override
    public UsageDefinition define() {
        UsageDefinition.Builder builder = UsageDefinition.builder(NAME);
        builder.define("conditions", TokenType.TEXT);
        builder.define("metrics", TokenType.TEXT, Optional.TRUE);
        return builder.build();

    }

    @Override
    public void initialize(Arguments args) throws DirectiveParseException {
        String conditionsParamValue =  ((Text) args.value("conditions")).value();
        String[] conditionsList = conditionsParamValue.split(",");
        String[] metricsList = null;
        // exists, since it is an optional parameter
        if (args.contains("metrics")) {
            String metricsParamValue = ((Text) args.value("metrics")).value();
            metricsList = metricsParamValue.split(",");
        }
        if (metricsList != null && conditionsList.length != metricsList.length) {
            throw new DirectiveParseException("Number of metrics should be equal to the number of conditions. Conditions = " +
					      conditionsList.length + "; metrics = " + metricsList.length);
        }
        // Populate the conditions map
        for (int i = 0; i < conditionsList.length; i++) {
            EL el;
	    
            try {
		el = EL.compile(conditionsList[i]);
            } catch (ELException e) {
                throw new DirectiveParseException(
						  String.format("Invalid conditions '%s'.", conditions)
						  );
            }
            String metric = metricsList != null ? metricsList[i] : null;
            conditions.put(conditionsList[i], new ConditionInfo(el, metric));
        }
    }

    @Override
    public List<Row> execute(List<Row> rows, ExecutorContext context)
	throws DirectiveExecutionException, ErrorRowException {
        List<Row> results = new ArrayList<>();
        for (Row row : rows) {
            // Move the fields from the row into the context.
            ELContext ctx = new ELContext();
            ctx.set("this", row);
            List<String> satisfiedConditions = new ArrayList<>();
            for (Map.Entry<String, ConditionInfo> conditionEntry : conditions.entrySet()) {
                String condition = conditionEntry.getKey();
                ConditionInfo conditionInfo = conditionEntry.getValue();
                EL el = conditionInfo.compiledCondition;
                String metric = conditionInfo.metric;
                for (String var : el.variables()) {
                    ctx.set(var, row.getValue(var));
                }

                // Transient variables are added.
                if (context != null) {
                    for (String variable : context.getTransientStore().getVariables()) {
                        ctx.set(variable, context.getTransientStore().get(variable));
                    }
                }

                // Execution of the script / expression based on the row data
                // mapped into context.
                try {
                    ELResult result = el.execute(ctx);
				       
                    if (result.getBoolean()) {
                        if (metric != null && context != null) {
                            context.getMetrics().count(metric, 1);
                        }
			satisfiedConditions.add(condition);
                    }
                } catch (ELException e) {
                    throw new DirectiveExecutionException(e.getMessage());
                }
            }
            if (!satisfiedConditions.isEmpty()) {
		throw new ErrorRowException(NAME, Joiner.on(",").join(satisfiedConditions), 1);
            }
            results.add(row);
        }
        return results;
    }


    @Override
    public Mutation lineage() {
	Mutation.Builder builder = Mutation.builder()
	    .readable("Redirecting records to error path based on expression '%s'", Joiner.on(",").join(conditions.keySet().toArray()));
	for (Map.Entry<String, ConditionInfo> conditionEntry : conditions.entrySet()) {
	    ConditionInfo conditionInfo = conditionEntry.getValue();
	    EL el = conditionInfo.compiledCondition;
	    el.variables().forEach(column -> builder.relation(column, column));
	}
	return builder.build();
    }

    @Override
    public void destroy() {
	// no-op
    }
}
