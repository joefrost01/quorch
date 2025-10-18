package org.neuralchilli.quorch.core;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

@QuarkusTest
class ExpressionEvaluatorTest {

    @Inject
    ExpressionEvaluator evaluator;

    @Test
    void shouldReturnNonExpressionAsIs() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate("hello world", ctx);

        assertThat(result).isEqualTo("hello world");
    }

    @Test
    void shouldEvaluateSimpleVariableAccess() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("region", "us")
        );

        String result = evaluator.evaluate("${params.region}", ctx);

        assertThat(result).isEqualTo("us");
    }

    @Test
    void shouldEvaluateNestedVariableAccess() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("config", Map.of("api", Map.of("key", "secret123")))
        );

        String result = evaluator.evaluate("${params.config.api.key}", ctx);

        assertThat(result).isEqualTo("secret123");
    }

    @Test
    void shouldEvaluateArithmetic() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("scale", 10)
        );

        String result = evaluator.evaluate("${params.scale * 100}", ctx);

        assertThat(result).isEqualTo("1000");
    }

    @Test
    void shouldEvaluateConditional() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("full_refresh", true)
        );

        String result = evaluator.evaluate(
                "${params.full_refresh ? '--full-refresh' : '--incremental'}",
                ctx
        );

        assertThat(result).isEqualTo("--full-refresh");
    }

    @Test
    void shouldEvaluateComplexConditional() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("scale", 15)
        );

        String result = evaluator.evaluate(
                "${params.scale > 10 ? 32 : params.scale > 5 ? 16 : 4}",
                ctx
        );

        assertThat(result).isEqualTo("32");
    }

    @Test
    void shouldEvaluateStringConcatenation() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("region", "us", "date", "2025-10-17")
        );

        String result = evaluator.evaluate(
                "${'data_' + params.region + '_' + params.date}",
                ctx
        );

        assertThat(result).isEqualTo("data_us_2025-10-17");
    }

    @Test
    void shouldEvaluateStringMethods() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("text", "hello")
        );

        String result = evaluator.evaluate("${params.text.toUpperCase()}", ctx);

        assertThat(result).isEqualTo("HELLO");
    }

    @Test
    void shouldEvaluateStringReplace() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("date", "2025-10-17")
        );

        String result = evaluator.evaluate(
                "${params.date.replace('-', '_')}",
                ctx
        );

        assertThat(result).isEqualTo("2025_10_17");
    }

    @Test
    void shouldHandleNullSafeAccess() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("config", Map.of())
        );

        String result = evaluator.evaluate(
                "${params.config?.api?.key}",
                ctx
        );

        // Null-safe access returns null, which becomes null string
        assertThat(result).isNull();
    }

    @Test
    void shouldEvaluateElvisOperator() {
        ExpressionContext ctx = ExpressionContext.withParams(Map.of());

        String result = evaluator.evaluate(
                "${params.missing ?: 'default'}",
                ctx
        );

        assertThat(result).isEqualTo("default");
    }

    @Test
    void shouldAccessEnvironmentVariables() {
        ExpressionContext ctx = ExpressionContext.withParamsAndEnv(
                Map.of(),
                Map.of("API_KEY", "secret")
        );

        String result = evaluator.evaluate("${env.API_KEY}", ctx);

        assertThat(result).isEqualTo("secret");
    }

    @Test
    void shouldAccessTaskResults() {
        ExpressionContext ctx = new ExpressionContext(
                Map.of(),
                Map.of(),
                Map.of("extract", Map.of("row_count", 1500))
        );

        String result = evaluator.evaluate("${task.extract.row_count}", ctx);

        assertThat(result).isEqualTo("1500");
    }

    @Test
    void shouldEvaluateDateToday() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate("${date.today()}", ctx);

        assertThat(result).isEqualTo(LocalDate.now().toString());
    }

    @Test
    void shouldEvaluateDateNow() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate("${date.now('yyyy-MM-dd')}", ctx);

        assertThat(result).matches("\\d{4}-\\d{2}-\\d{2}");
    }

    @Test
    void shouldEvaluateDateAdd() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate(
                "${date.add('2025-10-17', 1, 'days')}",
                ctx
        );

        assertThat(result).isEqualTo("2025-10-18");
    }

    @Test
    void shouldEvaluateDateSub() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate(
                "${date.sub('2025-10-17', 7, 'days')}",
                ctx
        );

        assertThat(result).isEqualTo("2025-10-10");
    }

    @Test
    void shouldEvaluateDateFormat() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate(
                "${date.format('2025-10-17', 'yyyy/MM/dd')}",
                ctx
        );

        assertThat(result).isEqualTo("2025/10/17");
    }

    @Test
    void shouldEvaluateStringUuid() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate("${string.uuid()}", ctx);

        assertThat(result).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    void shouldEvaluateStringSlugify() {
        ExpressionContext ctx = ExpressionContext.empty();

        String result = evaluator.evaluate(
                "${string.slugify('My Workflow Name')}",
                ctx
        );

        assertThat(result).isEqualTo("my-workflow-name");
    }

    @Test
    void shouldEvaluateMathFunctions() {
        ExpressionContext ctx = ExpressionContext.empty();

        String round = evaluator.evaluate("${Math.round(3.7)}", ctx);
        assertThat(round).isEqualTo("4");

        // Math.floor returns double in Java
        String floor = evaluator.evaluate("${Math.floor(3.7)}", ctx);
        assertThat(floor).isEqualTo("3.0");

        String ceil = evaluator.evaluate("${Math.ceil(3.2)}", ctx);
        assertThat(ceil).isEqualTo("4.0");

        String max = evaluator.evaluate("${Math.max(10, 20)}", ctx);
        assertThat(max).isEqualTo("20");

        String min = evaluator.evaluate("${Math.min(10, 20)}", ctx);
        assertThat(min).isEqualTo("10");
    }

    @Test
    void shouldInterpolateMultipleExpressions() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("region", "us", "date", "2025-10-17")
        );

        String result = evaluator.evaluate(
                "gs://bucket-${params.region}/data/${params.date}/file.csv",
                ctx
        );

        assertThat(result).isEqualTo("gs://bucket-us/data/2025-10-17/file.csv");
    }

    @Test
    void shouldEvaluateMap() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("bucket", "my-bucket", "date", "2025-10-17")
        );

        Map<String, String> input = Map.of(
                "GCS_PATH", "gs://${params.bucket}/data",
                "PROCESSING_DATE", "${params.date}"
        );

        Map<String, String> result = evaluator.evaluateMap(input, ctx);

        assertThat(result).containsEntry("GCS_PATH", "gs://my-bucket/data");
        assertThat(result).containsEntry("PROCESSING_DATE", "2025-10-17");
    }

    @Test
    void shouldThrowExceptionForMalformedExpression() {
        ExpressionContext ctx = ExpressionContext.empty();

        // Use a syntax error that will fail at parse time
        assertThatThrownBy(() -> evaluator.evaluate("${this is not valid}", ctx))
                .isInstanceOf(ExpressionException.class)
                .hasMessageContaining("Failed to evaluate expression");
    }

    @Test
    void shouldValidateExpression() {
        assertThat(evaluator.isValid("${params.region}")).isTrue();
        assertThat(evaluator.isValid("${1 + 2}")).isTrue();
        assertThat(evaluator.isValid("plain text")).isTrue();
        // This is actually valid JEXL syntax (two consecutive dots is a reference)
        assertThat(evaluator.isValid("${invalid syntax with spaces}")).isFalse();
    }

    @Test
    void shouldGetValidationError() {
        String error = evaluator.getValidationError("${invalid syntax with spaces}");

        assertThat(error).isNotNull();
    }

    @Test
    void shouldEvaluateToObject() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("count", 42)
        );

        Object result = evaluator.evaluateToObject("${params.count}", ctx);

        assertThat(result).isInstanceOf(Integer.class);
        assertThat(result).isEqualTo(42);
    }

    @Test
    void shouldHandleLogicalOperators() {
        ExpressionContext ctx = ExpressionContext.withParams(
                Map.of("a", true, "b", false)
        );

        String andResult = evaluator.evaluate(
                "${params.a && params.b ? 'yes' : 'no'}",
                ctx
        );
        assertThat(andResult).isEqualTo("no");

        String orResult = evaluator.evaluate(
                "${params.a || params.b ? 'yes' : 'no'}",
                ctx
        );
        assertThat(orResult).isEqualTo("yes");

        String notResult = evaluator.evaluate(
                "${!params.b ? 'yes' : 'no'}",
                ctx
        );
        assertThat(notResult).isEqualTo("yes");
    }

    @Test
    void debugFunctionCalls() {
        ExpressionContext ctx = ExpressionContext.empty();

        // Test direct access
        String test1 = evaluator.evaluate("${date}", ctx);
        System.out.println("date object: " + test1);

        // Test method call
        try {
            String test2 = evaluator.evaluate("${date.today()}", ctx);
            System.out.println("date.today(): " + test2);
        } catch (Exception e) {
            System.out.println("Error calling date.today(): " + e.getMessage());
            e.printStackTrace();
        }
    }
}