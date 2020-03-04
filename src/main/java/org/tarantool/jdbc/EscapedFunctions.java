package org.tarantool.jdbc;

import org.tarantool.util.ServerVersion;
import org.tarantool.util.ThrowingBiFunction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Supported escaped function by Tarantool JDBC driver.
 *
 * <table>
 *     <caption>Supported numeric scalar functions</caption>
 *     <tr>
 *         <th>JDBC escape</th>
 *         <th>Native</th>
 *         <th>Comment</th>
 *     </tr>
 *     <tr>
 *         <td>ABS(number)</td>
 *         <td>ABS(number)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>PI()</td>
 *         <td>3.141592653589793</td>
 *         <td>Driver replaces the function to Math.PI constant</td>
 *     </tr>
 *     <tr>
 *         <td>RAND(seed)</td>
 *         <td>0.6005595572679824</td>
 *         <td>
 *             The driver replaces the function to the decimal value
 *             0 <= x < 1 using Random.nextDouble(). Seed parameter is ignored
 *         </td>
 *     </tr>
 *     <tr>
 *         <td>ROUND(number, places)</td>
 *         <td>ROUND(number, places)</td>
 *         <td></td>
 *     </tr>
 * </table>
 * <p>
 * <table>
 *     <caption>Supported string scalar functions</caption>
 *     <tr>
 *         <th>JDBC escape</th>
 *         <th>Native</th>
 *         <th>Comment</th>
 *     </tr>
 *     <tr>
 *         <td>CHAR(code)</td>
 *         <td>CHAR(code)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>CHAR_LENGTH(code [, CHARACTERS | OCTETS])</td>
 *         <td>CHAR_LENGTH(code)</td>
 *         <td>Last optional parameters is not supported</td>
 *     </tr>
 *     <tr>
 *         <td>CHARACTER_LENGTH(code [, CHARACTERS | OCTETS])</td>
 *         <td>CHARACTER_LENGTH(code)</td>
 *         <td>Last optional parameters is not supported</td>
 *     </tr>
 *     <tr>
 *         <td>CONCAT(string1, string2)</td>
 *         <td>(string1 || string2)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>LCASE(string)</td>
 *         <td>LOWER(string)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>LEFT(string, count)</td>
 *         <td>SUBSTR(string, 1, count)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>LENGTH(string, [, CHARACTERS | OCTETS])</td>
 *         <td>LENGTH(TRIM(TRAILING FROM string))</td>
 *         <td>Last optional parameters is not supported</td>
 *     </tr>
 *     <tr>
 *         <td>LTRIM(string)</td>
 *         <td>
 *             LTRIM(string) for Tarantool < 2.2
 *             TRIM(LEADING FROM string) for Tarantool >= 2.2
 *         </td>
 *         <td>
 *             Tarantool 2.1 supports SQLite compatible function LTRIM,
 *             since 2.2 Tarantool supports ANSI SQL standard using TRIM(LEADING FROM) expression
 *         </td>
 *     </tr>
 *     <tr>
 *         <td>REPLACE(string1, string2, string3)</td>
 *         <td>REPLACE(string1, string2, string3)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>RIGHT(string, count)</td>
 *         <td>SUBSTR(string, -(count))</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>RTRIM(string)</td>
 *         <td>
 *             LTRIM(string) (for Tarantool < 2.2)
 *             TRIM(TRAILING FROM string) (for Tarantool >= 2.2)
 *         </td>
 *         <td>
 *             Tarantool 2.1 supports SQLite compatible function LTRIM,
 *             since 2.2 Tarantool supports ANSI SQL standard using TRIM(TRAILING FROM) expression
 *         </td>
 *     </tr>
 *     <tr>
 *         <td>SOUNDEX(string)</td>
 *         <td>SOUNDEX(string)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>SUBSTRING(string, start, length [, CHARACTERS | OCTETS])</td>
 *         <td>SUBSTR(string, start, length)</td>
 *         <td>Last optional parameters is not supported</td>
 *     </tr>
 *     <tr>
 *         <td>UCASE(string)</td>
 *         <td>UPPER(string)</td>
 *         <td></td>
 *     </tr>
 * </table>
 * <p>
 * <table>
 *     <caption>Supported system scalar functions</caption>
 *     <tr>
 *         <th>JDBC escape</th>
 *         <th>Native</th>
 *         <th>Comment</th>
 *     </tr>
 *     <tr>
 *         <td>DATABASE()</td>
 *         <td>'universe'</td>
 *         <td>Tarantool does not support databases. Driver always replaces it to 'universe'.</td>
 *     </tr>
 *     <tr>
 *         <td>IFNULL(expression1, expression2)</td>
 *         <td>IFNULL(expression1, expression2)</td>
 *         <td></td>
 *     </tr>
 *     <tr>
 *         <td>USER()</td>
 *         <td>'guest'</td>
 *         <td>Driver replaces the function to the current user name.</td>
 *     </tr>
 * </table>
 */
public class EscapedFunctions {

    private static Random cachedRandom = new Random();

    /**
     * Supported numeric scalar functions.
     */
    public enum NumericFunction {
        ABS, PI, RAND, ROUND
    }

    /**
     * Supported string scalar functions.
     */
    public enum StringFunction {
        CHAR,
        CHAR_LENGTH,
        CHARACTER_LENGTH,
        CONCAT,
        LCASE,
        LEFT,
        LENGTH,
        LTRIM,
        REPLACE,
        RIGHT,
        RTRIM,
        SOUNDEX,
        SUBSTRING,
        UCASE
    }

    /**
     * Supported system scalar functions.
     */
    public enum SystemFunction {
        DATABASE, IFNULL, USER
    }

    static Map<FunctionSignatureKey, TranslationFunction> functionMappings;

    static {
        functionMappings = new HashMap<>(128);
        // C.1 numeric scalar function
        functionMappings.put(
            FunctionSignatureKey.of(NumericFunction.ABS.name(), 1),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(NumericFunction.PI.name(), 0),
            (exp, context) -> new NumericLiteral(Math.PI)
        );
        functionMappings.put(
            FunctionSignatureKey.of(NumericFunction.RAND.name(), 1),
            (exp, context) -> new NumericLiteral(cachedRandom.nextDouble())
        );
        functionMappings.put(
            FunctionSignatureKey.of(NumericFunction.ROUND.name(), 2),
            (exp, context) -> exp
        );

        // C.2 string scalar function
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.CHAR.name(), 1),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.CHAR_LENGTH.name(), 1),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.CHARACTER_LENGTH.name(), 1),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.CONCAT.name(), 2),
            (exp, context) -> {
                List<String> parameters = exp.getParameters();
                return new FunctionExpression(
                    "",
                    Collections.singletonList(parameters.get(0) + " || " + parameters.get(1))
                );
            }
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.LCASE.name(), 1),
            (exp, context) -> new FunctionExpression("LOWER", exp.getParameters())
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.LEFT.name(), 2),
            (exp, context) -> {
                List<String> parameters = exp.getParameters();
                return new FunctionExpression("SUBSTR", Arrays.asList(parameters.get(0), "1", parameters.get(1)));
            }
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.LENGTH.name(), 1),
            (exp, context) -> {
                String string = "TRIM(TRAILING FROM " + exp.getParameters().get(0) + ")";
                return new FunctionExpression("LENGTH", Collections.singletonList(string));
            }
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.LTRIM.name(), 1),
            (exp, context) -> {
                try {
                    TarantoolDatabaseMetaData metaData = context.getMetaData().unwrap(TarantoolDatabaseMetaData.class);
                    if (metaData.getDatabaseVersion().isLessThan(ServerVersion.V_2_2)) {
                        return exp;
                    }
                    String string = "LEADING FROM " + exp.getParameters().get(0);
                    return new FunctionExpression("TRIM", Collections.singletonList(string));
                } catch (SQLException cause) {
                    throw new SQLSyntaxErrorException("Unresolvable LTRIM function", cause);
                }
            }
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.REPLACE.name(), 3),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.RIGHT.name(), 2),
            (exp, context) -> {
                String string = exp.getParameters().get(0);
                String count = exp.getParameters().get(1);
                return new FunctionExpression("SUBSTR", Arrays.asList(string, "-(" + count + ")"));
            }
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.RTRIM.name(), 1),
            (exp, context) -> {
                try {
                    TarantoolDatabaseMetaData metaData = context.getMetaData().unwrap(TarantoolDatabaseMetaData.class);
                    ServerVersion databaseVersion = metaData.getDatabaseVersion();
                    if (databaseVersion.isLessThan(ServerVersion.V_2_2)) {
                        return exp;
                    }
                    String string = "TRAILING FROM " + exp.getParameters().get(0);
                    return new FunctionExpression("TRIM", Collections.singletonList(string));
                } catch (SQLException cause) {
                    throw new SQLSyntaxErrorException("Unresolvable RTRIM function", cause);
                }
            }
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.SOUNDEX.name(), 1),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.SUBSTRING.name(), 3),
            (exp, context) -> new FunctionExpression("SUBSTR", exp.getParameters())
        );
        functionMappings.put(
            FunctionSignatureKey.of(StringFunction.UCASE.name(), 1),
            (exp, context) -> new FunctionExpression("UPPER", exp.getParameters())
        );

        // C.4 system scalar functions
        functionMappings.put(
            FunctionSignatureKey.of(SystemFunction.DATABASE.name(), 0),
            (exp, context) -> new StringLiteral("universe")
        );
        functionMappings.put(
            FunctionSignatureKey.of(SystemFunction.IFNULL.name(), 2),
            (exp, context) -> exp
        );
        functionMappings.put(
            FunctionSignatureKey.of(SystemFunction.USER.name(), 0),
            (exp, context) -> {
                try {
                    return new StringLiteral(context.getMetaData().getUserName());
                } catch (SQLException e) {
                    throw new SQLSyntaxErrorException("User cannot be resolved", e.getSQLState(), e);
                }
            }
        );
    }

    interface TranslationFunction
        extends ThrowingBiFunction<FunctionExpression, Connection, Expression, SQLSyntaxErrorException> {

    }

    static class FunctionSignatureKey {

        String name;
        int parametersCount;

        static FunctionSignatureKey of(String name, int parametersCount) {
            FunctionSignatureKey key = new FunctionSignatureKey();
            key.name = name.toUpperCase();
            key.parametersCount = parametersCount;
            return key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionSignatureKey that = (FunctionSignatureKey) o;
            return parametersCount == that.parametersCount &&
                Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, parametersCount);
        }

    }

    interface Expression {

    }

    static class StringLiteral implements Expression {

        final String value;

        public StringLiteral(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "'" + value + "'";
        }

    }

    static class NumericLiteral implements Expression {

        final double number;

        public NumericLiteral(double number) {
            this.number = number;
        }

        @Override
        public String toString() {
            return Double.toString(number);
        }

    }

    static class FunctionExpression implements Expression {

        String name;
        List<String> parameters;

        FunctionExpression(String name, List<String> parameters) {
            this.name = name;
            this.parameters = parameters;
        }

        public String getName() {
            return name;
        }

        public List<String> getParameters() {
            return parameters;
        }

        @Override
        public String toString() {
            return name +
                "(" +
                String.join(", ", parameters) +
                ')';
        }

    }

}
