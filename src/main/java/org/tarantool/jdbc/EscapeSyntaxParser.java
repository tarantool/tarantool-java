package org.tarantool.jdbc;

import static org.tarantool.jdbc.EscapeSyntaxParser.Comment.BLOCK;
import static org.tarantool.jdbc.EscapeSyntaxParser.Comment.LINE;
import static org.tarantool.jdbc.EscapedFunctions.Expression;
import static org.tarantool.jdbc.EscapedFunctions.FunctionExpression;
import static org.tarantool.jdbc.EscapedFunctions.FunctionSignatureKey;
import static org.tarantool.jdbc.EscapedFunctions.functionMappings;

import org.tarantool.util.SQLStates;
import org.tarantool.util.ThrowingBiFunction;

import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Set of utils to work with JDBC escape processing.
 * <p>
 * Supported escape syntax:
 * <ol>
 *  <li>Scalar functions (i.e. {@code {fn random()}}).</li>
 *  <li>Outer joins (i.e. {@code {oj "dept" left outer join "salary" on "dept_id" = 1412}}).</li>
 *  <li>Like escape character (i.e. {@code like '_|%_3%' {escape '|'}}).</li>
 *  <li>Limiting returned rows (i.e. {@code {limit 10 offset 20}}).</li>
 * </ol>
 *
 * <p>
 * Most of the supported expressions translates directly omitting escape borders.
 * In this way, {@code {fn abs(-5)}} becomes {@code abs(-5)}} or {@code {limit 10 offset 50}}
 * becomes {@code limit 10 offset 50} and so on. There are exceptions in case of scalar
 * functions where JDBC functions may not match exactly with Tarantool ones (for example,
 * JDBC {@code {fn rand()}} function becomes {@code random()} supported by Tarantool.
 *
 * <p>
 * Escape syntax explicitly do not allow or deny SQL comments within an escape expression.
 * To avoid undefined behaviours when processing is performed the parser always replaces
 * a comment with one whitespace.
 */
public class EscapeSyntaxParser {

    enum Comment {
        BLOCK("/*", "*/"),
        LINE("--", "\n");

        final String start;
        final String end;

        Comment(String start, String end) {
            this.start = start;
            this.end = end;
        }

        public String getStart() {
            return start;
        }

        public String getEnd() {
            return end;
        }
    }

    /**
     * Pattern that covers function names described in JDBC Spec
     * Appendix C. Scalar functions.
     */
    private static final Pattern IDENTIFIER = Pattern.compile("[_a-zA-Z][_a-zA-Z0-9]+");

    private final SQLConnection jdbcContext;

    public EscapeSyntaxParser(SQLConnection jdbcContext) {
        this.jdbcContext = jdbcContext;
    }

    /**
     * Performs escape processing for SQL queries. It translates
     * sql text with optional escape expressions such as {@code {fn abs(-1)}}.
     *
     * <p>
     * Comments inside SQL text can be eliminated as parsing goes using preserveComments
     * flag. Hence, Comments inside escape syntax are always omitted regardless of
     * the flag, though.
     *
     * @param sql SQL text to be processed
     *
     * @return native SQL query
     *
     * @throws SQLSyntaxErrorException if any syntax error happened
     */
    public String translate(String sql, boolean preserveComments) throws SQLSyntaxErrorException {
        StringBuilder nativeSql = new StringBuilder(sql.length());
        StringBuilder escapeBuffer = new StringBuilder();
        StringBuilder activeBuffer = nativeSql;
        LinkedList<Integer> escapeStartPositions = new LinkedList<>();

        int i = 0;
        while (i < sql.length()) {
            char currentChar = sql.charAt(i);
            switch (currentChar) {
            case '\'':
            case '"':
                int endOfString = seekEndOfRegion(sql, i, "" + currentChar, "" + currentChar);
                if (endOfString == -1) {
                    throw new SQLSyntaxErrorException(
                        "Not enclosed string literal or quoted identifier at position " + i,
                        SQLStates.SYNTAX_ERROR.getSqlState()
                    );
                }
                activeBuffer.append(sql, i, endOfString + 1);
                i = endOfString + 1;
                break;

            case '/':
            case '-':
                int endOfComment;
                if (currentChar == '/') {
                    endOfComment = seekEndOfRegion(sql, i, BLOCK.getStart(), BLOCK.getEnd());
                    if (endOfComment == -1) {
                        throw new SQLSyntaxErrorException(
                            "Open block comment at position " + i, SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                } else {
                    endOfComment = seekEndOfRegion(sql, i, LINE.getStart(), LINE.getEnd());
                    if (endOfComment == -1) {
                        endOfComment = sql.length() - 1;
                    }
                }
                if (i == endOfComment) {
                    activeBuffer.append(currentChar);
                    i++;
                } else {
                    if (preserveComments) {
                        activeBuffer.append(sql, i, endOfComment + 1);
                    } else {
                        activeBuffer.append(' ');
                    }
                    i = endOfComment + 1;
                }
                break;

            case '{':
                escapeStartPositions.addFirst(escapeBuffer.length());
                escapeBuffer.append(currentChar);
                activeBuffer = escapeBuffer;
                i++;
                break;

            case '}':
                Integer startPosition = escapeStartPositions.pollFirst();
                if (startPosition == null) {
                    throw new SQLSyntaxErrorException(
                        "Unexpected '}' at position " + i,
                        SQLStates.SYNTAX_ERROR.getSqlState()
                    );
                }
                escapeBuffer.append(currentChar);
                processEscapeExpression(escapeBuffer, startPosition, escapeBuffer.length());
                if (escapeStartPositions.isEmpty()) {
                    nativeSql.append(escapeBuffer);
                    escapeBuffer.setLength(0);
                    activeBuffer = nativeSql;
                }
                i++;
                break;

            default:
                activeBuffer.append(currentChar);
                i++;
                break;
            }
        }

        if (!escapeStartPositions.isEmpty()) {
            throw new SQLSyntaxErrorException(
                "Not enclosed escape expression at position " + escapeStartPositions.pollFirst(),
                SQLStates.SYNTAX_ERROR.getSqlState()
            );
        }
        return nativeSql.toString();
    }

    /**
     * Parses text like {@code functionName([arg[,args...]])}.
     * Arguments are not parsed recursively and saved as-is.
     *
     * <p>
     * In contrast to SQL where function name can be enclosed by double quotes,
     * it is not supported within escape syntax.
     *
     * @param functionString text to be parsed
     *
     * @return parsed result containing function name and its parameters, if any
     *
     * @throws SQLSyntaxErrorException if any syntax errors happened
     */
    private FunctionExpression parseFunction(String functionString) throws SQLSyntaxErrorException {
        int braceNestLevel = 0;
        String functionName = null;
        List<String> functionParameters = new ArrayList<>();
        int parameterStartPosition = 0;

        int i = 0;
        boolean completed = false;
        boolean wasComment = false;
        while (i < functionString.length() && !completed) {
            char currentChar = functionString.charAt(i);
            switch (currentChar) {
            case '\'':
            case '"':
                i = seekEndOfRegion(functionString, i, "" + currentChar, "" + currentChar) + 1;
                break;

            case '/':
            case '-':
                int endOfComment = (currentChar == '/')
                    ? seekEndOfRegion(functionString, i, BLOCK.getStart(), BLOCK.getEnd())
                    : seekEndOfRegion(functionString, i, LINE.getStart(), LINE.getEnd());
                wasComment = (i != endOfComment);
                i = endOfComment == -1 ? functionString.length() : endOfComment + 1;
                break;

            case '(':
                if (braceNestLevel++ == 0) {
                    functionName = trimExpression(functionString.substring(0, i), wasComment).toUpperCase();
                    if (!IDENTIFIER.matcher(functionName).matches()) {
                        throw new SQLSyntaxErrorException(
                            "Invalid function identifier '" + functionName + "'", SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                    parameterStartPosition = i + 1;
                    wasComment = false;
                }
                i++;
                break;

            case ')':
                if (--braceNestLevel == 0) {
                    // reach the function closing brace
                    // parse the last possible function parameter
                    String param = functionString.substring(parameterStartPosition, i);
                    String clearParam = trimExpression(param, wasComment);
                    if (!clearParam.isEmpty()) {
                        functionParameters.add(param.trim());
                    } else if (!functionParameters.isEmpty()) {
                        throw new SQLSyntaxErrorException(
                            "Empty function argument at " + (functionParameters.size() + 1) + " position",
                            SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                    completed = true;
                    wasComment = false;
                }
                i++;
                break;

            case ',':
                if (braceNestLevel == 1) {
                    // reach the function argument delimiter
                    // parse the argument before this comma
                    String param = functionString.substring(parameterStartPosition, i);
                    String clearParam = trimExpression(param, wasComment);
                    if (clearParam.isEmpty()) {
                        throw new SQLSyntaxErrorException(
                            "Empty function argument at " + (functionParameters.size() + 1) + " position",
                            SQLStates.SYNTAX_ERROR.getSqlState()
                        );
                    }
                    functionParameters.add(param.trim());
                    parameterStartPosition = i + 1;
                    wasComment = false;
                }
                i++;
                break;

            default:
                i++;
                break;
            }
        }

        if (functionName == null || !completed) {
            throw new SQLSyntaxErrorException(
                "Malformed function expression '" + functionString + "'", SQLStates.SYNTAX_ERROR.getSqlState()
            );
        }
        if (i < functionString.length()) {
            String tail = trimExpression(functionString.substring(i), true);
            if (!tail.isEmpty()) {
                throw new SQLSyntaxErrorException(
                    "Unexpected expression '" + tail + "' after a function declaration",
                    SQLStates.SYNTAX_ERROR.getSqlState()
                );
            }
        }
        return new FunctionExpression(functionName, functionParameters);
    }

    /**
     * Handles an escape expression. All expression substitutes are applied to
     * the passed {@code buffer} parameter. In case of {@code fn}, the function
     * name is case-insensitive.
     *
     * @param buffer buffer containing current escape expression
     * @param start  start position of the escape syntax in the buffer, inclusive
     * @param end    end position of the escape syntax in the buffer, exclusive
     *
     * @throws SQLSyntaxErrorException if any syntax error happen
     */
    private void processEscapeExpression(StringBuilder buffer, int start, int end)
        throws SQLSyntaxErrorException {
        if (buffer.charAt(start) != '{' || buffer.charAt(end - 1) != '}') {
            return;
        }
        int startExpression = seekFirstNonSpaceSymbol(buffer, start + 1);
        int endExpression = seekLastNonSpaceSymbol(buffer, end - 2) + 1;

        if (substringMatches(buffer, "fn ", startExpression)) {
            FunctionExpression expression = parseFunction(buffer.substring(startExpression + 3, endExpression));
            ThrowingBiFunction<FunctionExpression, Connection, Expression, SQLSyntaxErrorException> mapper =
                functionMappings.get(FunctionSignatureKey.of(expression.getName(), expression.getParameters().size()));
            if (mapper == null) {
                throw new SQLSyntaxErrorException(
                    "Unknown function " + expression.getName(),
                    SQLStates.SYNTAX_ERROR.getSqlState()
                );
            }
            buffer.replace(start, end, mapper.apply(expression, jdbcContext).toString());
        } else if (substringMatches(buffer, "oj ", startExpression)) {
            buffer.replace(start, end, buffer.substring(startExpression + 3, endExpression));
        } else if (substringMatches(buffer, "escape ", startExpression)) {
            buffer.replace(start, end, buffer.substring(startExpression, endExpression));
        } else if (substringMatches(buffer, "limit ", startExpression)) {
            buffer.replace(start, end, buffer.substring(startExpression, endExpression));
        } else {
            throw new SQLSyntaxErrorException("Unrecognizable escape expression", SQLStates.SYNTAX_ERROR.getSqlState());
        }
    }

    /**
     * Looks for the end of the region defined by its start and end
     * substring patterns.
     *
     * @param text        search text
     * @param position    start position in text to search the region, inclusive
     * @param startRegion pattern of the region start
     * @param endRegion   pattern of the region end
     *
     * @return found position of the region end, inclusive. Start position if the region start
     *     pattern does not match the text start position and {@literal -1} if the
     *     region end is not found.
     */
    private int seekEndOfRegion(String text, int position, String startRegion, String endRegion) {
        if (!text.regionMatches(position, startRegion, 0, startRegion.length())) {
            return position;
        }
        int end = text.indexOf(endRegion, position + startRegion.length());
        return end == -1 ? end : end + endRegion.length() - 1;
    }

    private boolean substringMatches(StringBuilder text, String substring, int start) {
        return text.indexOf(substring, start) == start;
    }

    private int seekFirstNonSpaceSymbol(CharSequence text, int position) {
        while (position < text.length() && Character.isWhitespace(text.charAt(position))) {
            position++;
        }
        return position;
    }

    private int seekLastNonSpaceSymbol(CharSequence text, int position) {
        while (position > 0 && Character.isWhitespace(text.charAt(position))) {
            position--;
        }
        return position;
    }

    /**
     * Returns a string where all leading and trailing
     * skippable parts such as whitespaces or optional
     * comments removed.
     *
     * @param expression      source string
     * @param includeComments flag indication should comments be removed
     *
     * @return trimmed source string without trailing
     *     and leading comments and whitespaces
     */
    private String trimExpression(String expression, boolean includeComments) {
        if (!includeComments) {
            return expression.trim();
        }

        int position = 0;
        StringBuilder clearExpression = new StringBuilder(expression.length());
        while (position < expression.length()) {
            char currentChar = expression.charAt(position);
            if (currentChar == '/') {
                int ahead = seekEndOfRegion(expression, position, BLOCK.getStart(), BLOCK.getEnd());
                position = ahead == -1 ? expression.length() : ahead + 1;
            } else if (currentChar == '-') {
                int ahead = seekEndOfRegion(expression, position, LINE.getStart(), LINE.getEnd());
                position = ahead == -1 ? expression.length() : ahead + 1;
            } else {
                clearExpression.append(expression.charAt(position));
                position++;
            }
        }
        return clearExpression.toString().trim();
    }

}
