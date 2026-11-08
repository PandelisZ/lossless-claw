import { POSTGRES_TABLES } from "./schema.js";

const tables = new Set<string>(POSTGRES_TABLES);
/** Translate only the SQL constructs used by native stores, preserving bound values. */
export function compilePostgresStatement(
  input: string,
  schema: string
): string {
  let sql = input;
  // Native FTS queries have one sanitized MATCH parameter. Parameter 1 remains bound.
  if (/\b(?:messages|summaries)_fts\s+MATCH\s+\?/.test(sql)) {
    const table = /\b(messages_fts|summaries_fts)\s+MATCH/.exec(sql)![1];
    sql = sql
      .replace(
        /snippet\(\w+,\s*\d+,\s*'',\s*'',\s*'\.\.\.',\s*32\)/g,
        `ts_headline('simple', ${table}.content, ${schema}.lcm_tsquery($1), 'MaxWords=32,MinWords=8,StartSel="",StopSel=""')`
      )
      .replace(
        new RegExp(`\\b${table}\\s+MATCH\\s+\\?`),
        `${table}.search @@ ${schema}.lcm_tsquery($1)`
      )
      .replace(
        /\brank\b/g,
        `(-ts_rank_cd(${table}.search, ${schema}.lcm_tsquery($1)))`
      );
    // Give the expression the native column name, without aliasing ORDER BY expressions.
    sql = sql.replace(/(\(-ts_rank_cd\([^\n]+\)\)),\s*\n/, "$1 AS rank,\n");
  }
  if (/summaries_fts_cjk\s+MATCH/.test(sql))
    throw new Error("PostgreSQL CJK searches use the native LIKE fallback");
  sql = sql.replace(
    /SELECT 1 AS present FROM sqlite_master WHERE type = 'table' AND name = \?/g,
    "SELECT 1 AS present FROM information_schema.tables WHERE table_schema = '" +
      schema +
      "' AND table_name = ?"
  );
  sql = sql.replace(/CROSS JOIN (\w+\s+\w+) ON/g, "JOIN $1 ON");
  sql = sql
    .replace(/printf\('%04d',\s*([\w.]+)\)/g, "lpad($1::text, 4, '0')")
    .replace(/char\(9,\s*10,\s*13\)/g, "(chr(9) || chr(10) || chr(13))")
    .replace(/char\(31\)/g, "chr(31)")
    .replace(/group_concat\(/g, "string_agg(")
    .replace(/\btrim\(/g, "btrim(")
    .replace(/\bIS NOT (excluded\.\w+)/g, "IS DISTINCT FROM $1")
    .replace(/\bjson_valid\(/g, `${schema}.lcm_json_valid(`)
    .replace(/\bjson_extract\(/g, `${schema}.lcm_json_extract(`)
    .replace(/\bjson_type\(/g, `${schema}.lcm_json_type(`)
    .replace(/\bdatetime\(/g, `${schema}.lcm_datetime(`)
    .replace(/\bjulianday\(/g, `${schema}.lcm_julianday(`);
  if (/INSERT OR IGNORE INTO/.test(sql))
    sql =
      sql.replace("INSERT OR IGNORE INTO", "INSERT INTO") +
      " ON CONFLICT DO NOTHING";
  sql = sql.replace(
    /MIN\(\?, MAX\(0, consumed_tokens\) \+ \?\)/g,
    "LEAST(?, GREATEST(0, consumed_tokens) + ?)"
  );
  // Quote mixed-case aliases; PostgreSQL otherwise folds SQLite's result keys.
  sql = sql.replace(
    /\bAS ([a-z][a-zA-Z0-9_]*[A-Z][a-zA-Z0-9_]*)\b/g,
    'AS "$1"'
  );
  // Lex SQL so placeholders and table references inside values/comments are untouched.
  let param = /\bMATCH\b/.test(input) && sql.includes("$1") ? 1 : 0;
  return sql.replace(
    /'(?:''|[^'])*'|"(?:""|[^"])*"|--[^\n]*|\/\*[\s\S]*?\*\/|\?|\b[A-Za-z_][A-Za-z_0-9]*\b/g,
    (token, offset) => {
      if (token === "?") return `$${++param}`;
      const identifier = token.startsWith('"')
        ? token.slice(1, -1).replace(/""/g, '"')
        : token;
      if (
        tables.has(identifier) &&
        /\b(?:FROM|JOIN|INTO|UPDATE|TABLE)\s*$/i.test(sql.slice(0, offset))
      ) {
        // Qualify relation references, but retain aliases/column qualifiers in SELECT.
        const tail = sql.slice(offset + token.length).trimStart();
        if (tail.startsWith(".")) return token;
        return `"${schema}".${token}`;
      }
      return token;
    }
  );
}

export const POSTGRES_FUNCTIONS = `
CREATE OR REPLACE FUNCTION lcm_datetime(value text) RETURNS text LANGUAGE sql STABLE AS $$
 SELECT to_char(CASE WHEN normalized = 'now' THEN statement_timestamp()
 ELSE CASE WHEN normalized ~ '(Z|z|[+-][0-9]{2}:?[0-9]{2})$' THEN normalized::timestamptz ELSE normalized::timestamp AT TIME ZONE 'UTC' END END AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS')
 FROM (SELECT btrim(value, chr(9)||chr(10)||chr(11)||chr(12)||chr(13)||' ') AS normalized) input
$$;
CREATE OR REPLACE FUNCTION lcm_julianday(value text) RETURNS double precision LANGUAGE sql STABLE AS $$
 SELECT extract(epoch FROM CASE WHEN normalized = 'now' THEN statement_timestamp()
 ELSE CASE WHEN normalized ~ '(Z|z|[+-][0-9]{2}:?[0-9]{2})$' THEN normalized::timestamptz ELSE normalized::timestamp AT TIME ZONE 'UTC' END END)::double precision / 86400 + 2440587.5
 FROM (SELECT btrim(value, chr(9)||chr(10)||chr(11)||chr(12)||chr(13)||' ') AS normalized) input
$$;
CREATE OR REPLACE FUNCTION lcm_json_valid(value text) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $$
 BEGIN PERFORM value::jsonb; RETURN true; EXCEPTION WHEN invalid_text_representation THEN RETURN false; END
$$;
CREATE OR REPLACE FUNCTION lcm_json_extract(value text, path text) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
 BEGIN RETURN value::jsonb ->> substring(path FROM 3); EXCEPTION WHEN invalid_text_representation THEN RETURN NULL; END
$$;
CREATE OR REPLACE FUNCTION lcm_json_type(value text, path text) RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
 BEGIN RETURN CASE jsonb_typeof(value::jsonb -> substring(path FROM 3)) WHEN 'string' THEN 'text' ELSE jsonb_typeof(value::jsonb -> substring(path FROM 3)) END; EXCEPTION WHEN invalid_text_representation THEN RETURN NULL; END
$$;
CREATE OR REPLACE FUNCTION lcm_tsquery(value text) RETURNS tsquery LANGUAGE sql IMMUTABLE AS $$
 WITH terms AS (SELECT phraseto_tsquery('simple', phrase[1]) AS query FROM regexp_matches(value, '"([^"]+)"', 'g') phrase)
 SELECT COALESCE(string_agg('(' || query::text || ')', ' & ') FILTER (WHERE numnode(query)>0), '')::tsquery FROM terms
$$;
`;
