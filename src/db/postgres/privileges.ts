import type { PostgresDatabase } from "./database.js";

// Shared with provisioning. Catalog reads only: no grants, roles or objects are changed here.
export const UNRELATED_DATA_PRIVILEGES = `WITH identity AS (SELECT $1::text AS role_name)
SELECT n.nspname AS schema_name,c.relname AS object_name
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN identity
WHERE c.relkind IN ('r','p','v','m','f') AND left(n.nspname,3)<>'pg_'
AND n.nspname<>'information_schema' AND left(n.nspname,4)<>'lcm_'
AND has_schema_privilege(identity.role_name,n.oid,'USAGE')
AND (has_table_privilege(identity.role_name,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
OR has_any_column_privilege(identity.role_name,c.oid,'SELECT,INSERT,UPDATE,REFERENCES'))`;

export const ROLE_DDL_NAMESPACES =
  "SELECT nspname FROM pg_namespace WHERE left(nspname,3)<>'pg_' AND has_schema_privilege($1,oid,'CREATE')";

export async function inspectRuntimePrivileges(db: PostgresDatabase) {
  const role = await db
    .prepare(
      "SELECT current_user::text AS role_name,rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=current_user"
    )
    .get();
  const memberships = await db
    .prepare(
      "SELECT COUNT(*) AS count FROM pg_auth_members m JOIN pg_roles r ON r.oid=m.member WHERE r.rolname=current_user"
    )
    .get();
  const ownership = await db
    .prepare(
      "SELECT COUNT(*) AS count FROM pg_shdepend d JOIN pg_roles r ON r.oid=d.refobjid WHERE d.refclassid='pg_authid'::regclass AND d.deptype='o' AND r.rolname=current_user"
    )
    .get();
  const metadata = await db
    .prepare(
      "SELECT has_schema_privilege(current_user,?,'CREATE') AS schema_create,has_table_privilege(current_user,?,'UPDATE') AS identity_update,has_table_privilege(current_user,?,'UPDATE') AS import_update"
    )
    .get(
      db.schema,
      `"${db.schema}".lcm_store_identity`,
      `"${db.schema}".lcm_import_state`
    );
  const unrelatedData = role
    ? await db.prepare(UNRELATED_DATA_PRIVILEGES).all(String(role.role_name))
    : [];
  const ddlNamespaces = role
    ? await db.prepare(ROLE_DDL_NAMESPACES).all(String(role.role_name))
    : [];
  const privilegedRole =
    !role ||
    [
      "rolsuper",
      "rolcreaterole",
      "rolcreatedb",
      "rolreplication",
      "rolbypassrls",
    ].some((key) => Boolean(role[key]));
  const safe =
    !privilegedRole &&
    memberships?.count === 0 &&
    ownership?.count === 0 &&
    !!metadata &&
    !Object.values(metadata).some(Boolean) &&
    unrelatedData.length === 0 &&
    ddlNamespaces.length === 0;
  return {
    safe,
    privilegedRole,
    inheritedMemberships: memberships?.count,
    ownedObjects: ownership?.count,
    metadata,
    unrelatedData,
    ddlNamespaces,
  };
}
