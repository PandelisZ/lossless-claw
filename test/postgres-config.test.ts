import { describe, expect, it } from "vitest";
import manifest from "../openclaw.plugin.json" with {type:"json"};
import { resolveLcmConfig } from "../src/db/config.js";
describe("PostgreSQL opt-in configuration",()=>{
 it("keeps SQLite as the default and declares every experimental field",()=>{
  expect(resolveLcmConfig({},{}).storageBackend).toBe("sqlite");
  for(const key of ["storageBackend","postgresSchema","postgresSchemas","postgresOrganizationId","postgresAgentId"]){
   expect(manifest.configSchema.properties).toHaveProperty(key);
   expect(manifest.uiHints).toHaveProperty(key);
   expect(manifest.configGroups.flatMap(g=>g.properties)).toContain(key);
  }
 });
 it("applies explicit environment precedence and rejects unknown backend names",()=>{
  const config=resolveLcmConfig({LCM_STORAGE_BACKEND:"postgres",LCM_POSTGRES_SCHEMA:"lcm_test",LCM_POSTGRES_ORGANIZATION_ID:"org",LCM_POSTGRES_AGENT_ID:"one"},{storageBackend:"sqlite"});
  expect(config).toMatchObject({storageBackend:"postgres",postgresSchema:"lcm_test",postgresOrganizationId:"org",postgresAgentId:"one"});
  expect(()=>resolveLcmConfig({},{storageBackend:"unknown"})).toThrow(/storageBackend/);
 });
});
