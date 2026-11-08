import { afterEach, describe, expect, it, vi } from "vitest";
import pg from "pg";
import {
  PostgresDatabase,
  postgresEnvironment,
} from "../src/db/postgres/database.js";
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllEnvs();
});
describe("dedicated LCM connection credentials", () => {
  it("rejects missing dedicated credentials before any pool or connection despite ambient native credentials", async () => {
    for (const key of [
      "LCM_DISPOSABLE_POSTGRES",
      "LCM_PGHOST",
      "LCM_PGPORT",
      "LCM_PGDATABASE",
      "LCM_PGUSER",
      "LCM_PGPASSWORD",
      "LCM_PGSSLMODE",
      "LCM_PGSSLROOTCERT",
    ])
      vi.stubEnv(key, undefined);
    vi.stubEnv("PGHOST", "native-only.invalid");
    vi.stubEnv("PGUSER", "native-only");
    vi.stubEnv("PGPASSWORD", "synthetic-native-password");
    const pool = vi.spyOn(pg, "Pool");
    await expect(
      PostgresDatabase.open({
        schema: "lcm_test",
        organizationId: "org",
        agentId: "one",
      })
    ).rejects.toThrow(/ambient native/);
    await expect(
      PostgresDatabase.open({
        schema: "lcm_test",
        organizationId: "org",
        agentId: "one",
        pool: { max: 1 },
      })
    ).rejects.toThrow(/ambient native/);
    expect(pool).not.toHaveBeenCalled();
  });
  it("requires a complete namespace and verified TLS, without native password fallback", async () => {
    expect(() =>
      postgresEnvironment({ LCM_PGUSER: "runtime", PGHOST: "legacy" })
    ).toThrow(/explicit host/);
    const env = {
      LCM_PGHOST: "dedicated.invalid",
      LCM_PGDATABASE: "lcm",
      LCM_PGUSER: "runtime",
      LCM_PGPASSWORD: "synthetic-lcm-password",
      PGPASSWORD: "synthetic-native-password",
    };
    const options = postgresEnvironment(env);
    expect(options.ssl).toEqual({ rejectUnauthorized: true });
    expect(options.host).toBe("dedicated.invalid");
    expect(options.port).toBe(5432);
    expect(await (options.password as () => string)()).toBe(
      "synthetic-lcm-password"
    );
    expect(() =>
      postgresEnvironment({ ...env, LCM_PGPASSWORD: undefined })
    ).toThrow(/own password/);
    expect(() =>
      postgresEnvironment({ ...env, LCM_PGSSLMODE: "disable" })
    ).toThrow(/disposable/);
    const local = postgresEnvironment({
      ...env,
      LCM_PGPASSWORD: undefined,
      LCM_PGSSLMODE: "disable",
      LCM_DISPOSABLE_POSTGRES: "1",
    });
    expect(local.ssl).toBe(false);
    expect(await (local.password as () => string)()).toBe("");
  });
  it("rejects partial connection overrides and empty explicit passwords before constructing a pool", async () => {
    for (const [key, value] of Object.entries({
      LCM_PGHOST: "dedicated.invalid",
      LCM_PGDATABASE: "lcm",
      LCM_PGUSER: "runtime",
      LCM_PGPASSWORD: "synthetic-dedicated",
      LCM_PGSSLMODE: "verify-full",
      PGPASSWORD: "synthetic-native",
    }))
      vi.stubEnv(key, value);
    vi.stubEnv("LCM_DISPOSABLE_POSTGRES", undefined);
    const pool = vi.spyOn(pg, "Pool"),
      identity = { schema: "lcm_test", organizationId: "org", agentId: "one" };
    await expect(
      PostgresDatabase.open({ ...identity, pool: { ssl: false } })
    ).rejects.toThrow(/Partial/);
    await expect(
      PostgresDatabase.open({
        ...identity,
        pool: { host: undefined, password: undefined },
      })
    ).rejects.toThrow(/Partial/);
    const explicit = {
      host: "dedicated.invalid",
      database: "lcm",
      user: "runtime",
      port: 5432,
      ssl: true,
    };
    await expect(
      PostgresDatabase.open({
        ...identity,
        pool: { ...explicit, password: null as unknown as string },
      })
    ).rejects.toThrow();
    await expect(
      PostgresDatabase.open({
        ...identity,
        pool: { ...explicit, password: "" },
      })
    ).rejects.toThrow(/own password/);
    expect(pool).not.toHaveBeenCalled();
  });
  it("normalizes explicit TLS and passwords independently of Node/libpq insecure defaults", async () => {
    vi.stubEnv("LCM_DISPOSABLE_POSTGRES", undefined);
    vi.stubEnv("PGPASSWORD", "synthetic-native");
    vi.stubEnv("NODE_TLS_REJECT_UNAUTHORIZED", "0");
    let captured: pg.PoolConfig | undefined;
    vi.spyOn(pg, "Pool").mockImplementation(function (options) {
      captured = options;
      throw new Error("synthetic pool capture");
    });
    const identity = {
      schema: "lcm_test",
      organizationId: "org",
      agentId: "one",
    };
    await expect(
      PostgresDatabase.open({
        ...identity,
        pool: {
          host: "dedicated.invalid",
          database: "lcm",
          user: "runtime",
          port: 5432,
          ssl: true,
          password: "synthetic-dedicated",
        },
      })
    ).rejects.toThrow(/pool capture/);
    expect(captured!.ssl).toEqual({ rejectUnauthorized: true });
    expect(typeof captured!.password).toBe("function");
    expect(await (captured!.password as () => Promise<string>)()).toBe(
      "synthetic-dedicated"
    );
    await expect(
      PostgresDatabase.open({
        ...identity,
        pool: {
          host: "dedicated.invalid",
          database: "lcm",
          user: "runtime",
          port: 5432,
          ssl: {},
          password: () => "",
        },
      })
    ).rejects.toThrow(/pool capture/);
    expect(captured!.ssl).toEqual({ rejectUnauthorized: true });
    await expect(
      (captured!.password as () => Promise<string>)()
    ).rejects.toThrow(/no password/);
  });
});
