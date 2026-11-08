/** Experimental transport: no retries of ambiguous writes and no credential logs. */
export type Document = {
  id: string;
  kind: string;
  conversation: string;
  payload: string;
  version?: number;
  commit?: string;
};
export type Write = {
  upsert_rows?: Document[];
  upsert_condition?: unknown;
  patch_rows?: Partial<Document>[];
  patch_condition?: unknown;
};
export type WriteResult = { upserted: number; patched: number };
export interface DocumentTransport {
  read(ids: string[]): Promise<Document[]>;
  write(body: Write): Promise<WriteResult>;
}
export type RequestMetric = {
  method: string;
  suffix: string;
  ms: number;
  status?: number;
  requestBytes: number;
  responseBytes?: number;
  serverMs?: number;
  billing?: unknown;
};

export class TurbopufferTransport implements DocumentTransport {
  readonly metrics: RequestMetric[] = [];
  readonly namespace: string;
  #key: string;
  #url: string;
  constructor(options: { apiKey: string; namespace: string; region?: string }) {
    if (!/^[A-Za-z0-9_.-]{1,128}$/.test(options.namespace))
      throw new Error("Invalid namespace");
    const region = options.region ?? "aws-us-east-1";
    if (!/^[a-z0-9-]+$/.test(region)) throw new Error("Invalid region");
    this.namespace = options.namespace;
    this.#key = options.apiKey;
    this.#url = `https://${region}.turbopuffer.com/v2/namespaces/${options.namespace}`;
  }
  async request(
    method: string,
    suffix: string,
    body?: unknown,
  ): Promise<Record<string, any>> {
    const text = body === undefined ? undefined : JSON.stringify(body);
    const metric: RequestMetric = {
      method,
      suffix,
      ms: 0,
      requestBytes: Buffer.byteLength(text ?? ""),
    };
    const start = performance.now();
    try {
      const response = await fetch(this.#url + suffix, {
        method,
        headers: {
          Authorization: `Bearer ${this.#key}`,
          "Content-Type": "application/json",
        },
        body: text,
        signal: AbortSignal.timeout(30000),
      });
      const raw = await response.text();
      metric.status = response.status;
      metric.responseBytes = Buffer.byteLength(raw);
      if (!response.ok) throw new Error(`Turbopuffer HTTP ${response.status}`);
      const value = JSON.parse(raw);
      metric.serverMs = value.performance?.server_total_ms;
      metric.billing = value.billing;
      return value;
    } catch (error) {
      if (
        error instanceof Error &&
        /^Turbopuffer HTTP \d+$/.test(error.message)
      )
        throw error;
      throw new Error(
        "Turbopuffer request failed; a write outcome may be unknown",
      );
    } finally {
      metric.ms = performance.now() - start;
      this.metrics.push(metric);
    }
  }
  async read(ids: string[]): Promise<Document[]> {
    const result: Document[] = [];
    for (let at = 0; at < ids.length; at += 128) {
      const response = await this.request("POST", "/query", {
        rank_by: ["id", "asc"],
        filters: ["id", "In", ids.slice(at, at + 128)],
        top_k: 128,
        include_attributes: true,
        consistency: { level: "strong" },
      });
      result.push(...response.rows);
    }
    return result;
  }
  async write(body: Write): Promise<WriteResult> {
    const response = await this.request("POST", "", {
      ...body,
      schema: {
        payload: { type: "string", filterable: false },
        kind: "string",
        conversation: "string",
        version: "int",
        commit: "string",
      },
    });
    // Aggregate rows_affected includes losing immutable inserts. Only the
    // separately reported root patch count establishes CAS success.
    if (
      (body.patch_rows && !Number.isSafeInteger(response.rows_patched)) ||
      (body.upsert_rows && !Number.isSafeInteger(response.rows_upserted))
    )
      throw new Error("Turbopuffer write counts missing; reconcile outcome");
    return {
      upserted: response.rows_upserted ?? 0,
      patched: response.rows_patched ?? 0,
    };
  }
}
