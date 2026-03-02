import { describe, expect, it } from "vitest"
import { Field, isValidFlinkType, Schema } from "../schema.js"

describe("Field type builders", () => {
  it("returns primitive types as strings", () => {
    expect(Field.STRING()).toBe("STRING")
    expect(Field.BIGINT()).toBe("BIGINT")
    expect(Field.INT()).toBe("INT")
    expect(Field.BOOLEAN()).toBe("BOOLEAN")
    expect(Field.DOUBLE()).toBe("DOUBLE")
    expect(Field.DATE()).toBe("DATE")
    expect(Field.TIME()).toBe("TIME")
    expect(Field.BYTES()).toBe("BYTES")
    expect(Field.FLOAT()).toBe("FLOAT")
    expect(Field.TINYINT()).toBe("TINYINT")
    expect(Field.SMALLINT()).toBe("SMALLINT")
  })

  it("returns parameterized types with arguments", () => {
    expect(Field.DECIMAL(10, 2)).toBe("DECIMAL(10, 2)")
    expect(Field.TIMESTAMP(3)).toBe("TIMESTAMP(3)")
    expect(Field.TIMESTAMP_LTZ(6)).toBe("TIMESTAMP_LTZ(6)")
    expect(Field.VARCHAR(255)).toBe("VARCHAR(255)")
    expect(Field.CHAR(10)).toBe("CHAR(10)")
    expect(Field.BINARY(16)).toBe("BINARY(16)")
    expect(Field.VARBINARY(256)).toBe("VARBINARY(256)")
  })

  it("returns composite types", () => {
    expect(Field.ARRAY("STRING")).toBe("ARRAY<STRING>")
    expect(Field.MAP("STRING", "INT")).toBe("MAP<STRING, INT>")
    expect(Field.ROW("name STRING, age INT")).toBe("ROW<name STRING, age INT>")
  })

  it("defaults TIMESTAMP precision to 3", () => {
    expect(Field.TIMESTAMP()).toBe("TIMESTAMP(3)")
  })
})

describe("isValidFlinkType", () => {
  it("accepts all primitive types", () => {
    expect(isValidFlinkType("STRING")).toBe(true)
    expect(isValidFlinkType("BIGINT")).toBe(true)
    expect(isValidFlinkType("BOOLEAN")).toBe(true)
  })

  it("accepts parameterized types", () => {
    expect(isValidFlinkType("DECIMAL(10, 2)")).toBe(true)
    expect(isValidFlinkType("TIMESTAMP(3)")).toBe(true)
    expect(isValidFlinkType("VARCHAR(255)")).toBe(true)
  })

  it("accepts composite types", () => {
    expect(isValidFlinkType("ARRAY<STRING>")).toBe(true)
    expect(isValidFlinkType("MAP<STRING, INT>")).toBe(true)
    expect(isValidFlinkType("ROW<name STRING>")).toBe(true)
  })

  it("rejects invalid types", () => {
    expect(isValidFlinkType("STRIG")).toBe(false)
    expect(isValidFlinkType("integer")).toBe(false)
    expect(isValidFlinkType("")).toBe(false)
    expect(isValidFlinkType("NVARCHAR")).toBe(false)
  })
})

describe("Schema()", () => {
  it("creates a schema with simple fields", () => {
    const schema = Schema({
      fields: {
        user_id: Field.BIGINT(),
        name: Field.STRING(),
        balance: Field.DECIMAL(10, 2),
      },
    })

    expect(schema.fields.user_id).toBe("BIGINT")
    expect(schema.fields.name).toBe("STRING")
    expect(schema.fields.balance).toBe("DECIMAL(10, 2)")
    expect(schema.metadataColumns).toEqual([])
    expect(schema.watermark).toBeUndefined()
    expect(schema.primaryKey).toBeUndefined()
  })

  it("creates a schema with watermark", () => {
    const schema = Schema({
      fields: {
        event_time: Field.TIMESTAMP(3),
        data: Field.STRING(),
      },
      watermark: {
        column: "event_time",
        expression: "event_time - INTERVAL '5' SECOND",
      },
    })

    expect(schema.watermark).toEqual({
      column: "event_time",
      expression: "event_time - INTERVAL '5' SECOND",
    })
  })

  it("creates a schema with primary key", () => {
    const schema = Schema({
      fields: {
        id: Field.BIGINT(),
        name: Field.STRING(),
      },
      primaryKey: { columns: ["id"] },
    })

    expect(schema.primaryKey).toEqual({ columns: ["id"] })
  })

  it("creates a schema with metadata columns", () => {
    const schema = Schema({
      fields: {
        data: Field.STRING(),
      },
      metadataColumns: [
        { column: "proc_time", type: "TIMESTAMP_LTZ(3)", isVirtual: true },
      ],
    })

    expect(schema.metadataColumns).toHaveLength(1)
    expect(schema.metadataColumns[0].column).toBe("proc_time")
    expect(schema.metadataColumns[0].isVirtual).toBe(true)
  })

  it("throws on invalid field type", () => {
    expect(() =>
      Schema({
        fields: { bad: "NOPE" as never },
      }),
    ).toThrow("Invalid Flink SQL type 'NOPE' for field 'bad'")
  })

  it("throws when watermark references non-existent field", () => {
    expect(() =>
      Schema({
        fields: { data: Field.STRING() },
        watermark: { column: "ts", expression: "ts - INTERVAL '1' SECOND" },
      }),
    ).toThrow("Watermark column 'ts' is not a declared field")
  })

  it("throws when primary key references non-existent field", () => {
    expect(() =>
      Schema({
        fields: { data: Field.STRING() },
        primaryKey: { columns: ["id"] },
      }),
    ).toThrow("Primary key column 'id' is not a declared field")
  })
})
