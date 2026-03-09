import { Join } from "@/components/joins"
import { Pipeline } from "@/components/pipeline"
import { Route } from "@/components/route"
import { FileSystemSink, JdbcSink } from "@/components/sinks"
import { GenericSource } from "@/components/sources"
import { Aggregate } from "@/components/transforms"
import { createElement } from "@/core/jsx-runtime"
import { Field, Schema } from "@/core/schema"

const TransactionSchema = Schema({
  fields: {
    transaction_id: Field.STRING(),
    account_id: Field.STRING(),
    merchant_id: Field.STRING(),
    amount: Field.DECIMAL(10, 2),
    currency: Field.STRING(),
    transaction_type: Field.STRING(),
    transaction_date: Field.DATE(),
    is_fraud: Field.BOOLEAN(),
  },
})

const MerchantSchema = Schema({
  fields: {
    merchant_id: Field.STRING(),
    merchant_name: Field.STRING(),
    merchant_category: Field.STRING(),
    merchant_country: Field.STRING(),
  },
})

const transactions = (
  <GenericSource
    connector="filesystem"
    format="parquet"
    schema={TransactionSchema}
    options={{ path: "s3://data-warehouse/transactions/year=2024/month=01/" }}
  />
)

const merchants = (
  <GenericSource
    connector="filesystem"
    format="parquet"
    schema={MerchantSchema}
    options={{ path: "s3://data-warehouse/merchants/" }}
  />
)

const enriched = (
  <Join
    left={transactions}
    right={merchants}
    on="merchant_id = merchant_id"
    hints={{ broadcast: "right" }}
  />
)

export default (
  <Pipeline name="monthly-financial-report" mode="batch" parallelism={64}>
    {enriched}
    <Route>
      {/* Report 1: Category summary */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["merchant_category", "transaction_date"]}
          select={{
            merchant_category: "merchant_category",
            transaction_date: "transaction_date",
            total_volume: "SUM(amount)",
            txn_count: "COUNT(*)",
            fraud_count: "SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)",
          }}
        />
        <FileSystemSink
          path="s3://reports/monthly/category_summary/"
          format="parquet"
        />
      </Route.Branch>

      {/* Report 2: Country report */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={["merchant_country", "transaction_type"]}
          select={{
            merchant_country: "merchant_country",
            transaction_type: "transaction_type",
            country_volume: "SUM(amount)",
            avg_txn_amount: "AVG(amount)",
            unique_accounts: "COUNT(DISTINCT account_id)",
          }}
        />
        <JdbcSink
          url="jdbc:postgresql://db:5433/reporting"
          table="monthly_country_report"
        />
      </Route.Branch>

      {/* Report 3: Fraud extract */}
      <Route.Branch condition="is_fraud = true">
        <FileSystemSink path="s3://reports/fraud/january_2024/" format="csv" />
      </Route.Branch>
    </Route>
  </Pipeline>
)
