import { createElement } from '../../core/jsx-runtime';
import { Schema, Field } from '../../core/schema';
import { Pipeline } from '../../components/pipeline';
import { GenericSource, JdbcSource } from '../../components/sources';
import { FileSystemSink, JdbcSink } from '../../components/sinks';
import { Aggregate } from '../../components/transforms';
import { Join } from '../../components/joins';
import { Route } from '../../components/route';

const SalesSchema = Schema({
  fields: {
    sale_id: Field.STRING(),
    store_id: Field.STRING(),
    product_id: Field.STRING(),
    customer_id: Field.STRING(),
    quantity: Field.INT(),
    amount: Field.DECIMAL(10, 2),
    sale_date: Field.DATE(),
    sale_timestamp: Field.TIMESTAMP(3),
  },
});

const ProductSchema = Schema({
  fields: {
    product_id: Field.STRING(),
    product_name: Field.STRING(),
    category: Field.STRING(),
    subcategory: Field.STRING(),
    brand: Field.STRING(),
  },
});

const sales = (
  <GenericSource
    connector="filesystem"
    format="parquet"
    schema={SalesSchema}
    options={{ 'path': 's3://data-warehouse/sales/2024/' }}
  />
);

const products = (
  <JdbcSource
    url="jdbc:postgresql://db:5432/catalog"
    table="products"
    schema={ProductSchema}
  />
);

const enriched = (
  <Join
    left={sales}
    right={products}
    on="product_id = product_id"
    hints={{ broadcast: 'right' }}
  />
);

export default (
  <Pipeline name="sales-batch-etl" mode="batch" parallelism={32}>
    {enriched}
    <Route>
      {/* Daily category sales → S3 Parquet */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={['category', 'subcategory', 'sale_date']}
          select={{
            category: 'category',
            subcategory: 'subcategory',
            sale_date: 'sale_date',
            total_revenue: 'SUM(amount)',
            total_units: 'SUM(quantity)',
            unique_customers: 'COUNT(DISTINCT customer_id)',
          }}
        />
        <FileSystemSink
          path="s3://data-warehouse/aggregates/daily_category_sales/"
          format="parquet"
          partitionBy={['sale_date']}
        />
      </Route.Branch>

      {/* Daily brand performance → PostgreSQL */}
      <Route.Branch condition="true">
        <Aggregate
          groupBy={['brand', 'sale_date']}
          select={{
            brand: 'brand',
            sale_date: 'sale_date',
            brand_revenue: 'SUM(amount)',
            transaction_count: 'COUNT(*)',
          }}
        />
        <JdbcSink
          url="jdbc:postgresql://db:5432/analytics"
          table="daily_brand_performance"
        />
      </Route.Branch>
    </Route>
  </Pipeline>
);
