from pyspark.sql.window import Window
import pyspark.sql.functions as F
from etl import ETL


class Aggregator(ETL):

    def extract(self):
        return self.spark.read.format("delta").load(
            "s3a://click/staging/search_events/"
        )

    def _calculate_average_price(self, dataframe):
        return dataframe.groupBy("route", "serviceClass").agg(
            F.mean("price").alias("average_price")
        )

    def _calculate_total_available_seats(self, dataframe):
        return dataframe.groupBy("travelCompanyName", "route").agg(
            F.sum("availableSeats").alias("total_available_seats")
        )

    def _rank_routes_by_company(self, dataframe):
        routes_by_company = dataframe.groupBy("travelCompanyName", "route").count()

        return (
            routes_by_company.withColumn(
                "rank",
                F.rank().over(
                    Window.partitionBy("travelCompanyName").orderBy(F.desc("count"))
                ),
            )
            .filter(F.col("rank") == 1)
            .select("travelCompanyName", "route")
        )

    def transform(self, dataframe):
        average_price_df = self._calculate_average_price(dataframe)
        available_seats_df = self._calculate_total_available_seats(dataframe)
        company_route_rank = self._rank_routes_by_company(dataframe)

        return {
            "average_price": average_price_df,
            "available_seats": available_seats_df,
            "company_route_rank": company_route_rank,
        }

    def load(self, dataframes):
        partitions = []
        path = "s3a://click/curated/{table_name}/"

        for table_name, df in dataframes.items():
            out_path = path.format(table_name=table_name)
            self.writer.write_data(df, partitions, out_path)

    def aggregate_data(self):
        self.run()
