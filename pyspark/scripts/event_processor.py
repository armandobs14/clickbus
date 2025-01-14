from pyspark.sql import functions as F
from etl import ETL


from functions import flatten_dataframe


class EventProcessor(ETL):
    def extract(self):
        return self.spark.read.json("s3a://click/raw/searches/*.json")

    def transform(self, events_df):
        flattened_df = flatten_dataframe(events_df)
        flattened_df = flattened_df.withColumn(
            "departure_datetime",
            F.to_timestamp(F.concat_ws("departureDate", "departureHour")),
        )

        flattened_df = flattened_df.withColumn(
            "arrival_datetime",
            F.to_timestamp(F.concat_ws("arrivalDate", "arrivalHour")),
        )

        flattened_df = flattened_df.withColumn(
            "route",
            F.concat("originCity", F.lit(" -> "), "destinationCity"),
        )

        flattened_df = flattened_df.withColumn(
            "route_id",
            F.concat("originId", F.lit(" -> "), "destinationId"),
        )

        flattened_df.withColumn("current_timestamp", F.current_timestamp())
        return flattened_df.filter("departure_datetime > current_timestamp").filter(
            "availableSeats > 0"
        )

    def load(self, events_df):
        path = "s3a://click/staging/search_events/"
        partitions = ["originState", "destinationState"]

        self.writer.write_data(events_df, partitions, path)

    def process_events(self):
        self.run()
