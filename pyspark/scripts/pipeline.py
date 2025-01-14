from functions import create_session

# Staging ETLs
from event_processor import EventProcessor
from aggregator import Aggregator
from writer import Writer


if __name__ == "__main__":
    spark = create_session("Click")

    writer = Writer()

    # crating staging area
    event_processor_etl = EventProcessor(spark, writer)
    event_processor_etl.process_events()  # alias to run method

    aggregator_etl = Aggregator(spark, writer)
    aggregator_etl.aggregate_data()  # alias to run method

    spark.stop()
