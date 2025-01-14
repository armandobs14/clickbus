from typing import Union, List
from pyspark.sql import DataFrame


class Writer:
    def __init__(self, output_format="delta"):
        self.format = output_format

    def write_data(
        self,
        dataframe: DataFrame,
        partitions: Union[str, List],
        path: str,
        mode: str = "overwrite",
    ):
        (
            dataframe.write.format(self.format)
            .partitionBy(partitions)
            .mode(mode)
            .save(path)
        )
