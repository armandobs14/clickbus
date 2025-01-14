class ETL:
    def __init__(self, spark, writer):
        self.spark = spark
        self.writer = writer

    def extract(self):
        pass

    def transform(self, df):
        pass

    def load(self):
        pass

    def run(self):
        df = self.extract()
        final_df = self.transform(df)
        self.load(final_df)
