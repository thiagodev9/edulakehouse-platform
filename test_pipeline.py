from framework.base_pipeline import BasePipeline


class BronzePipeline(BasePipeline):

    def run(self):

        self.logger.info("Extraindo dados...")

        self.logger.info("Transformando...")

        self.logger.info("Salvando Bronze...")


pipeline = BronzePipeline()

pipeline.execute()