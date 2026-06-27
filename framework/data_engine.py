import duckdb
from framework.logger import LoggerManager


class DataEngine:
    """
    Motor de processamento de dados usando DuckDB.
    Substitui a necessidade de Spark/Java para desenvolvimento local.
    """

    def __init__(self):
        self.logger = LoggerManager().get_logger()
        self.con = duckdb.connect(database=':memory:')
        self.logger.info("DataEngine (DuckDB) inicializado com sucesso.")

    def query(self, sql):
        return self.con.query(sql)

    def read_json(self, path):
        return self.con.read_json(path)

    def execute(self, sql):
        self.con.execute(sql)
