from framework.logger import LoggerManager

logger = LoggerManager().get_logger()

logger.info("Pipeline iniciado")

logger.warning("Validando schema")

logger.success("Pipeline finalizado com sucesso")