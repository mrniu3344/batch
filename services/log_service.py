import logging.config
import logging.handlers
import os

def getFileNameFromPath(filePath):
    if filePath is None:
        return "out"
    return os.path.splitext(os.path.basename(filePath))[0]

def getLogger(mod, filePath):
    logging.config.fileConfig(f"../configs/logging.{mod}.conf")
    logger = logging.getLogger()

    formatter = logging.Formatter('%(asctime)s][%(levelname)s](%(filename)s:%(lineno)s) %(message)s')    
    h = logging.handlers.TimedRotatingFileHandler("../log/error/"+ getFileNameFromPath(filePath) +".log", when="D")
    h.setLevel(logging.ERROR)
    h.setFormatter(formatter)
    logger.addHandler(h)
    return logger
