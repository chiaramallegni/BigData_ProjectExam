import logging
import datetime
import sys
from variables_and_path import *


class MyLogger:
    logger = None

    def __init__(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)
        time=datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        log_path = fld_log + '\\'+time

        if not os.path.exists(log_path):
            os.makedirs(log_path)

        # Crea un gestore per lo standard output
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)

        # Formatta i messaggi di log
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stdout_handler.setFormatter(formatter)

        # Aggiungi il gestore di stdout al logger radice

        # gestore per scrivere su console
        self.logger.addHandler(stdout_handler)

        # Crea un gestore per il file di log di debug
        debug_file_handler = logging.FileHandler(log_path+'\\debug.log')
        debug_file_handler.setLevel(logging.DEBUG)
        debug_file_handler.setFormatter(formatter)
        self.logger.addHandler(debug_file_handler)

        # Crea un gestore per il file di log di info
        info_file_handler = logging.FileHandler(log_path+'\\info.log')
        info_file_handler.setLevel(logging.INFO)
        info_file_handler.setFormatter(formatter)
        self.logger.addHandler(info_file_handler)

        # Crea un gestore per il file di log di errore
        error_file_handler = logging.FileHandler(log_path+'\\error.log')
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(formatter)
        self.logger.addHandler(error_file_handler)
