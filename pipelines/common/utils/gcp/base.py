# -*- coding: utf-8 -*-
"""Módulo base para as demais classes do módulo GCP"""

from dataclasses import dataclass
from typing import Union

from google.cloud import bigquery, storage

from pipelines.common import constants


@dataclass
class GCPBase:
    """
    Classe base para as demais classes do módulo GCP

    Args:
        dataset_id (str): dataset_id
        table_id (str): table_id
        bucket_names (dict): nome dos buckets de prod e dev associados ao objeto
        env (str): prod ou dev
    """

    dataset_id: str
    table_id: str
    bucket_names: dict
    env: str

    def __post_init__(self):
        self.set_env(env=self.env)

    def __getitem__(self, key):
        return self.__dict__[key]

    def set_env(self, env: str):
        """
        Altera o ambiente

        Args:
            env (str): prod ou dev

        Returns:
            self
        """
        self.env = env
        if self.bucket_names is None:
            self.bucket_name = constants.DEFAULT_BUCKET_NAME[env]
        else:
            self.bucket_name = self.bucket_names[env]

        return self

    def client(self, service: str) -> Union[storage.Client, bigquery.Client]:
        """
        Retorna o client para interagir com um servico

        Args:
            service (str): nome do serviço (storage ou bigquery)

        Returns:
            Union[storage.Client, bigquery.Client]: client do serviço
        """
        service_map = {"storage": storage.Client, "bigquery": bigquery.Client}
        return service_map[service](project=constants.PROJECT_NAME[self.env])
