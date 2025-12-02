# -*- coding: utf-8 -*-
"""Módulo com classe para interagir com o GCS"""

import io
import zipfile
from mimetypes import MimeTypes
from pathlib import Path
from typing import Optional, Union

from google.cloud import storage
from google.cloud.storage.blob import Blob

from pipelines.common.utils.gcp.base import GCPBase


class Storage(GCPBase):
    """
    Classe para interagir com o GCS

    Args:
        dataset_id (str): dataset_id
        table_id (str): table_id
        bucket_names (dict): nome dos buckets de prod e dev associados ao objeto
        env (str): prod ou dev
    """

    def __init__(
        self,
        env: str,
        dataset_id: str,
        table_id: Optional[str] = None,
        bucket_names: Optional[str] = None,
    ):
        super().__init__(
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_names=bucket_names,
            env=env,
        )

        self.bucket = self.client("storage").bucket(self.bucket_name)

    def create_blob_name(
        self,
        mode: str,
        filename: Optional[str] = None,
        filetype: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> str:
        """
        Cria o nome do blob

        Args:
            mode (str): pasta raiz
            filename (str): nome do arquivo
            filetype (str): extensão do arquivo
            partition (str): partições no formato Hive

        Returns:
            str: nome completo do blob
        """
        blob_name = f"{mode}/{self.dataset_id}"
        if self.table_id is not None:
            blob_name += f"/{self.table_id}"

        if partition is not None:
            partition = partition.strip("/")
            blob_name += f"/{partition}"

        if filename is not None:
            blob_name += f"/{filename}"

            if filetype is not None:
                blob_name += f".{filetype}"
        else:
            blob_name += "/"

        return blob_name

    def _check_mode(self, mode: str):
        accept = ["upload", "raw", "source"]
        if mode not in accept:
            raise ValueError(f"mode must be: {', '.join(accept)}. Received {mode}")

    def upload_file(
        self,
        mode: str,
        filepath: Union[str, Path],
        partition: Optional[str] = None,
        if_exists: str = "replace",
        chunk_size: Optional[int] = None,
        **upload_kwargs,
    ):
        """
        Sobe um arquivo para o Storage

        Args:
            mode (str): prod ou dev
            filepath (Union[str, Path]): Caminho do arquivo local
            partition (str): partição no formato Hive
            if_exists (str): Ação a ser tomada caso o arquivo exista
                no storage (raise, pass, replace)
            chunk_size (int): Tamanho do chunk do blob em bytes (deve ser múltiplo de 256 KB)
            upload_kwargs: Argumentos adicionais para a função upload_from_filename
        """
        filepath = Path(filepath)

        if filepath.is_dir():
            raise IsADirectoryError("filepath is a directory")

        filename_parts = filepath.name.rsplit(".", 1)
        filetype = filename_parts[1] if len(filename_parts) > 1 else None
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename_parts[0],
            filetype=filetype,
        )

        blob = self.bucket.blob(blob_name, chunk_size=chunk_size)

        if not blob.exists() or if_exists == "replace":
            print(f"Uploading file {filepath} to {self.bucket.name}/{blob_name}")
            upload_kwargs["timeout"] = upload_kwargs.get("timeout", None)

            blob.upload_from_filename(str(filepath), **upload_kwargs)
            print("File uploaded!")

        elif if_exists == "pass":
            print("Blob already exists skipping upload")

        else:
            raise FileExistsError("Blob already exists")

    def get_blob_obj(
        self,
        mode: str,
        filename: str,
        filetype: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> Blob:
        """
        Pega um blob no storage e retorna no formato de objeto Blob

        Args:
            mode (str): pasta raiz
            filename (str): nome do arquivo
            filetype (str): extensão do arquivo
            partition (str): partições no formato Hive

        Returns:
            Blob: o objeto que representa o arquivo no storage
        """
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename,
            filetype=filetype,
        )
        return self.bucket.get_blob(blob_name=blob_name)

    def get_blob_bytes(
        self,
        mode: str,
        filename: str,
        filetype: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> bytes:
        """
        Pega um blob no storage e retorna em bytes

        Args:
            mode (str): pasta raiz
            filename (str): nome do arquivo
            filetype (str): extensão do arquivo
            partition (str): partições no formato Hive

        Returns:
            bytes: bytes do arquivo no storage
        """
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename,
            filetype=filetype,
        )
        return self.bucket.get_blob(blob_name=blob_name).download_as_bytes()

    def get_blob_string(
        self,
        mode: str,
        filename: str,
        filetype: Optional[str] = None,
        partition: Optional[str] = None,
    ) -> str:
        """
        Pega um blob no storage e retorna em string

        Args:
            mode (str): pasta raiz
            filename (str): nome do arquivo
            filetype (str): extensão do arquivo
            partition (str): partições no formato Hive

        Returns:
            str: string do arquivo no storage
        """
        blob_name = self.create_blob_name(
            mode=mode,
            partition=partition,
            filename=filename,
            filetype=filetype,
        )
        return self.bucket.get_blob(blob_name=blob_name).download_as_text()

    def unzip_file(self, mode: str, zip_filename: str, unzip_to: str):
        """
        Faz o download de uma pasta compactada .zip no storage
        e descompacta em outra pasta no storage

        Args:
            mode (str): pasta raiz
            filename (str): nome do arquivo
            unzip_to (str): nome da pasta para salvar os arquivos

        """
        data = self.get_blob_bytes(mode=mode, filename=zip_filename)
        mime = MimeTypes()
        with zipfile.ZipFile(io.BytesIO(data), "r") as zipped_file:
            for name in zipped_file.namelist():
                unzipped_data = zipped_file.read(name=name)

                filename_parts = name.rsplit(".", 1)

                filetype = filename_parts[1] if len(filename_parts) > 1 else None

                blob_name = self.create_blob_name(
                    mode=mode,
                    partition=unzip_to,
                    filename=filename_parts[0],
                    filetype=filetype,
                )

                self.bucket.blob(blob_name).upload_from_string(
                    data=unzipped_data,
                    content_type=mime.guess_type(name)[0],
                )

    def move_folder(
        self,
        new_storage: "Storage",
        old_mode: str,
        new_mode: str,
        partitions: Optional[Union[str, list[str]]] = None,
    ):
        """
        Move uma pasta de um mode para outro ou de um Storage para outro

        Args:
            new_storage (Storage): Novo objeto de Storage para mover os arquivos
            old_mode (str): pasta raiz dos arquivos que serão movidos
            new_mode (str): pasta raiz para onde os arquivos vão ser movidos
            partitions Union[str, list[str]]: Partição ou lista de partições em
                formato Hive a serem movidas
        """
        partitions = (
            [partitions] if isinstance(partitions, str) or partitions is None else partitions
        )

        blobs = []

        for partition in partitions:
            blob_prefix = self.create_blob_name(mode=old_mode, partition=partition)
            source_blobs = list(self.bucket.list_blobs(prefix=blob_prefix))

            blob_mapping = [
                {
                    "source_blob": blob,
                    "new_name": blob.name.replace(
                        blob_prefix,
                        new_storage.create_blob_name(mode=new_mode, partition=partition),
                        1,
                    ),
                }
                for blob in source_blobs
                if not blob.name.endswith("/")
            ]

            blobs += blob_mapping

        if new_storage.bucket_name != self.bucket_name:
            for blob in blobs:
                source_blob: storage.Blob = blob["source_blob"]
                self.bucket.copy_blob(source_blob, new_storage.bucket, new_name=blob["new_name"])
                source_blob.delete()
        else:
            for blob in blobs:
                self.bucket.rename_blob(blob["source_blob"], new_name=blob["new_name"])
